from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from typing import Any

Info = Mapping[str, Any]

# (temporary_id, temporary_parent_id, info, marker)
# temporary_parent_id=None means the replacement subtree root.
Descendant = tuple[str, str | None, Info, str | None]


class NodeType(IntEnum):
    FILE = 0
    DIRECTORY = 1
    OTHER = 2


class IndexDatabase:
    def __init__(
        self,
        path: str | Path,
        *,
        full_scan: bool = False,
        read_only: bool = False,
    ):
        self.path = Path(path)
        self.full_scan = full_scan
        self.read_only = read_only
        self.con: sqlite3.Connection | None = None
        self.scan_id: int | None = None

    def __enter__(self):
        if self.read_only:
            uri = self.path.resolve().as_uri() + "?mode=ro"
            self.con = sqlite3.connect(uri, uri=True)
        else:
            self.con = sqlite3.connect(self.path)

        self.con.row_factory = sqlite3.Row

        if not self.read_only:
            self.con.execute("PRAGMA journal_mode = WAL")
            self.con.execute("PRAGMA synchronous = NORMAL")
            self._create_tables()

            self.scan_id = self._begin_scan()

        return self

    def __exit__(self, exc_type, exc, tb):
        con = self._connection

        try:
            if not self.read_only:
                if exc_type is None:
                    if self.scan_id is not None:
                        self._finish_scan()
                    con.commit()
                else:
                    con.rollback()
        finally:
            con.close()
            self.con = None
            self.scan_id = None

    def set_root_path(self, root_path: str) -> None:
        row = self._connection.execute(
            "SELECT root_path FROM metadata WHERE id = 1"
        ).fetchone()

        if row is None:
            self._connection.execute(
                "INSERT INTO metadata(id, root_path) VALUES (1, ?)",
                [root_path],
            )
        elif row["root_path"] != root_path:
            raise ValueError(
                f"Database root mismatch: {row['root_path']!r} != {root_path!r}"
            )

    def needs_scan(
        self,
        parent_id: int | None,
        name: str,
        fingerprint: str | None,
    ) -> bool:
        if self.full_scan:
            return True

        row = self._connection.execute(
            """
            SELECT fingerprint
            FROM node
            WHERE parent_id IS ? AND name = ?
            """,
            [parent_id, name],
        ).fetchone()

        return row is None or fingerprint is None or row["fingerprint"] != fingerprint

    def update_node(
        self,
        *,
        parent_id: int | None,
        info: Info,
        marker: str | None,
        fingerprint: str | None,
        archive_id: int | None = None,
    ) -> int:
        name = self._name(str(info["name"]))

        self._connection.execute(
            """
            INSERT INTO node (
                parent_id, name, node_type, size, mtime_ms, marker, fingerprint, archive_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO UPDATE SET
                node_type = excluded.node_type,
                size = excluded.size,
                mtime_ms = excluded.mtime_ms,
                marker = excluded.marker,
                fingerprint = excluded.fingerprint
            """,
            [
                parent_id,
                name,
                self._node_type(info),
                info.get("size"),
                self._mtime_ms(info),
                marker,
                fingerprint,
                archive_id,
            ],
        )

        row = self._connection.execute(
            """
            SELECT id
            FROM node
            WHERE parent_id IS ? AND name = ?
            """,
            [parent_id, name],
        ).fetchone()

        assert row is not None
        return int(row["id"])

    def remove_non_existent_children(
        self,
        parent_id: int,
        current_child_names: set[str],
    ) -> None:
        rows = self._connection.execute(
            "SELECT id, name FROM node WHERE parent_id = ?",
            [parent_id],
        )

        for row in rows:
            if row["name"] not in current_child_names:
                self._delete_subtree(int(row["id"]))

    def replace_subtree(
        self,
        *,
        root_id: int,
        descendants: Sequence[Descendant],
        archive_id: int | None = None,
    ) -> None:
        """Replace all descendants below ``root_id``.

        Descendants use temporary string IDs only to describe parent-child
        relationships inside the replacement data. Those temporary IDs are
        never stored in SQLite.
        """
        self._delete_children(root_id)

        remaining = {
            temp_id: (temp_parent_id, info, marker)
            for temp_id, temp_parent_id, info, marker in descendants
        }
        inserted: dict[str | None, int] = {None: root_id}

        while remaining:
            progressed = False

            for temp_id, (temp_parent_id, info, marker) in tuple(remaining.items()):
                parent_id = inserted.get(temp_parent_id)
                if parent_id is None:
                    continue

                inserted[temp_id] = self.update_node(
                    parent_id=parent_id,
                    info=info,
                    marker=marker,
                    fingerprint=None,
                    archive_id=archive_id,
                )
                del remaining[temp_id]
                progressed = True

            if not progressed:
                bad = ", ".join(sorted(remaining)[:3])
                raise ValueError(
                    f"replace_subtree contains nodes whose parents are missing: {bad}"
                )

    def iter_marked(self, batch_size: int = 10_000) -> Iterator[sqlite3.Row]:
        last_id = 0

        while True:
            cursor = self._connection.execute(
                """
                WITH RECURSIVE
                marked AS (
                    SELECT
                        id,
                        parent_id,
                        archive_id,
                        name,
                        node_type,
                        size,
                        mtime_ms,
                        marker,
                        fingerprint
                    FROM node
                    WHERE marker IS NOT NULL
                    AND id > ?
                    ORDER BY id
                    LIMIT ?
                ),

                -- 通常filesystem上のmarked nodeからrootへ遡る
                fs_path AS (
                    SELECT
                        id AS leaf_id,
                        id,
                        parent_id,
                        CASE
                            WHEN parent_id IS NULL THEN ''
                            ELSE name
                        END AS relative_path
                    FROM marked
                    WHERE archive_id IS NULL

                    UNION ALL

                    SELECT
                        p.leaf_id,
                        n.id,
                        n.parent_id,
                        CASE
                            WHEN n.parent_id IS NULL THEN p.relative_path
                            WHEN p.relative_path = '' THEN n.name
                            ELSE n.name || '/' || p.relative_path
                        END
                    FROM fs_path AS p
                    JOIN node AS n
                    ON n.id = p.parent_id
                ),

                fs_final AS (
                    SELECT
                        leaf_id,
                        relative_path
                    FROM fs_path
                    WHERE parent_id IS NULL
                ),

                -- ZIP内部のmarked nodeからarchive rootまで遡る
                zip_inner AS (
                    SELECT
                        id AS leaf_id,
                        parent_id,
                        archive_id,
                        name AS inner_path
                    FROM marked
                    WHERE archive_id IS NOT NULL

                    UNION ALL

                    SELECT
                        z.leaf_id,
                        n.parent_id,
                        z.archive_id,
                        n.name || '/' || z.inner_path
                    FROM zip_inner AS z
                    JOIN node AS n
                    ON n.id = z.parent_id
                    WHERE z.parent_id != z.archive_id
                ),

                zip_inner_final AS (
                    SELECT
                        leaf_id,
                        archive_id,
                        inner_path
                    FROM zip_inner
                    WHERE parent_id = archive_id
                ),

                -- 必要なouter ZIPだけfilesystem rootまで遡る
                archives AS (
                    SELECT DISTINCT archive_id
                    FROM marked
                    WHERE archive_id IS NOT NULL
                ),

                archive_path AS (
                    SELECT
                        a.archive_id,
                        n.id,
                        n.parent_id,
                        CASE
                            WHEN n.parent_id IS NULL THEN ''
                            ELSE n.name
                        END AS relative_path
                    FROM archives AS a
                    JOIN node AS n
                    ON n.id = a.archive_id

                    UNION ALL

                    SELECT
                        p.archive_id,
                        n.id,
                        n.parent_id,
                        CASE
                            WHEN n.parent_id IS NULL THEN p.relative_path
                            WHEN p.relative_path = '' THEN n.name
                            ELSE n.name || '/' || p.relative_path
                        END
                    FROM archive_path AS p
                    JOIN node AS n
                    ON n.id = p.parent_id
                ),

                archive_final AS (
                    SELECT
                        archive_id,
                        relative_path
                    FROM archive_path
                    WHERE parent_id IS NULL
                )

                SELECT
                    m.id,
                    m.parent_id,
                    m.archive_id,
                    m.name,
                    m.node_type,
                    m.size,
                    m.mtime_ms,
                    m.marker,
                    m.fingerprint,
                    CASE
                        WHEN m.archive_id IS NULL THEN
                            metadata.root_path
                            || CASE
                                WHEN f.relative_path = '' THEN ''
                                ELSE '/' || f.relative_path
                            END
                        ELSE
                            'zip://'
                            || z.inner_path
                            || '::'
                            || metadata.root_path
                            || CASE
                                WHEN a.relative_path = '' THEN ''
                                ELSE '/' || a.relative_path
                            END
                    END AS fs_path
                FROM marked AS m
                CROSS JOIN metadata
                LEFT JOIN fs_final AS f
                ON f.leaf_id = m.id
                LEFT JOIN zip_inner_final AS z
                ON z.leaf_id = m.id
                LEFT JOIN archive_final AS a
                ON a.archive_id = m.archive_id
                ORDER BY m.id
                """,
                [last_id, batch_size],
            )

            count = 0

            for row in cursor:
                last_id = int(row["id"])
                count += 1
                yield row

            if count < batch_size:
                break

    @property
    def _connection(self) -> sqlite3.Connection:
        if self.con is None:
            raise RuntimeError("IndexDatabase must be used as a context manager")
        return self.con

    def _create_tables(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS node (
                id          INTEGER PRIMARY KEY,
                parent_id   INTEGER,
                archive_id  INTEGER,       -- ZIP外ならNULL、ZIP内ならouter ZIPのnode id
                name        TEXT NOT NULL,
                node_type   INTEGER NOT NULL,
                size        INTEGER,
                mtime_ms    INTEGER,
                marker      TEXT,
                fingerprint TEXT
            );

            CREATE TABLE IF NOT EXISTS metadata (
                id        INTEGER PRIMARY KEY CHECK (id = 1),
                root_path TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_node_parent_name
            ON node(COALESCE(parent_id, -1), name);

            CREATE INDEX IF NOT EXISTS ix_node_parent
            ON node(parent_id);

            CREATE INDEX IF NOT EXISTS ix_node_marker
            ON node(marker)
            WHERE marker IS NOT NULL;

            CREATE INDEX IF NOT EXISTS ix_node_marked_id
            ON node(id)
            WHERE marker IS NOT NULL;

            CREATE TABLE IF NOT EXISTS scan (
                scan_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at   TEXT NOT NULL,
                completed_at TEXT,
                full_scan    INTEGER NOT NULL
            );
            """
        )

    def _begin_scan(self) -> int:
        cur = self._connection.execute(
            "INSERT INTO scan(started_at, full_scan) VALUES (?, ?)",
            [datetime.now().astimezone().isoformat(), int(self.full_scan)],
        )
        assert cur.lastrowid is not None
        return int(cur.lastrowid)

    def _finish_scan(self) -> None:
        self._connection.execute(
            "UPDATE scan SET completed_at = ? WHERE scan_id = ?",
            [datetime.now().astimezone().isoformat(), self.scan_id],
        )

    def _child_ids(self, parent_id: int) -> list[int]:
        return [
            int(row["id"])
            for row in self._connection.execute(
                "SELECT id FROM node WHERE parent_id = ?",
                [parent_id],
            )
        ]

    def _delete_children(self, parent_id: int) -> None:
        for node_id in self._child_ids(parent_id):
            self._delete_subtree(node_id)

    def _delete_subtree(self, node_id: int) -> None:
        self._connection.execute(
            """
            WITH RECURSIVE subtree(id) AS (
                SELECT id
                FROM node
                WHERE id = ?

                UNION ALL

                SELECT node.id
                FROM node
                JOIN subtree ON node.parent_id = subtree.id
            )
            DELETE FROM node
            WHERE id IN (SELECT id FROM subtree)
            """,
            [node_id],
        )

    @staticmethod
    def _node_type(info: Info) -> int:
        value = info.get("type")
        if value in {"directory", "dir", "folder"}:
            return NodeType.DIRECTORY
        if value == "file":
            return NodeType.FILE
        return NodeType.OTHER

    @staticmethod
    def _mtime_ms(info: Info) -> int | None:
        value = (
            info.get("mtime")
            or info.get("LastModified")
            or info.get("last_modified")
            or info.get("date_time")
        )

        if isinstance(value, datetime):
            return round(value.timestamp() * 1000)
        if isinstance(value, tuple):
            return round(datetime(*value).timestamp() * 1000)
        if isinstance(value, (int, float)):
            return round(value * 1000)
        return None

    @staticmethod
    def _name(path: str) -> str:
        return path.rstrip("/\\").replace("\\", "/").rsplit("/", 1)[-1]
