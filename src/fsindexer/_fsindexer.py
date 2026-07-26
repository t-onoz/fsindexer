"""
Experimental fsspec-based file indexer.

This is a personal research project and is not intended for
production use without additional testing.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from zipfile import BadZipFile

import fsspec
from fsspec import AbstractFileSystem  # type: ignore
from fsspec.implementations.zip import ZipFileSystem  # type: ignore

logger = logging.getLogger(__name__)


_NETWORK_WINERRORS = {
    53,  # ERROR_BAD_NETPATH
    59,  # ERROR_UNEXP_NET_ERR
    64,  # ERROR_NETNAME_DELETED
    67,  # ERROR_BAD_NET_NAME
    121,  # ERROR_SEM_TIMEOUT
    1231,  # ERROR_NETWORK_UNREACHABLE
    1232,  # ERROR_HOST_UNREACHABLE
}


def enable_debug_log(stream: Any = None):
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(stream))


def disable_debug_log():
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)


Info = Mapping[str, Any]

MarkerFunc = Callable[
    [Info, Sequence[Info], fsspec.AbstractFileSystem],
    str | None,
]


def marker_example(
    info: Info, children: Sequence[Info], fs: fsspec.AbstractFileSystem
) -> str | None:
    names = {os.path.basename(c["name"]).lower() for c in children}
    if "data.csv" in names and "metadata.json" in names:
        return "data folder"

    if info["type"] == "file" and info["name"].endswith(".raw"):
        return "raw file"

    if info["type"] == "file" and info["name"].endswith(".jpg"):
        return "jpg file"

    return None


FINGERPRINT_VERSION = b"file-index-v1"


@dataclass(frozen=True)
class MarkedNode:
    node_key: str
    fs_path: str
    name: str
    node_type: str
    marker: str


@dataclass(frozen=True)
class ScanLocation:
    """Identify a node in one fsspec filesystem."""

    fs: AbstractFileSystem
    path: str
    key: str
    parent_key: str | None
    allow_zip: bool = True


class FileIndexer:
    """ファイルシステムを走査し、ディレクトリ・ファイル・ZIP内ファイルをSQLiteに索引化する。

    本クラスは fsspec が扱えるファイルシステムを走査し、各ノードをツリー構造として
    SQLite に保存する。

    ディレクトリについては、以下の情報から「fingerprint」を計算する。

    - ディレクトリ自身の更新時刻
    - 直下の子の名前
    - 直下の子の更新時刻

    fingerprint が前回と一致した場合、そのディレクトリ以下に変更はないとみなし、
    子孫ディレクトリの走査を省略できる。

    fingerprint は「直下1階層のみ」を対象として計算される。
    サブツリー全体を再帰的にハッシュ化するものではない。

    Parameters
    ----------
    root
        走査開始ディレクトリ。fsspec.url_to_fs() が受け付けるパスまたはURL。

    database
        SQLiteデータベースの保存先。

    root_dir_pattern
        インデックスの対象範囲を定義する正規表現。

        ルートディレクトリ直下のディレクトリ名全体に対して
        ``match()`` で判定し、一致したディレクトリとその子孫だけを
        索引化する。

        ``None`` の場合、ルート直下のすべてのディレクトリを対象とする。

        一致しないディレクトリは単に今回の走査を省略されるのではなく、
        このインデックスの対象外として扱われる。以前の走査で索引化されていた
        ノードが現在のパターンに一致しない場合、そのノードと子孫は
        インデックスから削除される。

        この設定はルート直下にのみ適用され、それより深い階層の名前には
        適用されない。

    marker_func
        ノード判定関数。

        ``(info, children, fs)`` を受け取り、
        そのノードが意味のある単位である場合は
        マーカー名（``str``）を返し、
        それ以外は ``None`` を返す。

        マーカーは後続処理で対象ノードを識別するために利用される。
        例えば

        - "measurement"
        - "dataset"
        - "project"

        など任意の名前を使用できる。

        必要に応じて ``fs`` を用いてファイル内容を参照して判定してよい。

    fingerprint_depth
        fingerprint による走査省略を開始するディレクトリ深さ。

        ルートディレクトリを深さ0とする。

        例えば

            root/                  (depth=0)
            └── folder_A/          (depth=1)
                └── request_001/   (depth=2)
                    └── data.csv

        の場合、

        fingerprint_depth=1 なら

        - root は毎回走査する。
        - folder_A も毎回一覧取得（ls）する。
        - folder_A の fingerprint が一致すれば、
          request_001 以下は走査しない。

        fingerprint_depth=2 なら

        - root と folder_A は毎回走査する。
        - request_001 も一覧取得（ls）して fingerprint を比較する。
        - request_001 の fingerprint が一致した場合のみ、
          data.csv など request_001 以下を走査しない。

        つまり、この値は
        「fingerprint を何階層まで計算するか」
        ではなく、
        「どの深さから枝刈り（キャッシュ利用）を許可するか」
        を表す。

        値を小さくすると SMB など高レイテンシ環境で高速になる一方、
        深い階層の変更検出は親ディレクトリの更新時刻に依存する。

    scan_archives
        True の場合、ZIPファイルを仮想ファイルシステムとして走査する。
        ZIP内のZIPは展開しない。

    Notes
    -----
    スキャン中にファイルやディレクトリが削除・移動されることがある。
    このような一過性の変化はスキップして処理を継続し、後続のスキャンで整合性を回復する。

    full_scan() は fingerprint を無視して全ディレクトリを再走査する。
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def __init__(
        self,
        *,
        root: str,
        database: str | Path,
        root_dir_pattern: str | re.Pattern[str] | None = None,
        marker_func: MarkerFunc | None = None,
        fingerprint_depth: int = 1,
        scan_archives: bool = True,
    ) -> None:
        self.fs, self.root = fsspec.url_to_fs(root)
        self.marker_func = marker_func
        self.fingerprint_depth = fingerprint_depth
        self.scan_zip = scan_archives
        if isinstance(root_dir_pattern, str):
            root_dir_pattern = re.compile(root_dir_pattern)
        self.root_dir_pattern = root_dir_pattern

        self.con = sqlite3.connect(database)
        self.con.row_factory = sqlite3.Row

        self.con.execute("PRAGMA journal_mode = WAL")
        self.con.execute("PRAGMA synchronous = NORMAL")

        self._create_tables()

    def close(self) -> None:
        """Close the SQLite database."""
        self.con.close()

    def scan(
        self,
        *,
        depth: int | None = None,
        full: bool = False,
    ) -> None:
        """Scan the configured root and update the index."""
        root = ScanLocation(
            fs=self.fs,
            path=self.root,
            key=self._external_key(self.fs, self.root),
            parent_key=None,
        )

        scan_id = self._begin_scan(full=full)

        try:
            self._scan_filesystem(
                root,
                depth=depth,
                current_depth=0,
                scan_id=scan_id,
                full=full,
            )
        except Exception:
            self.con.rollback()
            raise
        else:
            self._finish_scan(scan_id)
            self.con.commit()

    def full_scan(self, *, depth: int | None = None) -> None:
        """Scan the root while ignoring cached fingerprints."""
        self.scan(depth=depth, full=True)

    def iter_marked(self) -> Iterator[MarkedNode]:
        """Iterate over indexed nodes having a marker."""
        for row in self.con.execute(
            """
            SELECT
                node_key,
                fs_path,
                name,
                node_type,
                marker
            FROM node
            WHERE marker IS NOT NULL
            ORDER BY node_key
            """
        ):
            yield MarkedNode(**row)

    def _to_polars(self, table: Literal["node", "scan"]) -> pl.DataFrame:
        if table not in ("node", "scan"):
            raise ValueError(f"Unknown table: {table}")
        import polars as pl

        cur = self.con.execute(f'SELECT * FROM "{table}"')
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pl.DataFrame(rows, schema=cols, orient="row")

    # ------------------------------------------------------------------
    # Filesystem traversal
    # ------------------------------------------------------------------

    def _scan_filesystem(
        self,
        root: ScanLocation,
        *,
        depth: int | None,
        current_depth: int,
        scan_id: int,
        full: bool,
    ) -> None:
        """Scan one filesystem from the supplied root location."""
        queue: deque[tuple[ScanLocation, int | None, int]] = deque(
            [(root, depth, current_depth)]
        )

        while queue:
            location, remaining_depth, node_depth = queue.popleft()
            try:
                info = location.fs.info(location.path)
            except (FileNotFoundError, PermissionError) as exc:
                if getattr(exc, "winerror", None) in _NETWORK_WINERRORS:
                    # 共有またはネットワークへ到達できない。
                    # スキャン結果をコミットせず、上位でロールバックする。
                    raise
                logger.debug(
                    "could not get info on node %s (%s): %s",
                    location.path,
                    location.fs,
                    exc,
                )
                continue
            node_type = self._node_type(info)

            if node_type == "directory":
                children = self._scan_directory(
                    location,
                    info,
                    depth=remaining_depth,
                    current_depth=node_depth,
                    scan_id=scan_id,
                    full=full,
                )

                if remaining_depth == 0:
                    continue

                next_depth = None if remaining_depth is None else remaining_depth - 1

                for child in children:
                    child_info = child.info
                    child_type = self._node_type(child_info)

                    if child_type == "directory":
                        queue.append(
                            (
                                ScanLocation(
                                    fs=location.fs,
                                    path=str(child_info["name"]),
                                    key=child.key,
                                    parent_key=location.key,
                                    allow_zip=location.allow_zip,
                                ),
                                next_depth,
                                node_depth + 1,
                            )
                        )

                    elif (
                        self.scan_zip
                        and location.allow_zip
                        and child_type == "file"
                        and self._is_zip(child_info)
                    ):
                        self._scan_zip(
                            outer_fs=location.fs,
                            zip_info=child_info,
                            zip_key=child.key,
                            depth=next_depth,
                            current_depth=node_depth + 1,
                            scan_id=scan_id,
                            full=full,
                        )

            else:
                self._scan_file(
                    location,
                    info,
                    scan_id=scan_id,
                )

    def _scan_directory(
        self,
        location: ScanLocation,
        info: Info,
        *,
        depth: int | None,
        current_depth: int,
        scan_id: int,
        full: bool,
    ) -> list[_Child]:
        """Index one directory and return its immediate children."""
        logger.debug("scan %s (fs=%s)", location.path, location.fs)
        child_infos = list(location.fs.ls(location.path, detail=True))

        if current_depth == 0:
            child_infos = [
                child_info
                for child_info in child_infos
                if self._include_root_child(child_info)
            ]

        fingerprint = self._fingerprint(info, child_infos)

        children = [
            _Child(
                info=child_info,
                key=self._child_key(
                    location,
                    str(child_info["name"]),
                ),
            )
            for child_info in child_infos
        ]

        cache_hit = (
            not full
            and current_depth >= self.fingerprint_depth
            and self._cache_hit(
                location.key,
                fingerprint,
                depth,
            )
        )

        if cache_hit:
            logger.debug("cache hit: %s (fs=%s)", location.path, location.fs)
            return []

        existing_keys = self._indexed_child_keys(location.key)
        current_keys = {child.key for child in children}

        for deleted_key in existing_keys - current_keys:
            self._delete_subtree(deleted_key)

        self._upsert_node(
            node_key=location.key,
            parent_key=location.parent_key,
            fs_path=location.path,
            info=info,
            marker=self._marker(info, child_infos, location.fs),
            fingerprint=fingerprint,
            scanned_depth=depth,
            scan_id=scan_id,
        )

        for child in children:
            child_info = child.info

            self._upsert_node(
                node_key=child.key,
                parent_key=location.key,
                fs_path=str(child_info["name"]),
                info=child_info,
                marker=self._marker(child_info, (), location.fs),
                fingerprint=None,
                scanned_depth=None,
                scan_id=scan_id,
            )

        return children

    def _scan_file(
        self,
        location: ScanLocation,
        info: Info,
        *,
        scan_id: int,
    ) -> None:
        """Index one standalone file."""
        self._upsert_node(
            node_key=location.key,
            parent_key=location.parent_key,
            fs_path=location.path,
            info=info,
            marker=self._marker(info, (), location.fs),
            fingerprint=None,
            scanned_depth=None,
            scan_id=scan_id,
        )

    def _scan_zip(
        self,
        *,
        outer_fs: AbstractFileSystem,
        zip_info: Info,
        zip_key: str,
        depth: int | None,
        current_depth: int,
        scan_id: int,
        full: bool,
    ) -> None:
        """Scan one ZIP archive without expanding nested archives."""
        zip_path = str(zip_info["name"])
        zip_root_key = self._zip_key(zip_key, "")

        # ZIPファイルが更新された場合、内部索引を一度破棄する。
        previous_mtime = self._node_mtime_text(zip_key)
        current_mtime = self._mtime_text(zip_info)

        if previous_mtime != current_mtime:
            self._delete_children(zip_key)
        try:
            with (
                outer_fs.open(zip_path, "rb") as archive_file,
                closing(
                    ZipFileSystem(
                        fo=archive_file,  # type: ignore[arg-type]
                        mode="r",
                    )
                ) as zip_fs,
            ):
                zip_root = ScanLocation(
                    fs=zip_fs,
                    path="",
                    key=zip_root_key,
                    parent_key=zip_key,
                    allow_zip=False,
                )

                self._scan_filesystem(
                    zip_root,
                    depth=depth,
                    current_depth=current_depth,
                    scan_id=scan_id,
                    full=full,
                )
        except (FileNotFoundError, PermissionError) as exc:
            if getattr(exc, "winerror", None) in _NETWORK_WINERRORS:
                # 共有またはネットワークへ到達できない。
                # スキャン結果をコミットせず、上位でロールバックする。
                raise
            logger.debug("error while reading zip %s: %s", zip_path, exc)
        except BadZipFile:
            logger.warning("invalid zip archive: %s", zip_path)
        except OSError:
            logger.exception("I/O error while scanning zip: %s", zip_path)
            raise

    def _marker(
        self, info: Info, children: Sequence[Info], fs: fsspec.AbstractFileSystem
    ) -> str | None:
        """Return the marker name for one node."""
        if self.marker_func is None:
            return None

        return self.marker_func(info, children, fs)

    # ------------------------------------------------------------------
    # Fingerprints
    # ------------------------------------------------------------------

    def _fingerprint(
        self,
        info: Info,
        children: Sequence[Info],
    ) -> str:
        """Calculate a directory fingerprint."""
        digest = hashlib.blake2b(digest_size=8)

        self._hash_value(digest, FINGERPRINT_VERSION)
        self._hash_value(digest, self._mtime_text(info))

        sorted_children = sorted(
            children,
            key=lambda child: self._child_name(str(child["name"])),
        )

        for child in sorted_children:
            self._hash_value(
                digest,
                self._child_name(str(child["name"])),
            )
            self._hash_value(
                digest,
                self._mtime_text(child),
            )

        return digest.hexdigest()

    def _cache_hit(
        self,
        node_key: str,
        fingerprint: str,
        depth: int | None,
    ) -> bool:
        """Return whether a cached subtree covers the requested depth."""
        row = self.con.execute(
            """
            SELECT fingerprint, scanned_depth
            FROM node
            WHERE node_key = ?
            """,
            [node_key],
        ).fetchone()

        if row is None:
            return False

        if row["fingerprint"] != fingerprint:
            return False

        cached_depth = self._depth_from_db(row["scanned_depth"])

        return self._depth_covers(
            cached_depth,
            depth,
        )

    # ------------------------------------------------------------------
    # Database operations
    # ------------------------------------------------------------------

    def _create_tables(self) -> None:
        """Create the SQLite schema."""
        self.con.executescript(
            """
            CREATE TABLE IF NOT EXISTS node (
                node_key          TEXT PRIMARY KEY,
                parent_key        TEXT,
                fs_path           TEXT NOT NULL,
                name              TEXT NOT NULL,
                node_type         TEXT NOT NULL,
                size              INTEGER,
                mtime_text        TEXT,
                marker          TEXT,
                fingerprint       TEXT,
                scanned_depth     INTEGER,
                last_seen_scan_id INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_node_parent
            ON node(parent_key);

            CREATE INDEX IF NOT EXISTS ix_node_marker
            ON node(marker)
            WHERE marker IS NOT NULL;

            CREATE TABLE IF NOT EXISTS scan (
                scan_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at   TEXT NOT NULL,
                completed_at TEXT,
                full_scan    INTEGER NOT NULL
            );
            """
        )

    def _begin_scan(self, *, full: bool) -> int:
        """Create a scan record and return its ID."""
        cursor = self.con.execute(
            """
            INSERT INTO scan(started_at, full_scan)
            VALUES (?, ?)
            """,
            [
                datetime.now().astimezone().isoformat(),
                int(full),
            ],
        )

        return int(cursor.lastrowid)  # type: ignore

    def _finish_scan(self, scan_id: int) -> None:
        """Mark a scan as completed."""
        self.con.execute(
            """
            UPDATE scan
            SET completed_at = ?
            WHERE scan_id = ?
            """,
            [
                datetime.now().astimezone().isoformat(),
                scan_id,
            ],
        )

    def _upsert_node(
        self,
        *,
        node_key: str,
        parent_key: str | None,
        fs_path: str,
        info: Info,
        marker: str | None,
        fingerprint: str | None,
        scanned_depth: int | None,
        scan_id: int,
    ) -> None:
        """Insert or update one indexed node."""
        self.con.execute(
            """
            INSERT INTO node (
                node_key,
                parent_key,
                fs_path,
                name,
                node_type,
                size,
                mtime_text,
                marker,
                fingerprint,
                scanned_depth,
                last_seen_scan_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_key) DO UPDATE SET
                parent_key = excluded.parent_key,
                fs_path = excluded.fs_path,
                name = excluded.name,
                node_type = excluded.node_type,
                size = excluded.size,
                mtime_text = excluded.mtime_text,
                marker = excluded.marker,
                fingerprint = COALESCE(
                    excluded.fingerprint,
                    node.fingerprint
                ),
                scanned_depth = COALESCE(
                    excluded.scanned_depth,
                    node.scanned_depth
                ),
                last_seen_scan_id = excluded.last_seen_scan_id
            """,
            [
                node_key,
                parent_key,
                fs_path,
                self._child_name(fs_path),
                self._node_type(info),
                info.get("size"),
                self._mtime_text(info),
                marker,
                fingerprint,
                self._depth_to_db(scanned_depth),
                scan_id,
            ],
        )

    def _indexed_child_keys(
        self,
        parent_key: str,
    ) -> set[str]:
        """Return directly indexed child keys."""
        return {
            row["node_key"]
            for row in self.con.execute(
                """
                SELECT node_key
                FROM node
                WHERE parent_key = ?
                """,
                [parent_key],
            )
        }

    def _delete_children(self, parent_key: str) -> None:
        """Delete all indexed subtrees below one parent."""
        for child_key in self._indexed_child_keys(parent_key):
            self._delete_subtree(child_key)

    def _delete_subtree(self, node_key: str) -> None:
        """Delete one node and all indexed descendants."""
        self.con.execute(
            """
            WITH RECURSIVE subtree(node_key) AS (
                SELECT node_key
                FROM node
                WHERE node_key = ?

                UNION ALL

                SELECT node.node_key
                FROM node
                JOIN subtree
                  ON node.parent_key = subtree.node_key
            )
            DELETE FROM node
            WHERE node_key IN (
                SELECT node_key
                FROM subtree
            )
            """,
            [node_key],
        )

    def _node_mtime_text(self, node_key: str) -> str | None:
        """Return the stored textual mtime of one node."""
        row = self.con.execute(
            """
            SELECT mtime_text
            FROM node
            WHERE node_key = ?
            """,
            [node_key],
        ).fetchone()

        return None if row is None else row["mtime_text"]

    # ------------------------------------------------------------------
    # Path and key helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _node_type(info: Info) -> str:
        """Normalize the fsspec node type."""
        value = info.get("type")

        if value in {"directory", "dir"}:
            return "directory"

        if value == "file":
            return "file"

        return "other"

    @staticmethod
    def _is_zip(info: Info) -> bool:
        """Return whether one file should be opened as a ZIP."""
        if FileIndexer._node_type(info) != "file":
            return False

        return str(info["name"]).lower().endswith(".zip")

    @staticmethod
    def _mtime_text(info: Info) -> str:
        """Return a stable textual mtime representation."""
        value = (
            info.get("mtime")
            or info.get("LastModified")
            or info.get("last_modified")
            or info.get("date_time")
        )

        if value is None:
            return ""

        if isinstance(value, datetime):
            return value.isoformat()

        return str(value)

    @staticmethod
    def _hash_value(
        digest: Any,
        value: str | bytes,
    ) -> None:
        """Append one length-prefixed value to a digest."""
        data = (
            value
            if isinstance(value, bytes)
            else value.encode("utf-8", errors="surrogatepass")
        )

        digest.update(len(data).to_bytes(8, "little"))
        digest.update(data)

    @staticmethod
    def _child_name(path: str) -> str:
        """Return the final path component."""
        return path.rstrip("/\\").replace("\\", "/").rsplit("/", 1)[-1]

    @staticmethod
    def _depth_covers(
        cached: int | None,
        requested: int | None,
    ) -> bool:
        """Return whether cached depth covers requested depth."""
        if cached is None:
            return True

        if requested is None:
            return False

        return cached >= requested

    @staticmethod
    def _depth_to_db(depth: int | None) -> int:
        """Encode unlimited depth for SQLite."""
        return -1 if depth is None else depth

    @staticmethod
    def _depth_from_db(depth: int | None) -> int | None:
        """Decode unlimited depth from SQLite."""
        if depth is None or depth == -1:
            return None

        return int(depth)

    @staticmethod
    def _protocol(fs: AbstractFileSystem) -> str:
        """Return one filesystem protocol name."""
        protocol = fs.protocol

        if isinstance(protocol, (tuple, list)):
            return str(protocol[0])

        return str(protocol)

    @classmethod
    def _external_key(
        cls,
        fs: AbstractFileSystem,
        path: str,
    ) -> str:
        """Create a unique key for an external filesystem node."""
        return f"{cls._protocol(fs)}://{path}"

    @classmethod
    def _child_key(
        cls,
        parent: ScanLocation,
        child_path: str,
    ) -> str:
        """Create a unique key for one child node."""
        if cls._protocol(parent.fs) == "zip":
            outer_key = parent.key.split("::", 1)[-1]
            return cls._zip_key(outer_key, child_path)

        return cls._external_key(parent.fs, child_path)

    @staticmethod
    def _zip_key(
        outer_key: str,
        inner_path: str,
    ) -> str:
        """Create a unique key for one ZIP-internal node."""
        inner_path = inner_path.lstrip("/")
        return f"zip://{inner_path}::{outer_key}"

    def _include_root_child(self, info: Info) -> bool:
        """Return whether a root-level node should be scanned."""
        pattern = self.root_dir_pattern

        if pattern is None:
            return True

        # ルート直下のファイルは対象外
        if info["type"] != "directory":
            return False

        name = os.path.basename(info["name"])
        return pattern.match(name) is not None

    def __enter__(self) -> FileIndexer:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class _Child:
    """Pair one child info object with its database key."""

    info: Info
    key: str


if __name__ == "__main__":
    import sys
    from pathlib import Path
    from tempfile import TemporaryDirectory

    import polars as pl

    answer = input('Press ENTER to scan "~/Downloads", or enter anything to cancel: ')
    if answer:
        sys.exit("Cancelled.")
    enable_debug_log(sys.stdout)

    with TemporaryDirectory() as td:
        td_path = Path(td)
        with FileIndexer(
            root=str(Path.home() / "Downloads"),
            database=td_path / "db.sqlite",
            scan_archives=True,
            marker_func=marker_example,
        ) as indexer:
            print("-" * 80)
            print("  First scan")
            print("-" * 80)
            indexer.scan()
            print("-" * 80)
            print("  Second scan")
            print("-" * 80)
            indexer.scan()
            df_marked = pl.DataFrame(indexer.iter_marked())
            df_node = indexer._to_polars("node")
            df_scan = indexer._to_polars("scan")
    print("-" * 80)
    print("  All nodes")
    print("-" * 80)
    print(df_node)

    print("-" * 80)
    print("  Scan")
    print("-" * 80)
    print(df_scan)

    print("-" * 80)
    print("  Marked nodes")
    print("-" * 80)
    print(df_marked)
