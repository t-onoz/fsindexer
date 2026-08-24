from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zipfile import BadZipFile

import fsspec
from fsspec import AbstractFileSystem
from fsspec.implementations.zip import ZipFileSystem
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ._database import Descendant, IndexDatabase, NodeType

logger = logging.getLogger(__name__)

Info = Mapping[str, Any]
MarkerFunc = Callable[[Info, Sequence[Info], AbstractFileSystem], str | None]
FINGERPRINT_VERSION = b"fsindexer-v2"
MAX_WORKERS = 8

_NETWORK_WINERRORS = {
    53,  # ERROR_BAD_NETPATH
    59,  # ERROR_UNEXP_NET_ERR
    64,  # ERROR_NETNAME_DELETED
    121,  # ERROR_SEM_TIMEOUT
    1231,  # ERROR_NETWORK_UNREACHABLE
    1232,  # ERROR_HOST_UNREACHABLE
}


_io_retry = retry(
    retry=retry_if_exception_type(OSError),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


@dataclass(frozen=True, slots=True)
class MarkedNode:
    id: int
    parent_id: int | None
    archive_id: int | None
    name: str
    node_type: int
    size: int | None
    mtime_ms: int | None
    marker: str | None
    fingerprint: str | None
    fs_path: str


class FileSystemIndexer:
    def __init__(
        self,
        *,
        root: str,
        database: str | Path,
        root_dir_pattern: str | re.Pattern[str] | None = None,
        marker_func: MarkerFunc | None = None,
        always_scan_depth: int = 1,
        scan_archives: bool = True,
    ) -> None:
        if always_scan_depth < 0:
            raise ValueError("always_scan_depth must be >= 0")

        self.fs, self.root = fsspec.url_to_fs(root)
        self.database = Path(database)
        self.marker_func = marker_func
        self.always_scan_depth = always_scan_depth
        self.scan_archives = scan_archives
        self.root_dir_pattern = (
            re.compile(root_dir_pattern)
            if isinstance(root_dir_pattern, str)
            else root_dir_pattern
        )

    # -----------------------------------------------------
    #   Public API
    # -----------------------------------------------------

    def scan(self, *, full: bool = False) -> None:
        with (
            IndexDatabase(self.database, full_scan=full) as db,
            ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor,
        ):
            db.set_root_path(self.root)
            self._scan_directory(
                db,
                executor,
                path=self.root,
                info=self.fs.info(self.root),
                parent_id=None,
                depth=0,
                is_root=True,
            )

    def full_scan(self) -> None:
        self.scan(full=True)

    def iter_marked(self) -> Iterator[MarkedNode]:
        with IndexDatabase(self.database, read_only=True) as db:
            yield from (MarkedNode(**row) for row in db.iter_marked())

    # -----------------------------------------------------
    #   Internals
    # -----------------------------------------------------

    def _scan_directory(
        self,
        db: IndexDatabase,
        executor: ThreadPoolExecutor,
        *,
        path: str,
        info: Info,
        parent_id: int | None,
        depth: int,
        is_root: bool = False,
        info_children: Sequence[Info] | None = None,
    ) -> None:
        if info_children is None:
            info_children = self._ls(path)

        if is_root and self.root_dir_pattern is not None:
            info_children = [
                child
                for child in info_children
                if (
                    self._node_type(child) != NodeType.DIRECTORY
                    or self.root_dir_pattern.match(self._name(str(child["name"])))
                )
            ]

        fingerprint = self._fingerprint(info)

        # replace `parent_id` with that of scanned path
        parent_id = db.update_node(
            parent_id=parent_id,
            info=info,
            marker=self._marker(info, info_children, self.fs),
            fingerprint=fingerprint,
        )

        child_names = {self._name(str(child["name"])) for child in info_children}
        db.remove_non_existent_children(parent_id, child_names)

        pending = []
        for child in info_children:
            child_path = str(child["name"])
            child_name = self._name(child_path)
            child_type = self._node_type(child)

            if child_type == NodeType.DIRECTORY:
                child_fingerprint = self._fingerprint(child)

                if depth < self.always_scan_depth or db.needs_scan(
                    parent_id, child_name, child_fingerprint
                ):
                    future = executor.submit(self._ls, child_path)
                    pending.append((child_path, child, future))
            elif self.scan_archives and self._is_zip(child):
                fingerprint = self._fingerprint(child)
                name = self._name(str(child["name"]))

                needs_scan = db.needs_scan(parent_id, name, fingerprint)

                zip_id = db.update_node(
                    parent_id=parent_id,
                    info=child,
                    marker=self._marker(child, (), self.fs),
                    fingerprint=fingerprint,
                )

                if needs_scan:
                    self._scan_zip(
                        db,
                        zip_id=zip_id,
                        info=child,
                    )
            else:
                db.update_node(
                    parent_id=parent_id,
                    info=child,
                    marker=self._marker(child, (), self.fs),
                    fingerprint=None,
                )
        for child_path, child, future in pending:
            child_children = future.result()
            self._scan_directory(
                db,
                executor,
                path=child_path,
                info=child,
                parent_id=parent_id,
                depth=depth + 1,
                info_children=child_children,
            )

    def _scan_zip(
        self,
        db: IndexDatabase,
        *,
        zip_id: int,
        info: Info,
    ) -> None:
        try:
            descendants = self._read_zip_tree(str(info["name"]))
        except BadZipFile:
            logger.warning("invalid zip archive: %s", info["name"])
            descendants = []

        db.replace_subtree(
            root_id=zip_id,
            descendants=descendants,
            archive_id=zip_id,
        )

    @_io_retry
    def _read_zip_tree(self, path: str) -> list[Descendant]:
        try:
            archive_file = self.fs.open(path, "rb")
        except FileNotFoundError as exc:
            if getattr(exc, "winerror", None) in _NETWORK_WINERRORS:
                raise
            return []

        with (
            archive_file,
            closing(ZipFileSystem(fo=archive_file, mode="r")) as zip_fs,
        ):
            entries: dict = zip_fs.find("", withdirs=True, detail=True)  # type: ignore

            children_by_parent = {}
            for inner_path, info in entries.items():
                parent = inner_path.rsplit("/", 1)[0] if "/" in inner_path else ""
                children_by_parent.setdefault(parent, []).append(info)

            nodes = []
            for inner_path, info in entries.items():
                parent_path = inner_path.rsplit("/", 1)[0] if "/" in inner_path else ""

                children = (
                    children_by_parent.get(inner_path, ())
                    if info["type"] == "directory"
                    else ()
                )

                nodes.append(
                    (
                        inner_path,  # temporary_id
                        parent_path or None,  # None = ZIP root
                        info,
                        self._marker(info, children, zip_fs),
                    )
                )

            return nodes

    @_io_retry
    def _ls(self, path: str) -> tuple[Info, ...]:
        logger.debug("ls: %s", path)

        try:
            return tuple(self.fs.ls(path, detail=True))
        except FileNotFoundError as exc:
            # Windowsのネットワークパス障害はretryさせる
            if getattr(exc, "winerror", None) in _NETWORK_WINERRORS:
                raise
            return ()
        except NotADirectoryError:
            return ()
        except PermissionError:
            logger.warning("Permission denied: %s", path)
            return ()

    def _marker(
        self,
        info: Info,
        children: Sequence[Info],
        fs: AbstractFileSystem,
    ) -> str | None:
        if self.marker_func is None:
            return None
        return self.marker_func(info, children, fs)

    @staticmethod
    def _node_type(info: Info) -> NodeType:
        value = info.get("type")
        if value in {"directory", "dir", "folder"}:
            return NodeType.DIRECTORY
        if value == "file":
            return NodeType.FILE
        return NodeType.OTHER

    @staticmethod
    def _is_zip(info: Info) -> bool:
        return FileSystemIndexer._node_type(info) == NodeType.FILE and str(
            info["name"]
        ).lower().endswith(".zip")

    @staticmethod
    def _mtime_ms_text(info: Info) -> str:
        value = (
            info.get("mtime")
            or info.get("LastModified")
            or info.get("last_modified")
            or info.get("date_time")
        )

        if isinstance(value, datetime):
            return str(round(value.timestamp() * 1000))
        if isinstance(value, tuple):
            return str(round(datetime(*value).timestamp() * 1000))
        if isinstance(value, (int, float)):
            return str(round(value * 1000))
        return "unknown"

    @classmethod
    def _fingerprint(cls, info: Info) -> str:
        digest = hashlib.blake2b(digest_size=8)
        for value in (
            FINGERPRINT_VERSION,
            cls._node_type(info),
            cls._mtime_ms_text(info),
            str(info.get("size", "")),
        ):
            if isinstance(value, bytes):
                data: bytes = value
            elif isinstance(value, NodeType):
                data = str(value.value).encode("utf-8")
            else:
                data = str(value).encode("utf-8")
            digest.update(len(data).to_bytes(8, "little"))
            digest.update(data)
        return digest.hexdigest()

    @staticmethod
    def _name(path: str) -> str:
        return path.rstrip("/\\").replace("\\", "/").rsplit("/", 1)[-1]
