import time
from collections.abc import Sequence
from pathlib import Path

import polars as pl
import rich
from fsspec import AbstractFileSystem

from fsindexer import FileSystemIndexer, Info, set_logging

set_logging(True)

home = Path.home()
root = home / "Downloads"

sqlite_path = home / "Downloads_index.sqlite"
csv_path = home / "Downloads_index.csv"
if sqlite_path.exists():
    sqlite_path.unlink()


# example marker
def jpg_marker(
    info: Info,
    children: Sequence[Info],
    fs: AbstractFileSystem,
) -> str | None:
    if info.get("type") == "file" and str(info["name"]).lower().endswith(
        (".jpg", ".jpeg")
    ):
        return "jpg"
    return None


indexer = FileSystemIndexer(
    root=str(root),
    database=sqlite_path,
    marker_func=jpg_marker,
    always_scan_depth=1,
)
t = time.perf_counter()
indexer.scan()
print(f"1st scan: {time.perf_counter() - t:.3f} seconds")

t = time.perf_counter()
indexer.scan()
print(f"2nd scan: {time.perf_counter() - t:.3f} seconds")

print("----- marked files -----")
df = pl.DataFrame(indexer.iter_marked(), infer_schema_length=None)
rich.print(df)
df.write_csv(csv_path, include_bom=True)
