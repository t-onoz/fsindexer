# fsindexer

`fsindexer` is an experimental file indexer built on `fsspec` and SQLite. It scans a filesystem or supported URL, indexes directories, files, and ZIP archive contents, and stores the results in a local SQLite database.

## Features

- Traverse local and remote filesystems supported by `fsspec`
- Index directories, files, and ZIP archive entries
- Use directory fingerprints to skip unchanged subtrees
- Apply custom markers to identify meaningful nodes
- Query scanned results using SQLite or tools like `polars`

## Installation

Clone the repository and install with `pip`:

```bash
pip install .
```

## Usage

### Basic example

```python
from fsindexer import FileIndexer

with FileIndexer(
    root="/path/to/scan",
    database="index.sqlite",
    scan_archives=True,
) as indexer:
    indexer.scan()

    for node in indexer.iter_marked():
        print(node)
```

### Custom marker function

A marker function can inspect node metadata and return a label for nodes that should be marked:

```python
from fsindexer import FileIndexer


def marker_func(info, children, fs):
    if info["type"] == "file" and info["name"].endswith(".csv"):
        return "csv file"
    return None

with FileIndexer(
    root="/path/to/scan",
    database="index.sqlite",
    marker_func=marker_func,
) as indexer:
    indexer.scan()
```

### Full scan vs. incremental scan

- `indexer.scan()` uses cached directory fingerprints to skip unchanged subtrees.
- `indexer.full_scan()` ignores fingerprints and rescans the entire tree.

## Public API

- `FileIndexer(root, database, root_dir_pattern=None, marker_func=None, fingerprint_depth=1, scan_archives=True)`
- `indexer.scan(depth=None, full=False)`
- `indexer.full_scan(depth=None)`
- `indexer.iter_marked()`

## Notes

- ZIP files are scanned only when `scan_archives=True`.
- Root-level files are excluded from scanning unless they are within an included directory.
- The package is intended as an experimental research project and may require additional testing before production use.

## License

This project is released under the MIT License. See `LICENSE` for details.
