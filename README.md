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

## Versioning

Package versions are generated from Git by `hatch-vcs` at build/install time;
there is no version number to update in `pyproject.toml`.

- A clean release tag such as `v0.2.2` produces version `0.2.2`.
- Five commits after that tag produce `0.2.3.dev5+g<commit-hash>`.
- Uncommitted tracked changes add a date suffix (`.dYYYYMMDD`).
- Without version tags, versions start at `0.1.dev<commit-count>+g<commit-hash>`.

The baseline tag `v0.2.2` points to commit `2b015c5` (the version bump to
`0.2.2`). Builds need the Git history and tags (use a full checkout in CI).
Published source distributions preserve their version and can be built
without Git metadata.

### Quick release workflow

1. Once per clone, enable automatic annotated-tag pushes:

   ```bash
   git config --local push.followTags true
   ```

2. During development, commit and push normally. Commit-based development
   versions are generated automatically; do not edit a version number.

3. To release, commit all release changes, then create an **annotated** tag
   on that commit and push (replace `0.2.3` with the desired release version):

   ```bash
   git tag -a v0.2.3 -m "Release 0.2.3"
   git push origin main --follow-tags
   git ls-remote --tags origin v0.2.3
   uv build
   ```

   With `push.followTags=true`, a normal `git push` also sends annotated tags
   reachable from the pushed branch. Lightweight tags are not included.
   To send a specific tag explicitly, use `git push origin tag v0.2.3`.
   Pushing a tag makes it available on GitHub; it does not publish to PyPI.

Build with `uv build`. Read the installed version with:

```python
from importlib.metadata import version

print(version("fsindexer"))
```

After changing commits or tags, run `uv sync` to refresh the installed version.

## Usage

### Basic example

```python
from fsindexer import FileSystemIndexer

indexer = FileSystemIndexer(
    root="/path/to/scan",
    database="index.sqlite",
    scan_archives=True,
)
indexer.scan()

for node in indexer.iter_marked():
    print(node)
```

### Custom marker function

A marker function can inspect node metadata and return a label for nodes that should be marked:

```python
from fsindexer import FileSystemIndexer


def marker_func(info, children, fs):
    if info["type"] == "file" and info["name"].endswith(".csv"):
        return "csv file"
    return None

indexer = FileSystemIndexer(
    root="/path/to/scan",
    database="index.sqlite",
    marker_func=marker_func,
)
indexer.scan()
```

### Full scan vs. incremental scan

- `indexer.scan()` uses cached directory fingerprints to skip unchanged subtrees.
- `indexer.full_scan()` ignores fingerprints and rescans the entire tree.

### `always_scan_depth`

`always_scan_depth` controls how many directory levels are always listed on
each incremental scan, even when their fingerprints have not changed.
The root is depth `0` and is always listed.

The default fingerprint is a 16-character hexadecimal BLAKE2b hash
(8-byte digest) of the fingerprint format version (`fsindexer-v2`), node
type, modification time rounded to milliseconds, and size from filesystem
metadata. Recognized time fields are `mtime`, `LastModified`, `last_modified`,
and `date_time`; missing or unsupported times are recorded as `unknown`.
It does not read file contents or hash directory children, so changes that
leave these metadata values unchanged can be missed. `always_scan_depth`
bypasses this check for directories within the specified depth.

| Value | Directories always listed |
| --- | --- |
| `0` | The root only; child directories use fingerprint checks. |
| `1` (default) | The root and its immediate child directories. |
| `2` | The root, its children, and their child directories. |

Deeper directories are still scanned when their fingerprint changes or they
are new, provided their parent is visited. This is not a maximum scan depth.
Increasing the value helps detect changes within those levels when a
filesystem does not reliably update directory metadata, at the cost of more
directory listings. Changes below a skipped directory can remain undetected;
use `full_scan()` to bypass fingerprint checks throughout the tree, including
ZIPs.

```python
from fsindexer import FileSystemIndexer

indexer = FileSystemIndexer(
    root="/path/to/scan",
    database="index.sqlite",
    always_scan_depth=2,
)
indexer.scan()
```

Negative values raise `ValueError`. `root_dir_pattern` still filters which
root child directories are included. This setting applies to filesystem
directories; ZIP contents use their archive fingerprint check.
ZIP entry fingerprints are stored as `None`; when the ZIP file's fingerprint
changes, all its indexed contents are replaced. Changes inside a ZIP can be
missed if the archive's metadata remains unchanged.

## Public API

- `FileSystemIndexer(*, root, database, root_dir_pattern=None, marker_func=None, always_scan_depth=1, scan_archives=True)`
- `indexer.scan(*, full=False)`
- `indexer.full_scan()`
- `indexer.iter_marked()`

## Notes

- ZIP files are scanned only when `scan_archives=True`.
- `root_dir_pattern` filters root child directories; root-level files are still indexed.
- The package is intended as an experimental research project and may require additional testing before production use.

## License

This project is released under the MIT License. See `LICENSE` for details.
