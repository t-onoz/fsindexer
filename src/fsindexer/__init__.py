import logging
from typing import IO

from ._fsindexer import FileSystemIndexer, Info, MarkedNode

__all__ = ["FileSystemIndexer", "Info", "MarkedNode", "set_logging"]

_handler: logging.Handler | None = None


def set_logging(
    enabled: bool = True,
    *,
    level: int = logging.DEBUG,
    stream: IO[str] | None = None,
) -> None:
    global _handler

    logger = logging.getLogger(__name__)

    if _handler is not None:
        logger.removeHandler(_handler)
        _handler.close()
        _handler = None

    if not enabled:
        return

    logger.setLevel(level)

    _handler = logging.StreamHandler(stream)
    _handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    logger.addHandler(_handler)
