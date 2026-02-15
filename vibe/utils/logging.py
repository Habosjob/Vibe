from __future__ import annotations

import logging
from pathlib import Path

from vibe.utils.fs import ensure_parent_dir


def setup_logging(log_path: Path | None, level: str = "INFO") -> None:
    formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    if log_path is not None:
        ensure_parent_dir(log_path)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
