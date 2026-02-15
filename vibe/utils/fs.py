from __future__ import annotations

import os
import tempfile
from pathlib import Path


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_bytes_atomic(content: bytes, out_path: Path) -> None:
    ensure_parent_dir(out_path)
    with tempfile.NamedTemporaryFile(dir=out_path.parent, delete=False) as tmp:
        tmp.write(content)
        tmp.flush()
        os.fsync(tmp.fileno())
        temp_path = Path(tmp.name)
    os.replace(temp_path, out_path)
