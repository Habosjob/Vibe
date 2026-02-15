from __future__ import annotations

import os
import tempfile
import time
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
    atomic_replace_with_retry(temp_path, out_path)


def atomic_replace_with_retry(
    src: Path,
    dst: Path,
    retries: int = 6,
    initial_backoff_s: float = 0.05,
) -> None:
    """Atomically replace ``dst`` with ``src`` with retries for transient Windows locks."""
    delay = initial_backoff_s
    for attempt in range(retries):
        try:
            os.replace(src, dst)
            return
        except PermissionError as exc:
            if attempt == retries - 1:
                raise PermissionError(
                    f"Failed to replace '{dst}' with '{src}': target file may be open in Excel"
                ) from exc
            time.sleep(delay)
            delay *= 2
