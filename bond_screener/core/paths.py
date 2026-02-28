from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    logs_dir: Path
    db_file: Path
    cache_http_dir: Path
    cache_checkpoints_dir: Path
    source_xlsx_dir: Path
    source_parquet_dir: Path
    config_file: Path
    reset_file: Path

    def ensure_dirs(self) -> None:
        for path in [
            self.logs_dir,
            self.db_file.parent,
            self.cache_http_dir,
            self.cache_checkpoints_dir,
            self.source_xlsx_dir,
            self.source_parquet_dir,
        ]:
            path.mkdir(parents=True, exist_ok=True)
