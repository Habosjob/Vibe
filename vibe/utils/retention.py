from __future__ import annotations

import logging
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def cleanup_old_dirs(base_dir: Path, keep_days: int, date_pattern: str = r"^\d{8}$") -> None:
    if keep_days < 1 or not base_dir.exists() or not base_dir.is_dir():
        return

    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=keep_days - 1))
    name_pattern = re.compile(date_pattern)

    for child in base_dir.iterdir():
        if not child.is_dir() or not name_pattern.match(child.name):
            continue

        try:
            dir_date = datetime.strptime(child.name, "%Y%m%d").date()
        except ValueError:
            continue

        if dir_date < cutoff:
            shutil.rmtree(child, ignore_errors=False)
            logger.info("Retention cleanup removed %s", child)


def cleanup_old_files(
    base_dir: Path,
    keep_days: int,
    filename_date_regex: str,
    date_format: str = "%Y%m%d",
) -> None:
    if keep_days < 1 or not base_dir.exists() or not base_dir.is_dir():
        return

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=keep_days - 1)
    name_pattern = re.compile(filename_date_regex)

    for child in base_dir.iterdir():
        if not child.is_file():
            continue

        match = name_pattern.match(child.name)
        if not match or not match.groups():
            continue

        try:
            file_date = datetime.strptime(match.group(1), date_format).date()
        except ValueError:
            continue

        if file_date < cutoff:
            child.unlink(missing_ok=True)
            logger.info("Retention cleanup removed %s", child)
