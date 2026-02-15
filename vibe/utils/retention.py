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
