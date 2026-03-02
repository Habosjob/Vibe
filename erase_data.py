from __future__ import annotations

import shutil
from pathlib import Path

import config


TARGET_DIRS: tuple[Path, ...] = (
    config.DB_DIR,
    config.CACHE_DIR,
    config.RAW_DIR,
    config.LOGS_DIR,
)


def wipe_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for item in path.iterdir():
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        else:
            item.unlink(missing_ok=True)


def main() -> int:
    for directory in TARGET_DIRS:
        wipe_directory(directory)
        print(f"Очищено: {directory}")
    print("Сброс данных завершён.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
