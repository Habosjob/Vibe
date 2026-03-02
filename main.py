from pathlib import Path

from config import REQUIRED_DIRS


def ensure_directories(paths: tuple[Path, ...]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    ensure_directories(REQUIRED_DIRS)
    print("Проект инициализирован")


if __name__ == "__main__":
    main()
