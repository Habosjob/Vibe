from __future__ import annotations

from scripts.stage0.run import run_stage0
from scripts.stage1.run import run_stage1


def main() -> None:
    """Точка входа проекта."""
    run_stage0()
    run_stage1()


if __name__ == "__main__":
    main()
