from __future__ import annotations

from scripts.stage0.run import run_stage0
from scripts.stage1.run import run_stage1
from scripts.stage2.run import run_stage2
from scripts.stage3.run import run_stage3


def main() -> None:
    """Точка входа проекта."""
    run_stage0()
    run_stage1()
    run_stage2()
    run_stage3()


if __name__ == "__main__":
    main()
