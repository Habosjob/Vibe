from __future__ import annotations

from scripts.stage0.run import run_stage0


def main() -> None:
    """Точка входа проекта. Пока запускается только Stage0."""
    run_stage0()
    # TODO: добавить Stage1..Stage5 по мере реализации.


if __name__ == "__main__":
    main()
