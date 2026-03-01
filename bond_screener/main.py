from __future__ import annotations

from pathlib import Path

import yaml

from src.logging_setup import setup_logging
from src.pipeline import run_pipeline
from src.utils import ensure_dirs


def main() -> None:
    root = Path(__file__).resolve().parent
    ensure_dirs(
        [
            root / "config",
            root / "data",
            root / "cache" / "http",
            root / "cache" / "checkpoints",
            root / "logs",
            root / "source",
        ]
    )

    setup_logging(root / "logs" / "app.log")

    config_path = root / "config" / "config.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    run_pipeline(config, root)


if __name__ == "__main__":
    main()
