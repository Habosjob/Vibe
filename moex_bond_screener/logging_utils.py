"""Настройка логирования в logs/latest.log."""

from __future__ import annotations

import logging
from pathlib import Path


def setup_logging() -> logging.Logger:
    Path("logs").mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("moex_bond_screener")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler("logs/latest.log", mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
