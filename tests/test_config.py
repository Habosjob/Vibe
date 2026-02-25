from __future__ import annotations

from moex_bond_screener.config import load_config


def test_load_config_uses_sqlite_default_when_file_missing(tmp_path) -> None:
    config = load_config(tmp_path / "missing_config.yml")
    assert config.storage_backend == "sqlite"
