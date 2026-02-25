from __future__ import annotations

from moex_bond_screener.config import load_config


def test_load_config_uses_sqlite_default_when_file_missing(tmp_path) -> None:
    config = load_config(tmp_path / "missing_config.yml")
    assert config.storage_backend == "sqlite"


def test_load_config_reads_yaml_with_slots_dataclass(tmp_path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text("storage_backend: json\nexclusions_state_dir: custom_state\n", encoding="utf-8")

    config = load_config(config_path)

    assert config.storage_backend == "json"
    assert config.exclusions_state_dir == "custom_state"
