from pathlib import Path

from bond_screener.runtime import run


EXPECTED_CONFIGS = {
    "config.yml",
    "scenarios.yml",
    "allowlist.yml",
    "issuer_links.yml",
    "portfolio.yml",
}


def test_autocreate_configs_and_runtime_dirs(tmp_path: Path) -> None:
    summary, elapsed = run(tmp_path)

    assert elapsed >= 0
    assert summary.errors == 0

    for dirname in ("config", "out", "logs", "raw"):
        assert (tmp_path / dirname).is_dir()

    created_files = {p.name for p in (tmp_path / "config").iterdir() if p.is_file()}
    assert EXPECTED_CONFIGS.issubset(created_files)

    latest_log = tmp_path / "logs" / "latest.log"
    assert latest_log.exists()
    assert "Этапы выполнения" in latest_log.read_text(encoding="utf-8")
