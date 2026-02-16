from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vibe.utils.retention import cleanup_old_dirs, cleanup_old_files


def test_cleanup_old_dirs_keeps_only_recent_dates(tmp_path):
    today = datetime.now(timezone.utc).date()
    old_name = (today - timedelta(days=9)).strftime("%Y%m%d")
    keep_name = (today - timedelta(days=2)).strftime("%Y%m%d")

    old_dir = tmp_path / old_name
    keep_dir = tmp_path / keep_name
    old_dir.mkdir()
    keep_dir.mkdir()

    cleanup_old_dirs(tmp_path, keep_days=7)

    assert not old_dir.exists()
    assert keep_dir.exists()


def test_cleanup_old_files_keeps_only_recent_files(tmp_path):
    today = datetime.now(timezone.utc).date()
    old_name = (today - timedelta(days=9)).strftime("%Y%m%d")
    keep_name = (today - timedelta(days=2)).strftime("%Y%m%d")

    old_file = tmp_path / f"bond_rates_{old_name}.csv"
    keep_file = tmp_path / f"bond_rates_{keep_name}.csv"
    old_file.write_text("x")
    keep_file.write_text("x")

    cleanup_old_files(tmp_path, keep_days=7, filename_date_regex=r"bond_rates_(\d{8})\.csv")

    assert not old_file.exists()
    assert keep_file.exists()
