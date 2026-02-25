from __future__ import annotations

import os
import time

from moex_bond_screener.raw_store import RawStore


def test_cleanup_removes_old_files(tmp_path):
    store = RawStore(str(tmp_path))
    old_file = tmp_path / "old.json"
    old_file.write_text("{}", encoding="utf-8")

    stale_time = time.time() - 10_000
    os.utime(old_file, (stale_time, stale_time))

    store.cleanup(ttl_hours=1, max_size_mb=100)

    assert not old_file.exists()


def test_cleanup_by_size(tmp_path):
    store = RawStore(str(tmp_path))

    f1 = tmp_path / "1.json"
    f2 = tmp_path / "2.json"
    f1.write_text("x" * 1024, encoding="utf-8")
    f2.write_text("x" * 1024, encoding="utf-8")

    old = time.time() - 100
    new = time.time()
    os.utime(f1, (old, old))
    os.utime(f2, (new, new))

    store.cleanup(ttl_hours=100, max_size_mb=0)

    assert not f1.exists()
