from __future__ import annotations

from datetime import datetime, timezone, timedelta

from moex_bond_screener.moex_client import AMORTIZATION_CHECKPOINT_VERSION
from run import _prepare_amortization_checkpoint


def test_prepare_amortization_checkpoint_invalidates_legacy_version() -> None:
    checkpoint, invalidated, is_fresh = _prepare_amortization_checkpoint({"processed": {"A": ""}, "completed": False})

    assert invalidated is True
    assert is_fresh is False
    assert checkpoint == {}


def test_prepare_amortization_checkpoint_normalizes_valid_payload() -> None:
    updated_at = datetime.now(timezone.utc).isoformat()
    checkpoint, invalidated, is_fresh = _prepare_amortization_checkpoint(
        {
            "version": AMORTIZATION_CHECKPOINT_VERSION,
            "processed": {"A": "2025-01-01", "": "2024-01-01", "B": None},
            "updated_at": updated_at,
            "completed": 1,
        }
    )

    assert invalidated is False
    assert is_fresh is True
    assert checkpoint == {
        "version": AMORTIZATION_CHECKPOINT_VERSION,
        "processed": {"A": "2025-01-01", "B": ""},
        "cache_stats": {"date": "", "hits": 0, "misses": 0},
        "updated_at": updated_at,
        "completed": True,
    }


def test_prepare_amortization_checkpoint_keeps_cache_stats() -> None:
    updated_at = datetime.now(timezone.utc).isoformat()
    checkpoint, invalidated, is_fresh = _prepare_amortization_checkpoint(
        {
            "version": AMORTIZATION_CHECKPOINT_VERSION,
            "processed": {"A": "2025-01-01"},
            "cache_stats": {"date": "2026-01-01", "hits": 2, "misses": 3},
            "updated_at": updated_at,
            "completed": True,
        }
    )

    assert invalidated is False
    assert is_fresh is True
    assert checkpoint["cache_stats"] == {"date": "2026-01-01", "hits": 2, "misses": 3}


def test_prepare_amortization_checkpoint_invalidates_stale_cache() -> None:
    stale_updated_at = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
    checkpoint, invalidated, is_fresh = _prepare_amortization_checkpoint(
        {
            "version": AMORTIZATION_CHECKPOINT_VERSION,
            "processed": {"A": "2025-01-01"},
            "updated_at": stale_updated_at,
            "completed": True,
        }
    )

    assert invalidated is True
    assert is_fresh is False
    assert checkpoint == {}
