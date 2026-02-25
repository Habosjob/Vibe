from __future__ import annotations

from moex_bond_screener.moex_client import AMORTIZATION_CHECKPOINT_VERSION
from run import _prepare_amortization_checkpoint


def test_prepare_amortization_checkpoint_invalidates_legacy_version() -> None:
    checkpoint, invalidated = _prepare_amortization_checkpoint({"processed": {"A": ""}, "completed": False})

    assert invalidated is True
    assert checkpoint == {}


def test_prepare_amortization_checkpoint_normalizes_valid_payload() -> None:
    checkpoint, invalidated = _prepare_amortization_checkpoint(
        {
            "version": AMORTIZATION_CHECKPOINT_VERSION,
            "processed": {"A": "2025-01-01", "": "2024-01-01", "B": None},
            "completed": 1,
        }
    )

    assert invalidated is False
    assert checkpoint == {
        "version": AMORTIZATION_CHECKPOINT_VERSION,
        "processed": {"A": "2025-01-01", "B": ""},
        "completed": True,
    }
