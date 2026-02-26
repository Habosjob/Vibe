from __future__ import annotations

from datetime import datetime, timezone, timedelta

from moex_bond_screener.moex_client import AMORTIZATION_CHECKPOINT_VERSION
from run import _collect_forced_blacklist_emitters, _normalize_emitter_id, _prepare_amortization_checkpoint, _print_emitents_progress, _sanitize_date_fields


class _DummyProgress:
    def __init__(self) -> None:
        self.ticks: list[str] = []
        self.fractions: list[tuple[int, int, str]] = []

    def tick(self, message: str) -> None:
        self.ticks.append(message)

    def report_fraction(self, processed: int, total: int, message: str) -> None:
        self.fractions.append((processed, total, message))


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
            "processed": {"A": {"amortization_start_date": "2025-01-01", "flags": {}}, "": "2024-01-01", "B": None},
            "updated_at": updated_at,
            "completed": 1,
        }
    )

    assert invalidated is False
    assert is_fresh is True
    assert checkpoint == {
        "version": AMORTIZATION_CHECKPOINT_VERSION,
        "processed": {"A": {"amortization_start_date": "2025-01-01", "flags": {}}, "B": None},
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


def test_print_emitents_progress_reports_sample_descriptions() -> None:
    progress = _DummyProgress()

    _print_emitents_progress(
        {
            "phase": "sample_descriptions",
            "processed": 3,
            "total": 10,
            "message": "Сопоставление SECID -> EMITTER_ID",
        },
        progress,
    )

    assert progress.ticks == ["Сопоставление SECID -> EMITTER_ID"]
    assert progress.fractions == [(3, 10, "обработано карточек эмитентов")]


def test_print_emitents_progress_reports_emitter_profiles() -> None:
    progress = _DummyProgress()

    _print_emitents_progress(
        {
            "phase": "emitter_profiles",
            "processed": 8,
            "total": 324,
            "message": "Загрузка карточек эмитентов по EMITTER_ID",
        },
        progress,
    )

    assert progress.ticks == ["Загрузка карточек эмитентов по EMITTER_ID"]
    assert progress.fractions == [(8, 324, "обработано профилей эмитентов")]


def test_print_emitents_progress_reports_market_descriptions() -> None:
    progress = _DummyProgress()

    _print_emitents_progress(
        {
            "phase": "market_descriptions",
            "processed": 15,
            "total": 120,
            "message": "Запрашиваем description для market SECID без EMITTER_ID",
        },
        progress,
    )

    assert progress.ticks == ["Запрашиваем description для market SECID без EMITTER_ID"]
    assert progress.fractions == [(15, 120, "обработано market-description карточек")]


def test_sanitize_date_fields_removes_json_artifacts() -> None:
    bonds = [{
        "MATDATE": "2033-10-12 {'flags': {'ISQUALIFIEDINVESTORS': '0'}}",
        "Amortization_start_date": "amortization_start_date: 2027-09-27",
        "OFFERDATE": "24.10.2039",
    }]

    _sanitize_date_fields(bonds)

    assert bonds[0]["MATDATE"] == "2033-10-12"
    assert bonds[0]["Amortization_start_date"] == "2027-09-27"
    assert bonds[0]["OFFERDATE"] == "2039-10-24"


def test_normalize_emitter_id_handles_excel_float_suffix() -> None:
    assert _normalize_emitter_id("123.0") == "123"
    assert _normalize_emitter_id("123") == "123"
    assert _normalize_emitter_id("") == ""


class _DummyClient:
    def __init__(self, mapping: dict[str, str]) -> None:
        self.mapping = mapping

    def fetch_security_description(self, secid: str):
        emitter_id = self.mapping.get(secid, "")
        return ({"EMITTER_ID": emitter_id}, 0)


def test_collect_forced_blacklist_emitters_resolves_missing_ids_from_description() -> None:
    bonds = [
        {"SECID": "AAA", "HASDEFAULT": "1"},
        {"SECID": "BBB", "HASDEFAULT": "1", "EMITTER_ID": "222.0"},
        {"SECID": "CCC", "HASDEFAULT": "0", "EMITTER_ID": "333"},
    ]

    emitters, secid_map = _collect_forced_blacklist_emitters(
        bonds=bonds,
        secid_to_emitter_map={"AAA": ""},
        client=_DummyClient({"AAA": "111"}),
    )

    assert emitters == {"111", "222"}
    assert secid_map["AAA"] == "111"
