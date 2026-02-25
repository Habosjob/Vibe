"""Оценка качества данных облигаций для итоговой выгрузки."""

from __future__ import annotations

from datetime import datetime
from typing import Any


CRITICAL_FIELDS = ("SECID",)
WARNING_FIELDS = ("ISIN", "SHORTNAME", "MATDATE")


def attach_data_status(bonds: list[dict[str, Any]]) -> None:
    """Добавляет в каждую бумагу поля DATA_STATUS и DATA_STATUS_REASON."""
    for bond in bonds:
        status, reason = evaluate_bond_data_status(bond)
        bond["DATA_STATUS"] = status
        bond["DATA_STATUS_REASON"] = reason


def evaluate_bond_data_status(bond: dict[str, Any]) -> tuple[str, str]:
    missing_critical = [field for field in CRITICAL_FIELDS if not str(bond.get(field) or "").strip()]
    if missing_critical:
        return "error", f"missing_critical:{','.join(missing_critical)}"

    missing_warning = [field for field in WARNING_FIELDS if not str(bond.get(field) or "").strip()]
    reasons: list[str] = []
    if missing_warning:
        reasons.append(f"missing:{','.join(missing_warning)}")

    matdate = str(bond.get("MATDATE") or "").strip()
    if matdate and not _is_iso_date(matdate):
        reasons.append("invalid_matdate")

    if reasons:
        return "warning", ";".join(reasons)
    return "ok", ""


def _is_iso_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False
