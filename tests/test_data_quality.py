from __future__ import annotations

from moex_bond_screener.data_quality import attach_data_status, evaluate_bond_data_status


def test_evaluate_bond_data_status_ok() -> None:
    status, reason = evaluate_bond_data_status(
        {"SECID": "A", "ISIN": "RU1", "SHORTNAME": "Bond", "MATDATE": "2030-01-01"}
    )
    assert status == "ok"
    assert reason == ""


def test_evaluate_bond_data_status_warning_and_error() -> None:
    warning_status, warning_reason = evaluate_bond_data_status({"SECID": "A", "ISIN": "", "SHORTNAME": "Bond"})
    error_status, error_reason = evaluate_bond_data_status({"SECID": "", "ISIN": "RU1"})

    assert warning_status == "warning"
    assert "missing:" in warning_reason
    assert error_status == "error"
    assert error_reason == "missing_critical:SECID"


def test_attach_data_status_adds_fields() -> None:
    bonds = [{"SECID": "A", "ISIN": "RU1", "SHORTNAME": "Bond", "MATDATE": "bad-date"}]

    attach_data_status(bonds)

    assert bonds[0]["DATA_STATUS"] == "warning"
    assert "invalid_matdate" in bonds[0]["DATA_STATUS_REASON"]
