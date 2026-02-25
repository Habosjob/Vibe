from __future__ import annotations

from datetime import date

from moex_bond_screener.exclusion_rules import AMORTIZATION_RULE_NAME, BondExclusionFilter, PERMANENT_EXCLUDE_UNTIL


def test_exclusion_filter_applies_rules_in_priority_order() -> None:
    bonds = [
        {
            "SECID": "A",
            "BUYBACKDATE": "2026-06-01",
            "OFFERDATE": "2026-05-01",
            "CALLOPTIONDATE": "2026-04-01",
            "MATDATE": "2026-03-01",
        },
        {
            "SECID": "B",
            "BUYBACKDATE": "2030-01-01",
            "OFFERDATE": "2026-02-01",
            "CALLOPTIONDATE": "2026-03-01",
            "MATDATE": "2026-04-01",
        },
    ]
    result = BondExclusionFilter(days_threshold=365).apply(
        bonds=bonds,
        previous_exclusions={},
        today=date(2026, 1, 1),
    )

    assert result.eligible_bonds == []
    assert result.active_exclusions["A"]["rule"] == "buyback_lt_1y"
    assert result.active_exclusions["A"]["exclude_until"] == "2026-06-01"
    assert result.active_exclusions["B"]["rule"] == "offer_lt_1y"
    assert result.excluded_by_rule["buyback_lt_1y"] == 1
    assert result.excluded_by_rule["offer_lt_1y"] == 1
    assert result.excluded_by_rule["calloption_lt_1y"] == 0
    assert result.excluded_by_rule["mat_lt_1y"] == 0


def test_exclusion_filter_uses_saved_exclusion_until_expiration() -> None:
    bonds = [{"SECID": "A", "MATDATE": "2030-01-01"}, {"SECID": "B", "MATDATE": "2030-01-01"}]
    previous_exclusions = {
        "A": {"rule": "mat_lt_1y", "exclude_until": "2026-08-01"},
        "B": {"rule": "mat_lt_1y", "exclude_until": "2025-12-01"},
    }

    result = BondExclusionFilter(days_threshold=365).apply(
        bonds=bonds,
        previous_exclusions=previous_exclusions,
        today=date(2026, 1, 1),
    )

    assert [bond["SECID"] for bond in result.eligible_bonds] == ["B"]
    assert result.skipped_by_active_exclusion == 1
    assert result.restored_after_expiration == 1
    assert "A" in result.active_exclusions
    assert "B" not in result.active_exclusions


def test_exclusion_filter_excludes_amortization_lt_1y_permanently() -> None:
    bonds = [
        {"SECID": "A", "Amortization_start_date": "2026-02-01"},
        {"SECID": "B", "AMORTIZATION_START_DATE": "2025-12-31"},
        {"SECID": "C", "Amortization_start_date": "2028-01-01"},
    ]

    result = BondExclusionFilter(days_threshold=365).apply(
        bonds=bonds,
        previous_exclusions={},
        today=date(2026, 1, 1),
    )

    assert [bond["SECID"] for bond in result.eligible_bonds] == ["C"]
    assert result.active_exclusions["A"] == {
        "rule": AMORTIZATION_RULE_NAME,
        "exclude_until": PERMANENT_EXCLUDE_UNTIL,
    }
    assert result.active_exclusions["B"] == {
        "rule": AMORTIZATION_RULE_NAME,
        "exclude_until": PERMANENT_EXCLUDE_UNTIL,
    }
    assert result.excluded_by_rule[AMORTIZATION_RULE_NAME] == 2


def test_exclusion_filter_keeps_permanent_exclusion_on_rerun() -> None:
    bonds = [{"SECID": "A", "Amortization_start_date": "2030-01-01"}]
    previous_exclusions = {
        "A": {
            "rule": AMORTIZATION_RULE_NAME,
            "exclude_until": PERMANENT_EXCLUDE_UNTIL,
        }
    }

    result = BondExclusionFilter(days_threshold=365).apply(
        bonds=bonds,
        previous_exclusions=previous_exclusions,
        today=date(2026, 1, 1),
    )

    assert result.eligible_bonds == []
    assert result.skipped_by_active_exclusion == 1
    assert result.skipped_by_active_rule[AMORTIZATION_RULE_NAME] == 1
    assert result.active_exclusions["A"]["exclude_until"] == PERMANENT_EXCLUDE_UNTIL
