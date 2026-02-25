"""Фильтрация облигаций с учетом срока исключения и инкрементального состояния."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

DATE_RULES: list[tuple[str, str]] = [
    ("BUYBACKDATE", "buyback_lt_1y"),
    ("OFFERDATE", "offer_lt_1y"),
    ("CALLOPTIONDATE", "calloption_lt_1y"),
    ("MATDATE", "mat_lt_1y"),
]
PERMANENT_EXCLUDE_UNTIL = "permanent"
AMORTIZATION_RULE_NAME = "amortization_started_or_lt_1y_permanent"


@dataclass(slots=True)
class ExclusionResult:
    eligible_bonds: list[dict[str, Any]]
    active_exclusions: dict[str, dict[str, str]]
    excluded_by_rule: dict[str, int]
    restored_after_expiration: int
    skipped_by_active_exclusion: int
    skipped_by_active_rule: dict[str, int]


class BondExclusionFilter:
    """Применяет правила исключения по датам и хранит причины для отчета."""

    def __init__(self, days_threshold: int) -> None:
        self.days_threshold = days_threshold

    def apply(
        self,
        bonds: list[dict[str, Any]],
        previous_exclusions: dict[str, dict[str, str]],
        today: date | None = None,
    ) -> ExclusionResult:
        current_day = today or date.today()
        eligible_bonds: list[dict[str, Any]] = []
        active_exclusions: dict[str, dict[str, str]] = {}
        excluded_by_rule = {rule_name: 0 for _, rule_name in DATE_RULES}
        excluded_by_rule[AMORTIZATION_RULE_NAME] = 0
        restored_after_expiration = 0
        skipped_by_active_exclusion = 0
        skipped_by_active_rule: dict[str, int] = {}

        for bond in bonds:
            secid = str(bond.get("SECID") or "").strip()
            if not secid:
                eligible_bonds.append(bond)
                continue

            prev_exclusion = previous_exclusions.get(secid)
            prev_until_raw = str((prev_exclusion or {}).get("exclude_until", ""))
            if prev_exclusion and prev_until_raw == PERMANENT_EXCLUDE_UNTIL:
                rule_name = str(prev_exclusion.get("rule", "manual"))
                active_exclusions[secid] = {
                    "rule": rule_name,
                    "exclude_until": PERMANENT_EXCLUDE_UNTIL,
                }
                skipped_by_active_exclusion += 1
                skipped_by_active_rule[rule_name] = skipped_by_active_rule.get(rule_name, 0) + 1
                continue

            if prev_exclusion and self._parse_date(prev_until_raw):
                prev_until = self._parse_date(prev_until_raw)
                if prev_until and prev_until > current_day:
                    rule_name = str(prev_exclusion.get("rule", "manual"))
                    active_exclusions[secid] = {
                        "rule": rule_name,
                        "exclude_until": prev_until.isoformat(),
                    }
                    skipped_by_active_exclusion += 1
                    skipped_by_active_rule[rule_name] = skipped_by_active_rule.get(rule_name, 0) + 1
                    continue

            if prev_exclusion:
                restored_after_expiration += 1

            matched = False
            for field_name, rule_name in DATE_RULES:
                target_date = self._parse_date(str(bond.get(field_name) or ""))
                if not target_date:
                    continue

                days_left = (target_date - current_day).days
                if 0 <= days_left < self.days_threshold:
                    active_exclusions[secid] = {
                        "rule": rule_name,
                        "exclude_until": target_date.isoformat(),
                    }
                    excluded_by_rule[rule_name] += 1
                    matched = True
                    break

            if matched:
                continue

            amortization_start = self._parse_date(
                str(
                    bond.get("Amortization_start_date")
                    or bond.get("AMORTIZATION_START_DATE")
                    or bond.get("AMORTIZATIONSTARTDATE")
                    or ""
                )
            )
            if amortization_start:
                days_left = (amortization_start - current_day).days
                if days_left < self.days_threshold:
                    active_exclusions[secid] = {
                        "rule": AMORTIZATION_RULE_NAME,
                        "exclude_until": PERMANENT_EXCLUDE_UNTIL,
                    }
                    excluded_by_rule[AMORTIZATION_RULE_NAME] += 1
                    continue

            eligible_bonds.append(bond)

        return ExclusionResult(
            eligible_bonds=eligible_bonds,
            active_exclusions=active_exclusions,
            excluded_by_rule=excluded_by_rule,
            restored_after_expiration=restored_after_expiration,
            skipped_by_active_exclusion=skipped_by_active_exclusion,
            skipped_by_active_rule=skipped_by_active_rule,
        )

    @staticmethod
    def _parse_date(value: str) -> date | None:
        if not value or value == "0000-00-00":
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
