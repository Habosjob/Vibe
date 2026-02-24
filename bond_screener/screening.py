from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from bond_screener.db import Instrument, InstrumentField


@dataclass(slots=True)
class ScreenRow:
    isin: str
    secid: str | None
    name: str | None
    bond_class: str
    maturity_date: date | None
    offer_date: date | None
    amort_date: date | None
    reasons: list[str]


def classify_bond_class(secid: str | None, name: str | None, tags_json: str | None) -> str:
    normalized_name = (name or "").upper()
    normalized_secid = (secid or "").upper()
    tags = _parse_tags(tags_json)

    if _is_ofz(normalized_secid, normalized_name, tags):
        return "OFZ"
    if _has_any(tags, {"subfed", "субфед", "region", "regional"}) or "СУБФЕД" in normalized_name:
        return "Subfed"
    if _has_any(tags, {"municipal", "muni", "муницип"}) or "МУНИЦ" in normalized_name:
        return "Muni"
    if _has_any(tags, {"corporate", "corp", "корп"}) or "ООО" in normalized_name or "ПАО" in normalized_name:
        return "Corp"
    return "Other"


def detect_ofz_variant(
    secid: str | None,
    name: str | None,
    tags_json: str | None,
    coupon_type: str | None,
) -> str | None:
    normalized_name = (name or "").upper()
    normalized_secid = (secid or "").upper()
    normalized_coupon = (coupon_type or "").upper()
    tags = _parse_tags(tags_json)

    if not _is_ofz(normalized_secid, normalized_name, tags):
        return None

    if _contains_any(normalized_name, ["ОФЗ-ИН", "OFZ-IN", "ИНФЛЯЦ"]):
        return "OFZ-IN"

    pk_markers = ["ОФЗ-ПК", "OFZ-PK", "ПЛАВАЮЩ"]
    if _contains_any(normalized_name, pk_markers):
        return "OFZ-PK"
    if _contains_any(normalized_coupon, ["FLOAT", "ПЛАВА"]):
        return "OFZ-PK"
    if _has_any(tags, {"ofz-pk", "floating_coupon", "float_coupon", "плавающий"}):
        return "OFZ-PK"

    # Эвристика по SECID для ОФЗ-ПК: в большинстве случаев это серия SU29***.
    if re.match(r"^SU29\d{3}RMFS\d$", normalized_secid):
        return "OFZ-PK"

    return "OFZ"


def build_screen_rows(session_factory: sessionmaker, today: date | None = None) -> tuple[list[ScreenRow], list[ScreenRow]]:
    as_of = today or date.today()

    with session_factory() as session:
        instruments = session.scalars(select(Instrument)).all()
        raw_fields = session.scalars(select(InstrumentField)).all()

        fields_by_isin: dict[str, dict[str, str | None]] = {}
        for field in raw_fields:
            fields_by_isin.setdefault(field.isin, {})[field.field] = field.value

        pass_rows: list[ScreenRow] = []
        drop_rows: list[ScreenRow] = []

        for instrument in instruments:
            fields = fields_by_isin.get(instrument.isin, {})
            bond_class = classify_bond_class(instrument.secid, instrument.name or instrument.shortname, instrument.tags_json)
            ofz_variant = detect_ofz_variant(
                instrument.secid,
                instrument.name or instrument.shortname,
                instrument.tags_json,
                fields.get("coupon_type"),
            )

            _upsert_bond_class_field(session, instrument.isin, bond_class)

            maturity_date = _parse_date(fields.get("maturity_date"))
            offer_date = _parse_date(fields.get("offer_date") or fields.get("next_offer_date"))
            amort_date = _parse_date(fields.get("amort_date") or fields.get("amort_start_date"))

            reasons: list[str] = []
            if maturity_date and maturity_date < as_of:
                reasons.append("maturity_in_past")
            if maturity_date and (maturity_date - as_of).days < 365:
                reasons.append("maturity_lt_365")
            if offer_date and (offer_date - as_of).days < 365:
                reasons.append("offer_lt_365")
            if amort_date and (amort_date - as_of).days < 365:
                reasons.append("amort_lt_365")
            if bond_class == "OFZ" and ofz_variant == "OFZ-PK":
                reasons.append("ofz_pk_excluded")

            row = ScreenRow(
                isin=instrument.isin,
                secid=instrument.secid,
                name=instrument.name or instrument.shortname,
                bond_class=bond_class,
                maturity_date=maturity_date,
                offer_date=offer_date,
                amort_date=amort_date,
                reasons=reasons,
            )
            if reasons:
                drop_rows.append(row)
            else:
                pass_rows.append(row)

        session.commit()

    return pass_rows, drop_rows


def export_screen_to_excel(pass_rows: list[ScreenRow], drop_rows: list[ScreenRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pass_df = _rows_to_frame(pass_rows)
    drop_df = _rows_to_frame(drop_rows)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        pass_df.to_excel(writer, sheet_name="screen_pass", index=False)
        drop_df.to_excel(writer, sheet_name="screen_drop", index=False)

        for sheet_name in ["screen_pass", "screen_drop"]:
            ws = writer.book[sheet_name]
            ws.auto_filter.ref = ws.dimensions
            for column in ws.columns:
                values = [str(cell.value) if cell.value is not None else "" for cell in column]
                max_len = min(max(len(v) for v in values) + 2, 60)
                ws.column_dimensions[column[0].column_letter].width = max_len


def _rows_to_frame(rows: list[ScreenRow]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "isin": row.isin,
                "secid": row.secid,
                "name": row.name,
                "bond_class": row.bond_class,
                "maturity_date": row.maturity_date,
                "offer_date": row.offer_date,
                "amort_date": row.amort_date,
                "reasons": ",".join(row.reasons),
            }
            for row in rows
        ]
    )


def _upsert_bond_class_field(session, isin: str, bond_class: str) -> None:
    row = session.scalar(
        select(InstrumentField).where(InstrumentField.isin == isin, InstrumentField.field == "bond_class")
    )
    if row is None:
        row = InstrumentField(isin=isin, field="bond_class")
        session.add(row)
    row.value = bond_class
    row.source = "classifier_ofz_minimal"
    row.confidence = 0.6
    row.fetched_at = datetime.utcnow()


def _parse_tags(tags_json: str | None) -> set[str]:
    if not tags_json:
        return set()
    try:
        parsed = json.loads(tags_json)
    except json.JSONDecodeError:
        return {tags_json.lower()}

    tags: set[str] = set()
    if isinstance(parsed, list):
        for item in parsed:
            tags.add(str(item).strip().lower())
    elif isinstance(parsed, dict):
        for key, value in parsed.items():
            tags.add(str(key).strip().lower())
            tags.add(str(value).strip().lower())
    else:
        tags.add(str(parsed).strip().lower())
    return tags


def _has_any(tags: set[str], expected: set[str]) -> bool:
    return any(tag in tags for tag in expected)


def _contains_any(text: str, needles: list[str]) -> bool:
    return any(needle in text for needle in needles)


def _is_ofz(secid: str, name: str, tags: set[str]) -> bool:
    if secid.startswith("SU"):
        return True
    if _contains_any(name, ["ОФЗ", "OFZ"]):
        return True
    return _has_any(tags, {"ofz", "federal", "gov", "government", "гос", "офз"})


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None
