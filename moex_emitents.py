# moex_emitents.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import pandas as pd

from SQL import SQLiteCache
from iss_client import IssClient
from moex_parsers import parse_iss_json_tables_safe, pick_first, description_to_kv


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt: Any) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return datetime.fromisoformat(str(dt))
    except Exception:
        return None


def emitent_is_fresh(row: Dict[str, Any], ttl_days: int) -> bool:
    if ttl_days <= 0:
        return False
    upd = _parse_iso(row.get("updated_utc") or row.get("UPDATED_UTC"))
    if not upd:
        return False
    return upd >= (_utc_now() - timedelta(days=int(ttl_days)))


def try_fetch_emitent(
    cache: SQLiteCache,
    client: IssClient,
    logger,
    emitter_id: int,
    *,
    secid_hint: Optional[str] = None,
    force_refresh: bool = False,
    emitent_ttl_days: int = 90,
    snippet_chars: int = 800,
) -> Optional[dict]:
    if not emitter_id:
        return None

    if not force_refresh:
        existing = cache.get_emitent(emitter_id)
        if existing and (existing.get("inn") or existing.get("title")) and emitent_is_fresh(existing, emitent_ttl_days):
            return existing

    def upsert_from_row(row: Dict[str, Any], raw_json: Optional[str]) -> None:
        cache.upsert_emitent(
            emitter_id=emitter_id,
            inn=pick_first(row, ["INN", "inn"]),
            title=pick_first(row, ["TITLE", "title", "NAME", "name"]),
            short_title=pick_first(row, ["SHORT_TITLE", "short_title", "SHORTNAME", "shortname"]),
            ogrn=pick_first(row, ["OGRN", "ogrn"]),
            okpo=pick_first(row, ["OKPO", "okpo"]),
            kpp=pick_first(row, ["KPP", "kpp"]),
            okved=pick_first(row, ["OKVED", "okved"]),
            address=pick_first(row, ["ADDRESS", "address", "LEGAL_ADDRESS", "legal_address"]),
            phone=pick_first(row, ["PHONE", "phone", "TEL", "tel"]),
            site=pick_first(row, ["SITE", "site", "WWW", "www", "URL", "url"]),
            email=pick_first(row, ["EMAIL", "email", "E_MAIL", "e_mail"]),
            raw_json=raw_json,
        )

    # /emitents/{id}.json
    res = client.get(f"/emitents/{emitter_id}.json", params={"iss.meta": "off", "lang": "ru"})
    if res.status == 200 and res.text:
        ct = (res.headers or {}).get("Content-Type", "")
        tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct, snippet_chars=snippet_chars, cache=cache)
        for _, df in tables.items():
            if df is None or df.empty:
                continue
            upsert_from_row(df.iloc[0].to_dict(), res.text)
            got = cache.get_emitent(emitter_id)
            if got and (got.get("inn") or got.get("title")):
                return got

    # fallback from securities description
    if secid_hint:
        r2 = client.get(f"/securities/{secid_hint}.json", params={"iss.meta": "off", "lang": "ru"})
        if r2.status == 200 and r2.text:
            ct2 = (r2.headers or {}).get("Content-Type", "")
            tables2 = parse_iss_json_tables_safe(r2.text, logger=logger, url=r2.url, content_type=ct2, snippet_chars=snippet_chars, cache=cache)
            desc = tables2.get("description", pd.DataFrame())
            kv = description_to_kv(desc)
            row2: Dict[str, Any] = {
                "INN": pick_first(kv, ["ИНН", "INN", "EMITENT_INN", "EMITTER_INN"]),
                "TITLE": pick_first(kv, ["ЭМИТЕНТ", "EMITENT", "EMITTER", "FULLNAME", "FULL_NAME", "NAME"]),
                "SHORT_TITLE": pick_first(kv, ["КРАТКОЕ НАИМЕНОВАНИЕ", "SHORTNAME", "SHORT_NAME"]),
                "OGRN": pick_first(kv, ["ОГРН", "OGRN"]),
                "OKPO": pick_first(kv, ["ОКПО", "OKPO"]),
                "KPP": pick_first(kv, ["КПП", "KPP"]),
                "OKVED": pick_first(kv, ["ОКВЭД", "OKVED"]),
                "ADDRESS": pick_first(kv, ["АДРЕС", "ADDRESS", "LEGAL_ADDRESS"]),
                "PHONE": pick_first(kv, ["ТЕЛЕФОН", "PHONE"]),
                "SITE": pick_first(kv, ["САЙТ", "SITE", "WWW", "URL"]),
                "EMAIL": pick_first(kv, ["EMAIL", "E-MAIL", "ПОЧТА"]),
            }
            upsert_from_row(row2, raw_json=None)
            got = cache.get_emitent(emitter_id)
            if got and (got.get("inn") or got.get("title")):
                return got

    return cache.get_emitent(emitter_id)