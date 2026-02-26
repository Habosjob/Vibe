"""Обогащение облигаций данными с analytics.dohod.ru."""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from html import unescape
from xml.etree import ElementTree
from typing import Any, Callable

import requests

from .config import AppConfig
from .raw_store import RawStore

DohodProgressCallback = Callable[[dict[str, Any]], None]
DohodCheckpointSaver = Callable[[dict[str, Any]], None]
DOHOD_CHECKPOINT_VERSION = 3

LABEL_VALUE_RE = r"{label}\s*</[^>]+>\s*<[^>]+[^>]*>(.*?)</"
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL)
NUMBER_RE = r"[+-]?\d+(?:[.,]\d+)?"
TABLE_RE = re.compile(r"<table[^>]*>(.*?)</table>", re.IGNORECASE | re.DOTALL)
INDEX_RE = re.compile(
    r"(RUONIA|R[-_\s]?UONIA|CBR_RATE|KEY_RATE|Z[-_\s]?CURVE[-_\s]?RUS|"
    r"КЛЮЧЕВАЯ\s+СТАВКА(?:\s+(?:ЦБ|БАНКА\s+РОССИИ))?|КС\s*ЦБ(?:\s*РФ)?|"
    r"КБД\s+ОФЗ|КРИВ[А-ЯA-Z\s]+ОФЗ)"
    r"(?:\s*(?:\(|:))?\s*"
    r"([+\-−]\s*\d+(?:[.,]\d+)?)?",
    re.IGNORECASE,
)
TENOR_RE = re.compile(r"сроком\s+погашения\s+(\d+)\s+лет", re.IGNORECASE)

DL_PAIR_RE = re.compile(
    r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>",
    re.IGNORECASE | re.DOTALL,
)
SCRIPT_ASK_RE = re.compile(r"\"(?:ask|ask_price|askPrice)\"\s*[:=]\s*\"?([+-]?\d+(?:[.,]\d+)?)", re.IGNORECASE)
SCRIPT_YTM_RE = re.compile(r"\"(?:ytm_date|ytmDate|date_ytm)\"\s*[:=]\s*\"?(\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)
SCRIPT_EVENT_RE = re.compile(r"\"(?:event|event_name|nearest_event)\"\s*[:=]\s*\"([^\"]+)\"", re.IGNORECASE)
YTM_NEARBY_RE = re.compile(
    r"ytm[^\d]{0,40}(\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})|"
    r"(\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})[^\n]{0,30}ytm",
    re.IGNORECASE,
)

@dataclass(slots=True)
class DohodBondPayload:
    ask_price: float | None = None
    index_name: str = ""
    index_spread: float = 0.0
    index_tenor_years: int | None = None
    event_name: str = ""
    ytm_date: str = ""
    real_price: float | None = None
    coupon_type: str = ""
    lesenka: str = ""
    formula_source: str = ""
    offer_source: str = ""


@dataclass(slots=True)
class DohodEnrichmentStats:
    bonds_total: int = 0
    cache_hits: int = 0
    requested: int = 0
    realprice_added: int = 0
    realprice_updated: int = 0
    coupon_added: int = 0
    coupon_updated: int = 0
    coupon_skipped_no_base: int = 0
    offer_added: int = 0
    offer_updated: int = 0
    parse_empty_payloads: int = 0
    corpbonds_realprice_added: int = 0
    corpbonds_coupontype_added: int = 0
    corpbonds_lesenka_added: int = 0
    corpbonds_offerdate_added: int = 0
    corpbonds_coupon_formula_applied: int = 0


class DohodEnricher:
    def __init__(self, config: AppConfig, logger: logging.Logger, raw_store: RawStore | None = None) -> None:
        self.config = config
        self.logger = logger
        self.raw_store = raw_store
        self.session = requests.Session()
        self._request_lock = threading.Lock()
        self._next_request_ts = 0.0
        self._thread_local = threading.local()
        self._missing_base_warning_counts: dict[tuple[str, float], int] = {}
        self.last_stats = DohodEnrichmentStats()
        self._corpbonds_secid_by_isin: dict[str, str] = {}

    def enrich_bonds(
        self,
        bonds: list[dict[str, Any]],
        checkpoint_data: dict[str, Any] | None = None,
        checkpoint_saver: DohodCheckpointSaver | None = None,
        progress_callback: DohodProgressCallback | None = None,
    ) -> int:
        self.last_stats = DohodEnrichmentStats()
        self._corpbonds_secid_by_isin: dict[str, str] = {}
        self._missing_base_warning_counts = {}
        checkpoint = self._normalize_checkpoint(checkpoint_data or {})
        previous_payload = checkpoint.get("bonds", {})
        processed: dict[str, dict[str, Any]] = dict(previous_payload) if isinstance(previous_payload, dict) else {}
        index_values = self._resolve_index_values(checkpoint)
        index_changed = self._has_index_changed(index_values, checkpoint.get("index_values", {}))
        fresh_cache = self._is_checkpoint_fresh(checkpoint)

        identifier_to_bonds: dict[str, list[dict[str, Any]]] = {}
        secid_by_identifier: dict[str, str] = {}
        for bond in bonds:
            primary_identifier = self._resolve_bond_identifier(bond)
            if not primary_identifier:
                continue
            secid = self._resolve_corpbonds_secid(bond)
            if secid:
                secid_by_identifier.setdefault(primary_identifier, secid)
            identifier_to_bonds.setdefault(primary_identifier, []).append(bond)

        self._corpbonds_secid_by_isin = dict(secid_by_identifier)
        pending: list[str] = []
        for identifier in identifier_to_bonds:
            if fresh_cache and not index_changed and identifier in processed:
                cached_payload = processed.get(identifier)
                if self._is_cached_payload_usable(cached_payload):
                    self._apply_cached(identifier_to_bonds[identifier], processed[identifier], index_values)
                    continue
            pending.append(identifier)

        self.last_stats.bonds_total = len(identifier_to_bonds)
        self.last_stats.cache_hits = len(identifier_to_bonds) - len(pending)
        self.last_stats.requested = len(pending)

        errors = 0
        processed_count = len(identifier_to_bonds) - len(pending)
        checkpoint_save_every = max(1, int(getattr(self.config, "dohod_checkpoint_save_every", 25) or 25))
        last_checkpoint_saved_count = processed_count
        if progress_callback:
            progress_callback({"processed": processed_count, "total": len(identifier_to_bonds), "message": "Кэш ДОХОД применен"})

        workers = max(1, int(getattr(self.config, "dohod_workers", 8)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._fetch_with_fallback, identifier, None): identifier
                for identifier in pending
            }
            for future in as_completed(futures):
                identifier = futures[future]
                try:
                    payload, request_errors = future.result()
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("Ошибка обработки ДОХОД instrument=%s: %s", identifier, exc)
                    payload, request_errors = DohodBondPayload(), 1

                if request_errors == 0 and _is_payload_empty(payload):
                    self.last_stats.parse_empty_payloads += 1
                    request_errors = 1
                    self.logger.warning("Пустой payload ДОХОД instrument=%s: карточка получена, но данные не извлечены", identifier)

                errors += request_errors
                if request_errors == 0:
                    serialized = {
                        "ask_price": payload.ask_price,
                        "real_price": payload.real_price,
                        "index_name": payload.index_name,
                        "index_spread": payload.index_spread,
                        "index_tenor_years": payload.index_tenor_years,
                        "event_name": payload.event_name,
                        "ytm_date": payload.ytm_date,
                        "coupon_type": payload.coupon_type,
                        "lesenka": payload.lesenka,
                        "formula_source": payload.formula_source,
                        "offer_source": payload.offer_source,
                    }
                    processed[identifier] = serialized
                    self._apply_cached(identifier_to_bonds[identifier], serialized, index_values)

                processed_count += 1
                should_save_checkpoint = (processed_count - last_checkpoint_saved_count) >= checkpoint_save_every
                if checkpoint_saver and should_save_checkpoint:
                    checkpoint_saver(
                        {
                            "version": DOHOD_CHECKPOINT_VERSION,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                            "completed": False,
                            "index_values": index_values,
                            "bonds": processed,
                        }
                    )
                    last_checkpoint_saved_count = processed_count
                if progress_callback:
                    progress_callback({"processed": processed_count, "total": len(identifier_to_bonds)})

        if checkpoint_saver:
            checkpoint_saver(
                {
                    "version": DOHOD_CHECKPOINT_VERSION,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "completed": True,
                    "index_values": index_values,
                    "bonds": processed,
                }
            )

        self._log_missing_base_summary_if_needed()

        return errors

    def _fetch_and_parse(self, isin: str) -> tuple[DohodBondPayload, int]:
        for attempt in range(1, self.config.retries + 1):
            try:
                response = self._get_with_rate_limit(
                    f"https://analytics.dohod.ru/bond/{isin}",
                    timeout=self.config.timeout_seconds,
                    delay_seconds=float(getattr(self.config, "dohod_request_delay_seconds", 0.05)),
                )
                response.raise_for_status()
                html = response.text
                if self.raw_store and self.config.raw_dump_enabled:
                    self.raw_store.dump_html(f"dohod_{isin}.html", html)
                payload = self.parse_bond_payload(html)
                if self.raw_store and _is_payload_empty(payload):
                    self.raw_store.dump_html(f"dohod_empty_{isin}.html", html)
                secid = self._corpbonds_secid_by_isin.get(isin, "")
                corpbonds_payload = self._fetch_and_parse_corpbonds(secid) if secid else DohodBondPayload()
                if corpbonds_payload.real_price is not None:
                    payload.real_price = corpbonds_payload.real_price
                elif payload.real_price is None and payload.ask_price is not None and payload.ask_price > 0:
                    payload.real_price = payload.ask_price
                if corpbonds_payload.coupon_type:
                    payload.coupon_type = corpbonds_payload.coupon_type
                if corpbonds_payload.lesenka:
                    payload.lesenka = corpbonds_payload.lesenka
                if corpbonds_payload.index_name:
                    payload.index_name = corpbonds_payload.index_name
                    payload.index_spread = corpbonds_payload.index_spread
                    payload.index_tenor_years = corpbonds_payload.index_tenor_years
                    payload.formula_source = "corpbonds"
                if corpbonds_payload.ytm_date:
                    payload.ytm_date = corpbonds_payload.ytm_date
                    payload.event_name = "оферта"
                    payload.offer_source = "corpbonds"
                return payload, 0
            except requests.RequestException as error:
                self.logger.warning("Ошибка запроса ДОХОД instrument=%s попытка=%s: %s", isin, attempt, error)
                if attempt == self.config.retries:
                    return DohodBondPayload(), 1
                time.sleep(float(getattr(self.config, "dohod_request_delay_seconds", 0.05)) * attempt)

        return DohodBondPayload(), 1

    def _fetch_and_parse_corpbonds(self, secid: str) -> DohodBondPayload:
        try:
            response = self._get_with_rate_limit(
                f"https://corpbonds.ru/bond/{secid}",
                timeout=self.config.timeout_seconds,
                delay_seconds=float(getattr(self.config, "dohod_request_delay_seconds", 0.05)),
            )
            response.raise_for_status()
            html = response.text
            if self.raw_store and self.config.raw_dump_enabled:
                self.raw_store.dump_html(f"corpbonds_{secid}.html", html)
            return self.parse_corpbonds_payload(html)
        except requests.RequestException as error:
            self.logger.warning("Ошибка запроса CorpBonds instrument=%s: %s", secid, error)
            return DohodBondPayload()


    def _fetch_with_fallback(self, primary_identifier: str, secondary_identifier: str | None = None) -> tuple[DohodBondPayload, int]:
        payload, errors = self._fetch_and_parse(primary_identifier)
        if secondary_identifier and (errors > 0 or _is_payload_empty(payload)):
            fallback_payload, fallback_errors = self._fetch_and_parse(secondary_identifier)
            if fallback_errors == 0 and not _is_payload_empty(fallback_payload):
                return fallback_payload, 0
            errors += fallback_errors
        return payload, errors

    @staticmethod
    def _resolve_bond_identifier(bond: dict[str, Any]) -> str:
        """Сервис ДОХОД принимает только ISIN."""
        return str(bond.get("ISIN") or "").strip()

    @staticmethod
    def _resolve_corpbonds_secid(bond: dict[str, Any]) -> str:
        """CorpBonds принимает только SECID."""
        return str(bond.get("SECID") or "").strip()

    @staticmethod
    def _resolve_secondary_identifier(bond: dict[str, Any], primary_identifier: str) -> str:
        _ = bond
        _ = primary_identifier
        return ""

    @staticmethod
    def parse_bond_payload(html: str) -> DohodBondPayload:
        extracted = _extract_label_map(html)
        price_value = extracted.get("Цена (last/bid/ask)", "")
        ask_price = _parse_ask_price(price_value)

        index_value = extracted.get("Привязка к индексу", "")
        formula_value = extracted.get("Описание формулы изменяемого купона/номинала", "")
        index_name, spread = _parse_index_and_spread(index_value)
        tenor_years: int | None = None
        if index_name == "Z_CURVE_RUS":
            tenor_years = _parse_tenor_years(formula_value)

        if ask_price is None:
            ask_price = _parse_ask_price_from_html(html)

        if not index_name:
            index_name, spread = _parse_index_and_spread(_strip_html(html))

        event_name = extracted.get("Событие в ближ. дату", "").strip().lower()
        if not event_name:
            event_name = _extract_event_name_from_html(html)

        ytm_date = _to_iso_date(extracted.get("Дата, к которой рассчит. YTM", ""))
        if not ytm_date:
            ytm_date = _extract_ytm_date_from_html(html)

        corpbonds_values = _extract_corpbonds_values(html)
        real_price = _parse_corpbonds_price(corpbonds_values.get("Цена последняя", "") or corpbonds_values.get("Цена", ""))
        coupon_type = corpbonds_values.get("Тип купона", "")
        lesenka = corpbonds_values.get("Купон лесенкой", "")
        formula_value = corpbonds_values.get("Формула купона", "")
        if formula_value:
            formula_index_name, formula_spread = _parse_index_and_spread(formula_value)
            if formula_index_name:
                index_name = formula_index_name
                spread = formula_spread
                if index_name == "Z_CURVE_RUS":
                    tenor_years = _parse_tenor_years(formula_value)
        offer_date_raw = corpbonds_values.get("Дата ближайшей оферты", "")
        if offer_date_raw and offer_date_raw.strip().lower() not in {"нет", "нет данных", "no", "n/a"}:
            ytm_date = _to_iso_date(offer_date_raw)
            event_name = "оферта"

        return DohodBondPayload(ask_price=ask_price, index_name=index_name, index_spread=spread, index_tenor_years=tenor_years, event_name=event_name, ytm_date=ytm_date, real_price=real_price, coupon_type=coupon_type, lesenka=lesenka, formula_source="corpbonds" if formula_value else "", offer_source="corpbonds" if offer_date_raw and ytm_date else "")

    @staticmethod
    def parse_corpbonds_payload(html: str) -> DohodBondPayload:
        values = _extract_corpbonds_values(html)
        real_price = _parse_corpbonds_price(values.get("Цена последняя", "") or values.get("Цена", ""))
        coupon_type = values.get("Тип купона", "")
        lesenka = values.get("Купон лесенкой", "")
        formula_value = values.get("Формула купона", "")
        index_name, spread = _parse_index_and_spread(formula_value)
        tenor_years = _parse_tenor_years(formula_value) if index_name == "Z_CURVE_RUS" else None
        offer_date_raw = values.get("Дата ближайшей оферты", "")
        ytm_date = ""
        event_name = ""
        if offer_date_raw and offer_date_raw.strip().lower() not in {"нет", "нет данных", "no", "n/a"}:
            ytm_date = _to_iso_date(offer_date_raw)
            event_name = "оферта"
        return DohodBondPayload(index_name=index_name, index_spread=spread, index_tenor_years=tenor_years, event_name=event_name, ytm_date=ytm_date, real_price=real_price, coupon_type=coupon_type, lesenka=lesenka, formula_source="corpbonds" if formula_value else "", offer_source="corpbonds" if ytm_date else "")

    def _resolve_index_values(self, checkpoint: dict[str, Any]) -> dict[str, float]:
        live_values = self._fetch_live_index_values()
        previous = checkpoint.get("index_values", {})
        normalized = {
            "RUONIA": live_values.get("RUONIA") or _as_float_or_none((previous or {}).get("RUONIA")) or 0.0,
            "CBR_RATE": live_values.get("CBR_RATE") or _as_float_or_none((previous or {}).get("CBR_RATE")) or 0.0,
            "Z_CURVE_RUS": live_values.get("Z_CURVE_RUS") or _as_float_or_none((previous or {}).get("Z_CURVE_RUS")) or 0.0,
        }
        for key, value in {**(previous or {}), **live_values}.items():
            normalized_key = _normalize_index_name(str(key))
            parsed = _as_float_or_none(value)
            if parsed is None:
                continue
            normalized[normalized_key] = parsed
        return normalized

    def _fetch_live_index_values(self) -> dict[str, float]:
        index_values: dict[str, float] = {}

        key_rate = self._fetch_cbr_metric(getattr(self.config, "cbr_key_rate_url", ""), metric_name="CBR_RATE")
        if key_rate is not None:
            index_values["CBR_RATE"] = key_rate

        ruonia = self._fetch_cbr_metric(getattr(self.config, "cbr_ruonia_url", ""), metric_name="RUONIA")
        if ruonia is not None:
            index_values["RUONIA"] = ruonia

        z_curve_values = self._fetch_cbr_z_curve_values()
        if not z_curve_values:
            z_curve_values = self._fetch_moex_z_curve_values()
        index_values.update(z_curve_values)
        return index_values

    def _fetch_cbr_metric(self, url: str, metric_name: str) -> float | None:
        target_url = str(url or "").strip()
        if not target_url:
            return None

        timeout = int(getattr(self.config, "cbr_key_rate_timeout_seconds", 10) or 10)
        try:
            response = self.session.get(target_url, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            self.logger.warning("Не удалось получить %s из ЦБ (%s): %s", metric_name, target_url, exc)
            return None

        try:
            payload = response.json()
            value = _extract_numeric_value(payload)
        except (ValueError, AttributeError):
            value = None

        if value is None and metric_name == "RUONIA":
            value = _extract_cbr_ruonia_value(response.text)

        if value is None:
            value = _extract_numeric_value(response.text)

        if value is None or value <= 0 or value > 100:
            self.logger.warning("Не удалось распарсить %s из ЦБ (%s)", metric_name, target_url)
            return None

        self.logger.info("%s обновлена по первоисточнику: %.4f", metric_name, value)
        return value

    def _fetch_moex_z_curve_values(self) -> dict[str, float]:
        url = str(getattr(self.config, "z_curve_moex_url", "") or "").strip()
        if not url:
            return {}

        timeout = int(getattr(self.config, "cbr_key_rate_timeout_seconds", 10) or 10)
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            self.logger.warning("Не удалось получить Z_CURVE_RUS с MOEX (%s): %s", url, exc)
            return {}

        curve_points = _extract_z_curve_points(payload)
        if not curve_points:
            self.logger.warning("MOEX вернул пустые данные Z_CURVE_RUS (%s)", url)
            return {}

        result: dict[str, float] = {}
        for tenor_years, rate in curve_points.items():
            result[f"Z_CURVE_RUS_{tenor_years}Y"] = rate

        result["Z_CURVE_RUS"] = result.get("Z_CURVE_RUS_1Y") or next(iter(result.values()))
        self.logger.info("Z_CURVE_RUS обновлена по MOEX: %s точек", len(curve_points))
        return result

    def _fetch_cbr_z_curve_values(self) -> dict[str, float]:
        url = str(getattr(self.config, "z_curve_cbr_url", "") or "").strip()
        if not url:
            return {}

        timeout = int(getattr(self.config, "cbr_key_rate_timeout_seconds", 10) or 10)
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            self.logger.warning("Не удалось получить Z_CURVE_RUS из ЦБ (%s): %s", url, exc)
            return {}

        curve_points = _extract_cbr_z_curve_points(response.text)
        if not curve_points:
            self.logger.warning("ЦБ вернул пустые данные Z_CURVE_RUS (%s)", url)
            return {}

        result: dict[str, float] = {f"Z_CURVE_RUS_{tenor}Y": value for tenor, value in curve_points.items()}
        result["Z_CURVE_RUS"] = result.get("Z_CURVE_RUS_1Y") or next(iter(result.values()))
        self.logger.info("Z_CURVE_RUS обновлена по первоисточнику ЦБ: %s точек", len(curve_points))
        return result

    @staticmethod
    def _has_index_changed(current: dict[str, float], previous: dict[str, Any]) -> bool:
        for key in ("RUONIA", "CBR_RATE", "Z_CURVE_RUS"):
            if abs(float(current.get(key, 0.0)) - float((previous or {}).get(key, 0.0))) > 1e-9:
                return True
        return False

    @staticmethod
    def _is_checkpoint_fresh(checkpoint: dict[str, Any]) -> bool:
        raw = checkpoint.get("updated_at")
        if not isinstance(raw, str):
            return False
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - dt <= timedelta(hours=24)

    @staticmethod
    def _normalize_checkpoint(checkpoint: dict[str, Any]) -> dict[str, Any]:
        if checkpoint.get("version") != DOHOD_CHECKPOINT_VERSION:
            return {}
        return checkpoint

    @staticmethod
    def _is_cached_payload_usable(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        real_price = payload.get("real_price")
        index_name = str(payload.get("index_name") or "").strip()
        ytm_date = str(payload.get("ytm_date") or "").strip()
        event_name = str(payload.get("event_name") or "").strip()
        if isinstance(real_price, (int, float)) and float(real_price) > 0:
            return True
        return bool(index_name or ytm_date or event_name)

    def _apply_cached(self, bonds: list[dict[str, Any]], payload: dict[str, Any], index_values: dict[str, float]) -> None:
        real_price = payload.get("real_price")
        index_name = str(payload.get("index_name") or "")
        index_spread = float(payload.get("index_spread") or 0.0)
        index_tenor_years = payload.get("index_tenor_years")
        event_name = str(payload.get("event_name") or "")
        ytm_date = str(payload.get("ytm_date") or "")
        coupon_type = str(payload.get("coupon_type") or "").strip()
        lesenka = str(payload.get("lesenka") or "").strip()
        formula_source = str(payload.get("formula_source") or "").strip()
        offer_source = str(payload.get("offer_source") or "").strip()

        for bond in bonds:
            if isinstance(real_price, (int, float)) and float(real_price) > 0:
                new_real_price = float(real_price)
                old_real_price = bond.get("RealPrice")
                if old_real_price in (None, ""):
                    self.last_stats.realprice_added += 1
                    self.last_stats.corpbonds_realprice_added += 1
                elif old_real_price != new_real_price:
                    self.last_stats.realprice_updated += 1
                bond["RealPrice"] = new_real_price

            if coupon_type:
                if not str(bond.get("CouponType") or "").strip():
                    self.last_stats.corpbonds_coupontype_added += 1
                bond["CouponType"] = coupon_type

            if lesenka and lesenka.lower() not in {"нет", "нет данных"}:
                if not str(bond.get("Lesenka") or "").strip():
                    self.last_stats.corpbonds_lesenka_added += 1
                bond["Lesenka"] = lesenka

            coupon_raw = str(bond.get("COUPONPERCENT") or "").strip()
            base_rate = float(index_values.get(index_name, 0.0))
            if index_name == "Z_CURVE_RUS" and index_tenor_years:
                base_rate = float(index_values.get(f"Z_CURVE_RUS_{index_tenor_years}Y", base_rate))
            if index_name and base_rate <= 0:
                self.last_stats.coupon_skipped_no_base += 1
                self._log_missing_base_warning(index_name, index_spread, bond)
                continue

            if _should_enrich_coupon(
                coupon_raw=coupon_raw,
                index_name=index_name,
                base_rate=base_rate,
                index_spread=index_spread,
                approx_flag=bool(bond.get("_COUPONPERCENT_APPROX")),
            ):
                new_coupon = round(base_rate + index_spread, 4)
                old_coupon = _as_float_or_none(bond.get("COUPONPERCENT"))
                if coupon_raw in ("", "-", "—", "нет", "n/a", "na", "none", "null", "nan"):
                    self.last_stats.coupon_added += 1
                elif old_coupon is None:
                    self.last_stats.coupon_added += 1
                elif abs(old_coupon - new_coupon) > 1e-9:
                    self.last_stats.coupon_updated += 1
                bond["COUPONPERCENT"] = new_coupon
                bond["_COUPONPERCENT_APPROX"] = True
                if formula_source == "corpbonds":
                    self.last_stats.corpbonds_coupon_formula_applied += 1

            offer_date = str(bond.get("OFFERDATE") or "")
            mat_date = str(bond.get("MATDATE") or "")
            if _should_enrich_offer(offer_date, ytm_date, mat_date, event_name):
                if ytm_date != mat_date:
                    if offer_date:
                        self.last_stats.offer_updated += 1
                    else:
                        self.last_stats.offer_added += 1
                    bond["OFFERDATE"] = ytm_date
                    if offer_source == "corpbonds":
                        self.last_stats.corpbonds_offerdate_added += 1

    def _get_with_rate_limit(self, url: str, timeout: int, delay_seconds: float) -> requests.Response:
        sleep_for = 0.0
        delay = max(0.0, delay_seconds)
        if delay > 0:
            with self._request_lock:
                now = time.monotonic()
                sleep_for = max(0.0, self._next_request_ts - now)
                reserve_from = max(now, self._next_request_ts)
                self._next_request_ts = reserve_from + delay
        if sleep_for > 0:
            time.sleep(sleep_for)
        session = self._get_thread_session()
        return session.get(url, timeout=timeout)

    def _get_thread_session(self) -> requests.Session:
        if threading.current_thread() is threading.main_thread():
            return self.session
        thread_session = getattr(self._thread_local, "session", None)
        if thread_session is None:
            thread_session = requests.Session()
            self._thread_local.session = thread_session
        return thread_session

    def _log_missing_base_warning(self, index_name: str, index_spread: float, bond: dict[str, Any]) -> None:
        key = (index_name, round(float(index_spread), 6))
        with self._request_lock:
            seen_count = self._missing_base_warning_counts.get(key, 0)
            self._missing_base_warning_counts[key] = seen_count + 1

        if seen_count > 0:
            return

        self.logger.warning(
            "Пропуск расчета COUPONPERCENT: нет базовой ставки для index=%s spread=%.4f isin=%s secid=%s (дальше одинаковые случаи агрегируются)",
            index_name,
            index_spread,
            str(bond.get("ISIN") or "").strip(),
            str(bond.get("SECID") or "").strip(),
        )

    def _log_missing_base_summary_if_needed(self) -> None:
        if not self._missing_base_warning_counts:
            return

        aggregated = ", ".join(
            f"{index}@{spread:.4f}: {count}"
            for (index, spread), count in sorted(self._missing_base_warning_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
        )
        self.logger.warning(
            "COUPONPERCENT пропущен из-за отсутствия базовой ставки: всего=%s, уникальных комбинаций=%s [%s]",
            self.last_stats.coupon_skipped_no_base,
            len(self._missing_base_warning_counts),
            aggregated,
        )


def _extract_label_map(html: str) -> dict[str, str]:
    labels = [
        "Цена (last/bid/ask)",
        "Привязка к индексу",
        "Описание формулы изменяемого купона/номинала",
        "Событие в ближ. дату",
        "Дата, к которой рассчит. YTM",
    ]
    result: dict[str, str] = {}

    for row_html in ROW_RE.findall(html):
        cells = CELL_RE.findall(row_html)
        if len(cells) < 2:
            continue
        target_label = _match_target_label(_strip_html(cells[0]))
        if not target_label or target_label in result:
            continue
        result[target_label] = _strip_html(cells[1])

    if len(result) == len(labels):
        return result

    # fallback для старой/нестандартной верстки
    for label in labels:
        if label in result:
            continue
        pattern = re.compile(LABEL_VALUE_RE.format(label=re.escape(label)), re.IGNORECASE | re.DOTALL)
        match = pattern.search(html)
        if not match:
            continue
        result[label] = _strip_html(match.group(1))

    if len(result) < len(labels):
        # На части карточек используется верстка через список определений (dt/dd) вместо таблицы.
        for raw_label, raw_value in DL_PAIR_RE.findall(html):
            target_label = _match_target_label(_strip_html(raw_label))
            if not target_label or target_label in result:
                continue
            result[target_label] = _strip_html(raw_value)

    if len(result) < len(labels):
        loose = _extract_label_map_loose(html)
        for label, value in loose.items():
            result.setdefault(label, value)

    return result


def _extract_label_map_loose(html: str) -> dict[str, str]:
    text = _strip_html(html)
    result: dict[str, str] = {}

    price_match = re.search(
        r"цена[^\n]{0,60}?last[^\n]{0,20}?bid[^\n]{0,20}?ask[^\n]{0,80}?((?:[+-]?\d+(?:[.,]\d+)?)(?:\s*/\s*[+-]?\d+(?:[.,]\d+)?){0,2})",
        text,
        re.IGNORECASE,
    )
    if price_match:
        result["Цена (last/bid/ask)"] = price_match.group(1)

    index_match = re.search(
        r"привязк[аи]\s+к\s+индекс[ау][^A-Z]{0,20}(RUONIA|CBR_RATE|Z_CURVE_RUS[^\s,;]*)",
        text,
        re.IGNORECASE,
    )
    if index_match:
        tail = text[index_match.start() : index_match.start() + 80]
        tail_match = re.search(r"(RUONIA|CBR_RATE|Z_CURVE_RUS)\s*([+-]\s*\d+(?:[.,]\d+)?)?", tail, re.IGNORECASE)
        if tail_match:
            result["Привязка к индексу"] = "".join(part for part in tail_match.groups() if part)

    event_match = re.search(r"событие\s+в\s+ближ[^\n]{0,80}", text, re.IGNORECASE)
    if event_match:
        result["Событие в ближ. дату"] = event_match.group(0)

    date_match = re.search(r"дата[^\n]{0,40}ytm[^\n]{0,20}(\d{2}\.\d{2}\.\d{4})", text, re.IGNORECASE)
    if date_match:
        result["Дата, к которой рассчит. YTM"] = date_match.group(1)

    return result


def _normalize_label(raw: str) -> str:
    normalized = raw.strip().lower().replace("ё", "е")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _match_target_label(raw_label: str) -> str:
    normalized = _normalize_label(raw_label)
    condensed = normalized.replace(".", "").replace(":", "").replace(",", "")

    if "цена" in condensed and "last" in condensed and "bid" in condensed and "ask" in condensed:
        return "Цена (last/bid/ask)"
    if "привязка" in condensed and "индекс" in condensed:
        return "Привязка к индексу"
    if "описание" in condensed and "формул" in condensed and "купон" in condensed:
        return "Описание формулы изменяемого купона/номинала"
    if "событие" in condensed and "ближ" in condensed:
        return "Событие в ближ. дату"
    if "дата" in condensed and "ytm" in condensed:
        return "Дата, к которой рассчит. YTM"
    return ""


def _strip_html(raw: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", raw)
    clean = unescape(clean)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def _parse_ask_price(raw: str) -> float | None:
    if not raw:
        return None
    parts = re.findall(NUMBER_RE, raw)
    if not parts:
        return None
    # обычно формат last/bid/ask (берем ask), но на некоторых карточках доступно только одно-два числа
    candidate = parts[2] if len(parts) >= 3 else parts[-1]
    try:
        parsed = float(candidate.replace(",", "."))
        return parsed if parsed > 0 else None
    except ValueError:
        return None


def _parse_index_and_spread(raw: str) -> tuple[str, float]:
    if not raw:
        return "", 0.0
    match = INDEX_RE.search(raw)
    if not match:
        return "", 0.0
    name = _normalize_index_name(str(match.group(1) or ""))
    spread_raw = str(match.group(2) or "0")
    spread = float(spread_raw.replace("−", "-").replace(" ", "").replace(",", ".")) if spread_raw else 0.0
    return name, spread


def _normalize_index_name(raw: str) -> str:
    normalized = raw.upper().replace("Ё", "Е")
    normalized = normalized.replace("-", "_")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if normalized.startswith("Z_CURVE_RUS_"):
        return normalized.replace(" ", "")
    if "RUONIA" in normalized or "R_UONIA" in normalized:
        return "RUONIA"
    if "CBR_RATE" in normalized or "KEY_RATE" in normalized or "КЛЮЧЕВАЯ СТАВКА" in normalized or "КС ЦБ" in normalized:
        return "CBR_RATE"
    if "Z_CURVE_RUS" in normalized or "Z CURVE RUS" in normalized or "КБД ОФЗ" in normalized or "КРИВ" in normalized:
        return "Z_CURVE_RUS"
    return normalized




def _extract_corpbonds_values(html: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for row_html in ROW_RE.findall(html):
        cells = CELL_RE.findall(row_html)
        if len(cells) < 2:
            continue
        label = _canonicalize_corpbonds_label(_strip_html(cells[0]))
        value = _strip_html(cells[1])
        if not label or label in values:
            continue
        values[label] = value
    return values


def _canonicalize_corpbonds_label(raw_label: str) -> str:
    label = _normalize_label(raw_label)
    if not label:
        return ""

    if "цена послед" in label:
        return "Цена последняя"
    if label.startswith("цена"):
        return "Цена"
    if "тип купона" in label:
        return "Тип купона"
    if "купон лесенкой" in label:
        return "Купон лесенкой"
    if "формула купона" in label:
        return "Формула купона"
    if "дата ближайшей оферты" in label or "дата оферты" in label or "ближайшая оферта" in label:
        return "Дата ближайшей оферты"
    return raw_label.strip()


def _parse_corpbonds_price(raw: str) -> float | None:
    value = str(raw or '').strip().lower()
    if not value or value in {'нет данных', 'нет', 'n/a', 'na'}:
        return None
    parsed = _as_float_or_none(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed
def _extract_index_base_rate_from_html(html: str, index_name: str) -> float | None:
    if not index_name:
        return None

    aliases: dict[str, tuple[str, ...]] = {
        "RUONIA": ("RUONIA", "R-UONIA", "R UONIA"),
        "CBR_RATE": ("CBR_RATE", "KEY_RATE", "КЛЮЧЕВАЯ СТАВКА", "КС ЦБ"),
        "Z_CURVE_RUS": ("Z_CURVE_RUS", "Z-CURVE-RUS", "КБД ОФЗ", "КРИВАЯ ОФЗ"),
    }
    alias_pattern = "|".join(re.escape(alias) for alias in aliases.get(index_name, (index_name,)))
    number_pattern = r"([+-]?\d+(?:[.,]\d+)?)"

    text = _strip_html(html).upper().replace("Ё", "Е")
    text = re.sub(r"\s+", " ", text)

    patterns = [
        rf"(?:ЗНАЧЕНИ[ЕЯ]|УРОВЕН[ЬЯ]|СТАВК[АИ]).{{0,40}}(?:{alias_pattern}).{{0,15}}{number_pattern}",
        rf"(?:{alias_pattern}).{{0,25}}(?:СОСТАВЛЯЕТ|СОСТАВИЛА|РАВНА|РАВНО|НА УРОВНЕ|=|:)\s*{number_pattern}",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        value = _as_float_or_none(match.group(1))
        if value is not None and value > 0:
            return value
    return None




def _extract_numeric_value(raw: Any) -> float | None:
    if isinstance(raw, dict):
        for key in ("value", "rate", "key_rate", "close", "cbr_rate", "ruonia"):
            if key in raw:
                parsed = _as_float_or_none(raw.get(key))
                if parsed is not None:
                    return parsed
        for value in raw.values():
            parsed = _extract_numeric_value(value)
            if parsed is not None:
                return parsed
        return None

    if isinstance(raw, list):
        for item in reversed(raw):
            parsed = _extract_numeric_value(item)
            if parsed is not None:
                return parsed
        return None

    if isinstance(raw, (int, float)):
        return float(raw)

    text = str(raw or "").strip()
    if not text:
        return None

    table_value = _extract_cbr_table_metric(text)
    if table_value is not None:
        return table_value

    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError:
        root = None

    if root is not None:
        values: list[float] = []
        for node in root.iter():
            parsed = _as_float_or_none(node.text)
            if parsed is not None:
                values.append(parsed)
        candidates = [value for value in values if 0 < value <= 100]
        if candidates:
            return candidates[-1]

    numbers = re.findall(NUMBER_RE, text)
    if not numbers:
        return None
    filtered = [parsed for parsed in (_as_float_or_none(number) for number in numbers) if parsed is not None and 0 < parsed <= 100]
    return filtered[-1] if filtered else None




def _extract_cbr_ruonia_value(html: str) -> float | None:
    table = _extract_table_by_headers(html, ("Дата ставки", "Ставка RUONIA"))
    if not table:
        return None

    for row in table:
        row_title = (row[0] if row else "").lower().replace("ё", "е")
        if "ставка ruonia" not in row_title:
            continue
        for cell in reversed(row[1:]):
            value = _as_float_or_none(cell)
            if value is not None and 0 < value <= 100:
                return value
    return None


def _extract_cbr_table_metric(html: str) -> float | None:
    table = _extract_table_by_headers(html, ("Дата", "Ставка"))
    if not table:
        return None

    for row in table:
        if len(row) < 2:
            continue
        value = _as_float_or_none(row[1])
        if value is not None and 0 < value <= 100:
            return value
    return None


def _extract_cbr_z_curve_points(html: str) -> dict[int, float]:
    table = _extract_table_by_headers(html, ("Дата", "0,25", "0,5", "0,75", "1"))
    if not table:
        return {}

    tenors = [0.25, 0.5, 0.75, 1, 2, 3, 5, 7, 10, 15, 20, 30]
    latest_row = next((row for row in table if len(row) >= len(tenors) + 1), None)
    if latest_row is None:
        return {}

    points: dict[int, float] = {}
    for idx, tenor in enumerate(tenors, start=1):
        value = _as_float_or_none(latest_row[idx] if idx < len(latest_row) else None)
        if tenor < 1 or value is None or value <= 0:
            continue
        points[int(round(tenor))] = value
    return points


def _extract_table_by_headers(html: str, required_headers: tuple[str, ...]) -> list[list[str]]:
    for table_html in TABLE_RE.findall(html):
        rows = []
        for row_html in ROW_RE.findall(table_html):
            cells = [_strip_html(cell) for cell in CELL_RE.findall(row_html)]
            if cells:
                rows.append(cells)
        if not rows:
            continue

        flat_headers = " ".join(" ".join(row) for row in rows[:3]).lower().replace("ё", "е")
        if all(header.lower().replace("ё", "е") in flat_headers for header in required_headers):
            return rows
    return []


def _extract_z_curve_points(payload: dict[str, Any]) -> dict[int, float]:
    if not isinstance(payload, dict):
        return {}

    securities = payload.get("securities")
    if not isinstance(securities, dict):
        return {}

    columns = [str(column).upper() for column in securities.get("columns", [])]
    data = securities.get("data", [])
    if not isinstance(data, list):
        return {}

    term_index = next((idx for idx, name in enumerate(columns) if name in {"TERM", "YEAR", "YEARS", "DURATION"}), None)
    value_index = next((idx for idx, name in enumerate(columns) if name in {"VALUE", "YIELD", "RATE", "VAL"}), None)
    if term_index is None or value_index is None:
        return {}

    points: dict[int, float] = {}
    for row in data:
        if not isinstance(row, list):
            continue
        tenor = _as_float_or_none(row[term_index] if term_index < len(row) else None)
        value = _as_float_or_none(row[value_index] if value_index < len(row) else None)
        if tenor is None or value is None or value <= 0:
            continue
        points[int(round(tenor))] = value

    return points

def _parse_tenor_years(raw: str) -> int | None:
    if not raw:
        return None
    match = TENOR_RE.search(raw)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _to_iso_date(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value.lower() in {"нет", "нет данных", "n/a", "na"}:
        return ""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_ask_price_from_html(html: str) -> float | None:
    script_match = SCRIPT_ASK_RE.search(html)
    if script_match:
        try:
            parsed = float(script_match.group(1).replace(",", "."))
            return parsed if parsed > 0 else None
        except ValueError:
            pass

    text = _strip_html(html)
    ask_match = re.search(r"\bask\b[^0-9]{0,20}([+-]?\d+(?:[.,]\d+)?)", text, re.IGNORECASE)
    if not ask_match:
        return None
    try:
        parsed = float(ask_match.group(1).replace(",", "."))
        return parsed if parsed > 0 else None
    except ValueError:
        return None


def _extract_ytm_date_from_html(html: str) -> str:
    script_match = SCRIPT_YTM_RE.search(html)
    if script_match:
        return _to_iso_date(script_match.group(1))

    text = _strip_html(html)
    match = YTM_NEARBY_RE.search(text)
    if not match:
        return ""

    candidate = next((group for group in match.groups() if group), "")
    return _to_iso_date(candidate)


def _extract_event_name_from_html(html: str) -> str:
    script_match = SCRIPT_EVENT_RE.search(html)
    if script_match:
        return script_match.group(1).strip().lower()

    text = _strip_html(html).lower()
    for marker in ("оферта", "put", "погашение"):
        if marker in text:
            return marker
    return ""


def _is_payload_empty(payload: DohodBondPayload) -> bool:
    """Совместимость: проверка пустого payload для старого fallback-кода."""
    return (
        payload.real_price is None
        and payload.ask_price is None
        and not payload.index_name
        and not payload.event_name
        and not payload.ytm_date
    )


def _as_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None


def _should_enrich_coupon(
    coupon_raw: str,
    index_name: str,
    base_rate: float = 0.0,
    index_spread: float = 0.0,
    approx_flag: bool = False,
) -> bool:
    if not index_name:
        return False
    normalized = coupon_raw.strip().lower()
    if normalized in {"", "-", "—", "нет", "n/a", "na", "none", "null", "nan"}:
        return True
    numeric = _as_float_or_none(coupon_raw)
    if numeric is None:
        return False
    if numeric <= 0:
        return True

    # Диагностика legacy-кеша: ранее могли сохранить только spread (без base_rate).
    if base_rate > 0 and abs(numeric - float(index_spread)) <= 1e-6:
        return True

    # Если поле было ранее помечено как приблизительное, разрешаем пересчет при наличии базы.
    if approx_flag and base_rate > 0:
        expected = base_rate + float(index_spread)
        return abs(numeric - expected) > 1e-6

    return False


def _should_enrich_offer(
    offer_date: str,
    ytm_date: str,
    mat_date: str,
    event_name: str,
    today: date | None = None,
) -> bool:
    if not ytm_date:
        return False
    if ytm_date == mat_date:
        return False
    if offer_date == ytm_date:
        return False
    try:
        ytm = datetime.strptime(ytm_date, "%Y-%m-%d").date()
    except ValueError:
        return False
    if ytm < (today or date.today()):
        return False
    event = event_name.strip().lower()
    return "погаш" not in event
