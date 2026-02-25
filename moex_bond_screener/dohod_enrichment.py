"""Обогащение облигаций данными с analytics.dohod.ru."""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from html import unescape
from typing import Any, Callable

import requests

from .config import AppConfig
from .raw_store import RawStore

DohodProgressCallback = Callable[[dict[str, Any]], None]
DohodCheckpointSaver = Callable[[dict[str, Any]], None]
DOHOD_CHECKPOINT_VERSION = 1

LABEL_VALUE_RE = r"{label}\s*</[^>]+>\s*<[^>]+[^>]*>(.*?)</"
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL)
NUMBER_RE = r"[+-]?\d+(?:[.,]\d+)?"
INDEX_RE = re.compile(r"(RUONIA|CBR_RATE|Z_CURVE_RUS)\s*([+-]\s*\d+(?:[.,]\d+)?)?", re.IGNORECASE)
TENOR_RE = re.compile(r"сроком\s+погашения\s+(\d+)\s+лет", re.IGNORECASE)


@dataclass(slots=True)
class DohodBondPayload:
    ask_price: float | None
    index_name: str
    index_spread: float
    index_tenor_years: int | None
    event_name: str
    ytm_date: str


@dataclass(slots=True)
class DohodEnrichmentStats:
    bonds_total: int = 0
    cache_hits: int = 0
    requested: int = 0
    realprice_added: int = 0
    realprice_updated: int = 0
    coupon_added: int = 0
    coupon_updated: int = 0
    offer_added: int = 0
    offer_updated: int = 0
    parse_empty_payloads: int = 0


class DohodEnricher:
    def __init__(self, config: AppConfig, logger: logging.Logger, raw_store: RawStore | None = None) -> None:
        self.config = config
        self.logger = logger
        self.raw_store = raw_store
        self.session = requests.Session()
        self._request_lock = threading.Lock()
        self._next_request_ts = 0.0
        self._thread_local = threading.local()
        self.last_stats = DohodEnrichmentStats()

    def enrich_bonds(
        self,
        bonds: list[dict[str, Any]],
        checkpoint_data: dict[str, Any] | None = None,
        checkpoint_saver: DohodCheckpointSaver | None = None,
        progress_callback: DohodProgressCallback | None = None,
    ) -> int:
        self.last_stats = DohodEnrichmentStats()
        checkpoint = self._normalize_checkpoint(checkpoint_data or {})
        previous_payload = checkpoint.get("bonds", {})
        processed: dict[str, dict[str, Any]] = dict(previous_payload) if isinstance(previous_payload, dict) else {}
        index_values = self._resolve_index_values(checkpoint)
        index_changed = self._has_index_changed(index_values, checkpoint.get("index_values", {}))
        fresh_cache = self._is_checkpoint_fresh(checkpoint)

        identifier_to_bonds: dict[str, list[dict[str, Any]]] = {}
        for bond in bonds:
            identifier = self._resolve_bond_identifier(bond)
            if not identifier:
                continue
            identifier_to_bonds.setdefault(identifier, []).append(bond)

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
        if progress_callback:
            progress_callback({"processed": processed_count, "total": len(identifier_to_bonds), "message": "Кэш ДОХОД применен"})

        workers = max(1, int(getattr(self.config, "dohod_workers", 8)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self._fetch_and_parse, identifier): identifier for identifier in pending}
            for future in as_completed(futures):
                identifier = futures[future]
                try:
                    payload, request_errors = future.result()
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("Ошибка обработки ДОХОД instrument=%s: %s", identifier, exc)
                    payload, request_errors = DohodBondPayload(None, "", 0.0, None, "", ""), 1

                if request_errors == 0 and _is_payload_empty(payload):
                    self.last_stats.parse_empty_payloads += 1
                    request_errors = 1
                    self.logger.warning("Пустой payload ДОХОД instrument=%s: карточка получена, но данные не извлечены", identifier)

                errors += request_errors
                if request_errors == 0:
                    serialized = {
                        "ask_price": payload.ask_price,
                        "index_name": payload.index_name,
                        "index_spread": payload.index_spread,
                        "index_tenor_years": payload.index_tenor_years,
                        "event_name": payload.event_name,
                        "ytm_date": payload.ytm_date,
                    }
                    processed[identifier] = serialized
                    self._apply_cached(identifier_to_bonds[identifier], serialized, index_values)

                processed_count += 1
                if checkpoint_saver:
                    checkpoint_saver(
                        {
                            "version": DOHOD_CHECKPOINT_VERSION,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                            "completed": False,
                            "index_values": index_values,
                            "bonds": processed,
                        }
                    )
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

        return errors

    def _fetch_with_fallback(self, primary_identifier: str, secondary_identifier: str | None) -> tuple[DohodBondPayload, int]:
        """Совместимость со старыми сборками: запрос только по primary (ISIN-only)."""
        return self._fetch_and_parse(primary_identifier)

    def _fetch_and_parse(self, secid: str) -> tuple[DohodBondPayload, int]:
        for attempt in range(1, self.config.retries + 1):
            try:
                response = self._get_with_rate_limit(
                    f"https://analytics.dohod.ru/bond/{secid}",
                    timeout=self.config.timeout_seconds,
                    delay_seconds=float(getattr(self.config, "dohod_request_delay_seconds", 0.05)),
                )
                response.raise_for_status()
                html = response.text
                if self.raw_store and self.config.raw_dump_enabled:
                    self.raw_store.dump_html(f"dohod_{secid}.html", html)
                return self.parse_bond_payload(html), 0
            except requests.RequestException as error:
                self.logger.warning("Ошибка запроса ДОХОД instrument=%s попытка=%s: %s", secid, attempt, error)
                if attempt == self.config.retries:
                    return DohodBondPayload(None, "", 0.0, None, "", ""), 1
                time.sleep(float(getattr(self.config, "dohod_request_delay_seconds", 0.05)) * attempt)

        return DohodBondPayload(None, "", 0.0, None, "", ""), 1

    @staticmethod
    def _resolve_bond_identifier(bond: dict[str, Any]) -> str:
        return str(bond.get("ISIN") or "").strip()

    @staticmethod
    def _resolve_secondary_identifier(bond: dict[str, Any], primary_identifier: str) -> str:
        """Совместимость со старыми сборками: fallback отключен, всегда пусто."""
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

        event_name = extracted.get("Событие в ближ. дату", "").strip().lower()
        ytm_date = _to_iso_date(extracted.get("Дата, к которой рассчит. YTM", ""))

        return DohodBondPayload(ask_price, index_name, spread, tenor_years, event_name, ytm_date)

    def _resolve_index_values(self, checkpoint: dict[str, Any]) -> dict[str, float]:
        configured = getattr(self.config, "dohod_index_values", None)
        if isinstance(configured, dict):
            normalized: dict[str, float] = {}
            for key, value in configured.items():
                try:
                    normalized[str(key).upper()] = float(value)
                except (TypeError, ValueError):
                    continue
            normalized.setdefault("RUONIA", 0.0)
            normalized.setdefault("CBR_RATE", 0.0)
            normalized.setdefault("Z_CURVE_RUS", 0.0)
            return normalized
        previous = checkpoint.get("index_values", {})
        return {
            "RUONIA": float((previous or {}).get("RUONIA") or 0.0),
            "CBR_RATE": float((previous or {}).get("CBR_RATE") or 0.0),
            "Z_CURVE_RUS": float((previous or {}).get("Z_CURVE_RUS") or 0.0),
        }

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
        ask_price = payload.get("ask_price")
        index_name = str(payload.get("index_name") or "").strip()
        ytm_date = str(payload.get("ytm_date") or "").strip()
        event_name = str(payload.get("event_name") or "").strip()
        if isinstance(ask_price, (int, float)):
            return True
        return bool(index_name or ytm_date or event_name)

    def _apply_cached(self, bonds: list[dict[str, Any]], payload: dict[str, Any], index_values: dict[str, float]) -> None:
        ask_price = payload.get("ask_price")
        index_name = str(payload.get("index_name") or "")
        index_spread = float(payload.get("index_spread") or 0.0)
        index_tenor_years = payload.get("index_tenor_years")
        event_name = str(payload.get("event_name") or "")
        ytm_date = str(payload.get("ytm_date") or "")

        for bond in bonds:
            if isinstance(ask_price, (int, float)):
                new_real_price = float(ask_price)
                old_real_price = bond.get("RealPrice")
                if old_real_price in (None, ""):
                    self.last_stats.realprice_added += 1
                elif old_real_price != new_real_price:
                    self.last_stats.realprice_updated += 1
                bond["RealPrice"] = new_real_price

            coupon_raw = str(bond.get("COUPONPERCENT") or "").strip()
            if _should_enrich_coupon(coupon_raw, index_name):
                base_rate = float(index_values.get(index_name, 0.0))
                if index_name == "Z_CURVE_RUS" and index_tenor_years:
                    base_rate = float(index_values.get(f"Z_CURVE_RUS_{index_tenor_years}Y", base_rate))
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

            offer_date = str(bond.get("OFFERDATE") or "")
            mat_date = str(bond.get("MATDATE") or "")
            if _should_enrich_offer(offer_date, ytm_date, mat_date, event_name):
                if ytm_date != mat_date:
                    if offer_date:
                        self.last_stats.offer_updated += 1
                    else:
                        self.last_stats.offer_added += 1
                    bond["OFFERDATE"] = ytm_date

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
        return float(candidate.replace(",", "."))
    except ValueError:
        return None


def _parse_index_and_spread(raw: str) -> tuple[str, float]:
    if not raw:
        return "", 0.0
    match = INDEX_RE.search(raw)
    if not match:
        return "", 0.0
    name = str(match.group(1) or "").upper()
    spread_raw = str(match.group(2) or "0")
    spread = float(spread_raw.replace(" ", "").replace(",", ".")) if spread_raw else 0.0
    return name, spread


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
    try:
        return datetime.strptime(value, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""



def _is_payload_empty(payload: DohodBondPayload) -> bool:
    """Совместимость: проверка пустого payload для старого fallback-кода."""
    return (
        payload.ask_price is None
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


def _should_enrich_coupon(coupon_raw: str, index_name: str) -> bool:
    if not index_name:
        return False
    normalized = coupon_raw.strip().lower()
    if normalized in {"", "-", "—", "нет", "n/a", "na", "none", "null", "nan"}:
        return True
    numeric = _as_float_or_none(coupon_raw)
    return numeric is not None and numeric <= 0


def _should_enrich_offer(offer_date: str, ytm_date: str, mat_date: str, event_name: str) -> bool:
    if not ytm_date:
        return False
    if ytm_date == mat_date:
        return False
    if offer_date == ytm_date:
        return False
    event = event_name.strip().lower()
    return "погаш" not in event
