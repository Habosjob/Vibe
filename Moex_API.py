# Moex_API.py
from __future__ import annotations

import argparse
import hashlib
import json
import random
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from logs import setup_logger, ensure_logs_dir, Timer
from SQL import SQLiteCache, RequestLogRow

BASE = "https://iss.moex.com/iss"


def today_str() -> str:
    return date.today().isoformat()


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ensure_single_secid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Гарантирует, что в df есть ровно ОДНА колонка 'SECID' (1-dimensional),
    иначе groupby('SECID') может падать: "Grouper not 1-dimensional".
    """
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    secid_idx = [i for i, c in enumerate(cols) if c == "SECID"]
    if len(secid_idx) <= 1:
        return df

    sec_cols = df.loc[:, ["SECID"]]  # вернёт DataFrame, если SECID дублируется
    secid_series = None
    for j in range(sec_cols.shape[1]):
        s = sec_cols.iloc[:, j]
        if secid_series is None:
            secid_series = s
        else:
            secid_series = secid_series.fillna(s)

    out = df.copy()
    out = out.loc[:, [c for c in out.columns if c != "SECID"]]
    out.insert(0, "SECID", secid_series)
    return out


@dataclass
class HttpResult:
    status: Optional[int]
    elapsed_ms: Optional[int]
    size_bytes: Optional[int]
    text: Optional[str]
    url: str
    params: dict
    error: Optional[str] = None
    headers: Optional[dict] = None


class RateLimiter:
    """
    Глобальный rate limiter (все потоки делят один лимит).
    Простой вариант: выдерживаем минимальный интервал между запросами.
    """

    def __init__(self, requests_per_sec: float):
        self.rps = float(requests_per_sec)
        self._lock = threading.Lock()
        self._next_ts = 0.0

    def acquire(self) -> None:
        if self.rps <= 0:
            return
        min_interval = 1.0 / self.rps
        with self._lock:
            now = time.perf_counter()
            if now < self._next_ts:
                time.sleep(self._next_ts - now)
            self._next_ts = max(self._next_ts, time.perf_counter()) + min_interval


class IssClient:
    """
    Клиент ISS с retries/backoff на 429/5xx.
    Пишет requests_log в SQLite на КАЖДУЮ попытку.
    """

    def __init__(
        self,
        cache: SQLiteCache,
        logger,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_base: float = 0.8,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.cache = cache
        self.logger = logger
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.rate_limiter = rate_limiter

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Vibe-MOEX-ISS/1.3",
                "Accept": "application/json,text/plain,*/*",
            }
        )

    def _sleep_for_retry(self, attempt: int, resp: Optional[requests.Response]) -> None:
        if resp is not None:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    sec = float(ra)
                    time.sleep(min(60.0, max(0.0, sec)))
                    return
                except Exception:
                    pass

        base = self.backoff_base * (2 ** (attempt - 1))
        jitter = random.random() * 0.25 * base
        time.sleep(min(30.0, base + jitter))

    def get(self, path: str, params: Optional[dict] = None) -> HttpResult:
        params = params or {}
        url = f"{BASE}{path}"
        retry_statuses = {429, 500, 502, 503, 504}
        last_err: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            t0 = time.perf_counter()
            resp: Optional[requests.Response] = None
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.acquire()

                resp = self.session.get(url, params=params, timeout=self.timeout)
                status = int(resp.status_code)
                headers = dict(resp.headers)
                text = resp.text if resp.text is not None else ""
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                size_bytes = len((text or "").encode("utf-8", errors="ignore"))

                self.cache.log_request(
                    RequestLogRow(
                        url=str(resp.url),
                        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
                        status=status,
                        elapsed_ms=elapsed_ms,
                        size_bytes=size_bytes,
                        created_utc=_utc_iso(),
                        error=None,
                    )
                )

                if status in retry_statuses:
                    self.logger.warning(
                        f"HTTP {status} retryable | attempt {attempt}/{self.max_retries} | {resp.url}"
                    )
                    if attempt < self.max_retries:
                        self._sleep_for_retry(attempt, resp)
                        continue

                return HttpResult(
                    status=status,
                    elapsed_ms=elapsed_ms,
                    size_bytes=size_bytes,
                    text=text,
                    url=str(resp.url),
                    params=params,
                    error=None,
                    headers=headers,
                )

            except Exception as e:
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                last_err = repr(e)

                self.cache.log_request(
                    RequestLogRow(
                        url=url,
                        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
                        status=None,
                        elapsed_ms=elapsed_ms,
                        size_bytes=0,
                        created_utc=_utc_iso(),
                        error=last_err,
                    )
                )

                self.logger.warning(
                    f"HTTP exception retryable | attempt {attempt}/{self.max_retries} | {url} | {last_err}"
                )
                if attempt < self.max_retries:
                    self._sleep_for_retry(attempt, resp)
                    continue

                return HttpResult(
                    status=None,
                    elapsed_ms=elapsed_ms,
                    size_bytes=0,
                    text=None,
                    url=url,
                    params=params,
                    error=last_err,
                    headers=None,
                )

        return HttpResult(
            status=None,
            elapsed_ms=None,
            size_bytes=None,
            text=None,
            url=url,
            params=params,
            error=last_err,
            headers=None,
        )


def parse_iss_json_tables_safe(
    payload_text: str, *, logger=None, url: str = "", content_type: str = "", snippet_chars: int = 800
) -> Dict[str, pd.DataFrame]:
    """
    Безопасный парсинг ISS JSON.
    Если JSON битый/не JSON — логируем кусок ответа и Content-Type.
    """
    try:
        obj = json.loads(payload_text)
    except Exception as e:
        if logger:
            snip = (payload_text or "")[: max(0, int(snippet_chars))]
            logger.warning(
                f"ISS parse failed | err={e} | content_type={content_type!r} | url={url}\n"
                f"ISS response snippet:\n{snip}"
            )
        return {}

    out: Dict[str, pd.DataFrame] = {}
    if not isinstance(obj, dict):
        return out

    for block, content in obj.items():
        if not isinstance(content, dict):
            continue
        cols = content.get("columns")
        data = content.get("data")
        if isinstance(cols, list) and isinstance(data, list):
            out[block] = pd.DataFrame(data, columns=cols)
    return out


def _hash_page_secids(secids: List[str]) -> str:
    h = hashlib.sha256()
    for s in secids:
        h.update(s.encode("utf-8", errors="ignore"))
        h.update(b"\n")
    return h.hexdigest()


def fetch_all_traded_bonds(
    client: IssClient,
    logger,
    limit: int = 200,
    max_pages: int = 2000,
    min_new_ratio_stop: float = 0.02,
    boardgroup: int = 58,
) -> List[dict]:
    columns = [
        "SECID",
        "ISIN",
        "REGNUMBER",
        "SHORTNAME",
        "NAME",
        "EMITTER_ID",
        "TYPE",
        "GROUP",
        "PRIMARY_BOARDID",
        "LISTLEVEL",
        "ISSUEDATE",
        "MATDATE",
        "FACEVALUE",
        "FACEUNIT",
        "COUPONPERCENT",
        "COUPONVALUE",
        "COUPONPERIOD",
    ]

    all_rows: List[dict] = []
    seen_secids: set[str] = set()
    seen_page_hashes: set[str] = set()
    start = 0
    page = 0
    low_new_streak = 0
    path = f"/engines/stock/markets/bonds/boardgroups/{boardgroup}/securities.json"

    with Timer(logger, f"fetch_all_traded_bonds(boardgroup={boardgroup})"):
        while True:
            page += 1
            if page > max_pages:
                logger.warning(f"STOP max_pages reached | page={page} max_pages={max_pages}")
                break

            params = {
                "iss.meta": "off",
                "lang": "ru",
                "is_trading": 1,
                "start": start,
                "limit": limit,
                "securities.columns": ",".join(columns),
            }

            res = client.get(path, params=params)
            if res.status != 200 or not res.text:
                logger.error(f"Failed bonds list | status={res.status} | url={res.url} | err={res.error}")
                break

            ct = (res.headers or {}).get("Content-Type", "")
            tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct)

            sec = tables.get("securities")
            if sec is None or sec.empty:
                logger.info(f"Pagination end (empty) | page={page} start={start}")
                break

            rows = sec.to_dict(orient="records")

            page_secids = [str(r.get("SECID")) for r in rows if r.get("SECID") is not None]
            page_secids = [s for s in page_secids if s and s.lower() != "nan"]
            page_hash = _hash_page_secids(page_secids)
            if page_hash in seen_page_hashes:
                logger.warning(f"STOP repeated page hash | page={page} start={start} rows={len(rows)}")
                break
            seen_page_hashes.add(page_hash)

            new = 0
            for r in rows:
                s = r.get("SECID")
                if s is None:
                    continue
                s = str(s)
                if not s or s.lower() == "nan":
                    continue
                if s not in seen_secids:
                    seen_secids.add(s)
                    all_rows.append(r)
                    new += 1

            total = len(all_rows)
            new_ratio = new / max(1, len(rows))
            logger.info(
                f"Page {page} | start={start} | rows={len(rows)} | new={new} | "
                f"new_ratio={new_ratio:.3f} | total_unique={total}"
            )

            if new_ratio < min_new_ratio_stop:
                low_new_streak += 1
            else:
                low_new_streak = 0
            if low_new_streak >= 3:
                logger.warning(
                    f"STOP low new_ratio streak | streak={low_new_streak} | "
                    f"last_new_ratio={new_ratio:.3f} | total_unique={total}"
                )
                break

            start += len(rows)

    logger.info(f"FETCH DONE | unique_secids={len(seen_secids)} | rows={len(all_rows)}")
    return all_rows


def get_bonds_list_daily(cache: SQLiteCache, client: IssClient, logger, force_refresh: bool) -> List[dict]:
    d = today_str()
    if not force_refresh:
        cached = cache.get_bonds_list(d)
        if cached is not None:
            logger.info(f"CACHE HIT | bonds_list | date={d} | rows={len(cached)}")
            return cached

    logger.info(f"CACHE MISS | bonds_list | date={d} | force_refresh={force_refresh}")
    bonds = fetch_all_traded_bonds(client, logger)
    cache.set_bonds_list(bonds, d)
    logger.info(f"CACHE SAVE | bonds_list | date={d} | rows={len(bonds)}")
    return bonds


def fetch_bondization_ttl(
    cache: SQLiteCache, client: IssClient, logger, secid: str, force_refresh: bool
) -> Dict[str, pd.DataFrame]:
    d = today_str()
    if not force_refresh:
        existing = cache.get_bond_raw(secid, "bondization", d)
        if existing and int(existing.get("status") or 0) == 200 and existing.get("response_text"):
            logger.info(
                f"TTL HIT | bondization | {secid} | date={d} | bytes={existing.get('size_bytes')}"
            )
            ct = ""
            return parse_iss_json_tables_safe(existing["response_text"], logger=logger, url="", content_type=ct)

    params = {
        "iss.meta": "off",
        "lang": "ru",
        "limit": "unlimited",
        "iss.only": "coupons,offers,amortizations,events",
    }

    res = client.get(f"/securities/{secid}/bondization.json", params=params)
    cache.set_bond_raw(
        secid=secid,
        kind="bondization",
        asof_date=d,
        url=res.url,
        params=res.params,
        status=res.status,
        elapsed_ms=res.elapsed_ms,
        size_bytes=res.size_bytes,
        response_text=res.text,
    )

    if res.status != 200 or not res.text:
        logger.warning(f"bondization failed | {secid} | status={res.status} | err={res.error}")
        return {}

    ct = (res.headers or {}).get("Content-Type", "")
    tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct)
    if not tables:
        logger.warning(f"bondization parsed empty | {secid} | url={res.url} | content_type={ct!r}")
    return tables


def fetch_description_ttl(
    cache: SQLiteCache, client: IssClient, logger, secid: str, force_refresh: bool
) -> pd.DataFrame:
    d = today_str()
    if not force_refresh:
        existing = cache.get_bond_raw(secid, "description", d)
        if existing and int(existing.get("status") or 0) == 200 and existing.get("response_text"):
            logger.info(f"TTL HIT | description | {secid} | date={d}")
            tables = parse_iss_json_tables_safe(existing["response_text"], logger=logger, url="", content_type="")
            return tables.get("description", pd.DataFrame())

    params = {"iss.meta": "off", "lang": "ru"}
    res = client.get(f"/securities/{secid}.json", params=params)
    cache.set_bond_raw(
        secid=secid,
        kind="description",
        asof_date=d,
        url=res.url,
        params=res.params,
        status=res.status,
        elapsed_ms=res.elapsed_ms,
        size_bytes=res.size_bytes,
        response_text=res.text,
    )

    if res.status != 200 or not res.text:
        logger.warning(f"description failed | {secid} | status={res.status} | err={res.error}")
        return pd.DataFrame()

    ct = (res.headers or {}).get("Content-Type", "")
    tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct)
    df = tables.get("description", pd.DataFrame())
    if df is None or df.empty:
        logger.warning(f"description parsed empty | {secid} | url={res.url} | content_type={ct!r}")
    return df


def _pick_first(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in d and d[k] not in (None, "", "nan", "None"):
            v = d[k]
            s = str(v).strip()
            if s and s.lower() not in ("nan", "none"):
                return s
    return None


def _description_to_kv(description_df: pd.DataFrame) -> Dict[str, str]:
    """
    Превращает таблицу description (обычно NAME/VALUE) в dict.
    """
    if description_df is None or description_df.empty:
        return {}
    df = description_df.copy()
    df.columns = [str(c).upper() for c in df.columns]
    name_col = "NAME" if "NAME" in df.columns else ("TITLE" if "TITLE" in df.columns else None)
    if name_col is None or "VALUE" not in df.columns:
        return {}
    out: Dict[str, str] = {}
    for _, r in df.iterrows():
        k = str(r.get(name_col) or "").strip()
        v = r.get("VALUE")
        if not k:
            continue
        if v is None:
            continue
        vs = str(v).strip()
        if not vs or vs.lower() in ("nan", "none"):
            continue
        out[k.upper()] = vs
    return out


def try_fetch_emitent(
    cache: SQLiteCache,
    client: IssClient,
    logger,
    emitter_id: int,
    *,
    secid_hint: Optional[str] = None,
    force_refresh: bool,
) -> Optional[dict]:
    """
    1) Если уже есть в emitents — используем.
    2) Пытаемся /emitents/{id}.json
    3) Если пусто — пытаемся дособрать из /securities/{secid}.json description (если есть secid_hint)
    """
    if not emitter_id:
        return None

    if not force_refresh:
        existing = cache.get_emitent(emitter_id)
        if existing and (existing.get("inn") or existing.get("title")):
            return existing

    d = today_str()
    fake = f"EMITENT:{emitter_id}"

    def upsert_from_row(row: Dict[str, Any], raw_json: Optional[str]) -> None:
        inn = _pick_first(row, ["INN", "inn"])
        title = _pick_first(row, ["TITLE", "title", "NAME", "name"])
        short_title = _pick_first(row, ["SHORT_TITLE", "short_title", "SHORTNAME", "shortname"])
        ogrn = _pick_first(row, ["OGRN", "ogrn"])
        okpo = _pick_first(row, ["OKPO", "okpo"])
        kpp = _pick_first(row, ["KPP", "kpp"])
        okved = _pick_first(row, ["OKVED", "okved"])
        address = _pick_first(row, ["ADDRESS", "address", "LEGAL_ADDRESS", "legal_address"])
        phone = _pick_first(row, ["PHONE", "phone", "TEL", "tel"])
        site = _pick_first(row, ["SITE", "site", "WWW", "www", "URL", "url"])
        email = _pick_first(row, ["EMAIL", "email", "E_MAIL", "e_mail"])

        cache.upsert_emitent(
            emitter_id=emitter_id,
            inn=inn,
            title=title,
            short_title=short_title,
            ogrn=ogrn,
            okpo=okpo,
            kpp=kpp,
            okved=okved,
            address=address,
            phone=phone,
            site=site,
            email=email,
            raw_json=raw_json,
        )

    # 2) Сначала пробуем взять из bond_raw (сегодня) чтобы не дергать сеть повторно в рамках дня
    if not force_refresh:
        raw_exist = cache.get_bond_raw(fake, "emitent", d)
        if raw_exist and int(raw_exist.get("status") or 0) == 200 and raw_exist.get("response_text"):
            try:
                ct = ""
                tables = parse_iss_json_tables_safe(
                    raw_exist["response_text"], logger=logger, url="", content_type=ct
                )
                # обычно там есть блок emitents или что-то похожее
                for _, df in tables.items():
                    if df is None or df.empty:
                        continue
                    row = df.iloc[0].to_dict()
                    upsert_from_row(row, raw_exist["response_text"])
                    return cache.get_emitent(emitter_id)
            except Exception:
                pass

    # 2) /emitents/{id}.json
    params = {"iss.meta": "off", "lang": "ru"}
    res = client.get(f"/emitents/{emitter_id}.json", params=params)
    cache.set_bond_raw(
        secid=fake,
        kind="emitent",
        asof_date=d,
        url=res.url,
        params=res.params,
        status=res.status,
        elapsed_ms=res.elapsed_ms,
        size_bytes=res.size_bytes,
        response_text=res.text,
    )

    if res.status == 200 and res.text:
        ct = (res.headers or {}).get("Content-Type", "")
        tables = parse_iss_json_tables_safe(res.text, logger=logger, url=res.url, content_type=ct)
        parsed_any = False
        for _, df in tables.items():
            if df is None or df.empty:
                continue
            row = df.iloc[0].to_dict()
            upsert_from_row(row, res.text)
            parsed_any = True
            break
        if parsed_any:
            got = cache.get_emitent(emitter_id)
            if got and (got.get("inn") or got.get("title")):
                return got

    logger.warning(
        f"emitent endpoint incomplete/failed | emitter_id={emitter_id} | status={res.status} | err={res.error}"
    )

    # 3) fallback: вытянуть реквизиты из /securities/{secid}.json description
    if secid_hint:
        try:
            desc = fetch_description_ttl(cache, client, logger, secid_hint, force_refresh=force_refresh)
            kv = _description_to_kv(desc)
            # распространённые варианты ключей (биржа может использовать разные названия)
            row2: Dict[str, Any] = {
                "INN": _pick_first(kv, ["ИНН", "INN", "EMITENT_INN", "EMITTER_INN"]),
                "TITLE": _pick_first(kv, ["ЭМИТЕНТ", "EMITENT", "EMITTER", "FULLNAME", "FULL_NAME", "NAME"]),
                "SHORT_TITLE": _pick_first(kv, ["КРАТКОЕ НАИМЕНОВАНИЕ", "SHORTNAME", "SHORT_NAME"]),
                "OGRN": _pick_first(kv, ["ОГРН", "OGRN"]),
                "OKPO": _pick_first(kv, ["ОКПО", "OKPO"]),
                "KPP": _pick_first(kv, ["КПП", "KPP"]),
                "OKVED": _pick_first(kv, ["ОКВЭД", "OKVED"]),
                "ADDRESS": _pick_first(kv, ["АДРЕС", "ADDRESS", "LEGAL_ADDRESS"]),
                "PHONE": _pick_first(kv, ["ТЕЛЕФОН", "PHONE"]),
                "SITE": _pick_first(kv, ["САЙТ", "SITE", "WWW", "URL"]),
                "EMAIL": _pick_first(kv, ["EMAIL", "E-MAIL", "ПОЧТА"]),
            }
            upsert_from_row(row2, raw_json=None)
            got = cache.get_emitent(emitter_id)
            if got and (got.get("inn") or got.get("title")):
                logger.info(f"emitent fallback success from description | emitter_id={emitter_id} | secid={secid_hint}")
                return got
        except Exception as e:
            logger.warning(f"emitent fallback failed | emitter_id={emitter_id} | secid={secid_hint} | err={e}")

    return cache.get_emitent(emitter_id)


def build_pivot_description(description_df: pd.DataFrame, emitents_df: pd.DataFrame) -> pd.DataFrame:
    if description_df.empty:
        base = pd.DataFrame(columns=["SECID"])
    else:
        df = description_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        df = _ensure_single_secid(df)
        if "SECID" not in df.columns:
            df["SECID"] = None
        key_col = "NAME" if "NAME" in df.columns else ("TITLE" if "TITLE" in df.columns else None)
        if key_col is None or "VALUE" not in df.columns:
            base = pd.DataFrame({"SECID": sorted(df["SECID"].dropna().astype(str).unique().tolist())})
        else:
            wide = df.pivot_table(index="SECID", columns=key_col, values="VALUE", aggfunc="first")
            if wide.index.name == "SECID" and "SECID" in wide.columns:
                wide = wide.drop(columns=["SECID"])
            wide = wide.reset_index()
            wide.columns = [str(c) for c in wide.columns]
            base = wide

    if emitents_df is not None and not emitents_df.empty:
        e = emitents_df.copy()
        e.columns = [str(c).upper() for c in e.columns]
        e = _ensure_single_secid(e)
        if "SECID" in e.columns:
            keep = [
                c
                for c in [
                    "SECID",
                    "EMITTER_ID",
                    "INN",
                    "TITLE",
                    "SHORT_TITLE",
                    "OGRN",
                    "OKPO",
                    "KPP",
                    "OKVED",
                    "ADDRESS",
                    "PHONE",
                    "SITE",
                    "EMAIL",
                ]
                if c in e.columns
            ]
            if keep:
                base = base.merge(e[keep].drop_duplicates(), on="SECID", how="left")
    return base


def _parse_date_safe(x: Any) -> Optional[pd.Timestamp]:
    """
    ВАЖНО: возвращаем tz-aware UTC Timestamp, чтобы сравнения работали
    (иначе падает tz-naive vs tz-aware).
    """
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def build_summary(
    sample_bonds: pd.DataFrame, emitents_df: pd.DataFrame, offers_df: pd.DataFrame, coupons_df: pd.DataFrame
) -> pd.DataFrame:
    out = sample_bonds.copy()
    out.columns = [str(c).upper() for c in out.columns]
    out = _ensure_single_secid(out)

    if emitents_df is not None and not emitents_df.empty:
        e = emitents_df.copy()
        e.columns = [str(c).upper() for c in e.columns]
        e = _ensure_single_secid(e)
        keep = [
            c
            for c in [
                "SECID",
                "EMITTER_ID",
                "INN",
                "TITLE",
                "SHORT_TITLE",
                "OGRN",
                "OKPO",
                "KPP",
                "OKVED",
            ]
            if c in e.columns
        ]
        if keep:
            out = out.merge(e[keep].drop_duplicates(), on="SECID", how="left")

    next_offer: Dict[str, str] = {}
    if offers_df is not None and not offers_df.empty:
        df = offers_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        df = _ensure_single_secid(df)
        date_col = "OFFERDATE" if "OFFERDATE" in df.columns else ("DATE" if "DATE" in df.columns else None)
        if "SECID" in df.columns and date_col:
            df["_DT"] = df[date_col].apply(_parse_date_safe)
            now = pd.Timestamp.now(tz="UTC")
            df = df[df["_DT"].notna()]
            for secid, g in df.groupby("SECID"):
                future = g[g["_DT"] >= now].sort_values("_DT")
                pick = future.iloc[0] if len(future) else g.sort_values("_DT").iloc[-1]
                next_offer[str(secid)] = pick["_DT"].date().isoformat()

    next_coupon: Dict[str, str] = {}
    if coupons_df is not None and not coupons_df.empty:
        df = coupons_df.copy()
        df.columns = [str(c).upper() for c in df.columns]
        df = _ensure_single_secid(df)
        date_col = "COUPONDATE" if "COUPONDATE" in df.columns else ("DATE" if "DATE" in df.columns else None)
        if "SECID" in df.columns and date_col:
            df["_DT"] = df[date_col].apply(_parse_date_safe)
            now = pd.Timestamp.now(tz="UTC")
            df = df[df["_DT"].notna()]
            for secid, g in df.groupby("SECID"):
                future = g[g["_DT"] >= now].sort_values("_DT")
                if len(future):
                    next_coupon[str(secid)] = future.iloc[0]["_DT"].date().isoformat()

    if "SECID" in out.columns:
        out["NEXT_OFFER_DATE"] = out["SECID"].astype(str).map(next_offer)
        out["NEXT_COUPON_DATE"] = out["SECID"].astype(str).map(next_coupon)

    preferred = [
        "SECID",
        "ISIN",
        "REGNUMBER",
        "SHORTNAME",
        "NAME",
        "EMITTER_ID",
        "INN",
        "TITLE",
        "ISSUEDATE",
        "MATDATE",
        "FACEVALUE",
        "FACEUNIT",
        "COUPONPERCENT",
        "COUPONVALUE",
        "COUPONPERIOD",
        "NEXT_OFFER_DATE",
        "NEXT_COUPON_DATE",
        "LISTLEVEL",
        "PRIMARY_BOARDID",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols]


def save_excel_bonds_list(bonds: List[dict], out_path: str | Path, logger) -> None:
    df = pd.DataFrame(bonds)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as w:
        df.to_excel(w, index=False, sheet_name="bonds")
        meta = pd.DataFrame([{"created_utc": _utc_iso(), "rows": len(df)}])
        meta.to_excel(w, index=False, sheet_name="meta")
    logger.info(f"Excel saved: {out_path} | rows={len(df)}")


def save_excel_detail(
    bonds_sample: pd.DataFrame,
    description_df: pd.DataFrame,
    events_df: pd.DataFrame,
    coupons_df: pd.DataFrame,
    offers_df: pd.DataFrame,
    amort_df: pd.DataFrame,
    emitents_df: pd.DataFrame,
    out_path: str | Path,
    logger,
) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pivot_df = build_pivot_description(description_df, emitents_df)
    summary_df = build_summary(bonds_sample, emitents_df, offers_df, coupons_df)

    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as w:
        meta = pd.DataFrame(
            [
                {
                    "created_utc": _utc_iso(),
                    "sample_rows": len(bonds_sample),
                    "desc_rows": len(description_df),
                    "events_rows": len(events_df),
                    "coupons_rows": len(coupons_df),
                    "offers_rows": len(offers_df),
                    "amort_rows": len(amort_df),
                    "emitents_rows": len(emitents_df),
                    "pivot_rows": len(pivot_df),
                }
            ]
        )
        meta.to_excel(w, index=False, sheet_name="meta")
        summary_df.to_excel(w, index=False, sheet_name="summary")
        bonds_sample.to_excel(w, index=False, sheet_name="sample_bonds")
        emitents_df.to_excel(w, index=False, sheet_name="emitents")
        pivot_df.to_excel(w, index=False, sheet_name="pivot_description")
        description_df.to_excel(w, index=False, sheet_name="description")
        events_df.to_excel(w, index=False, sheet_name="events")
        coupons_df.to_excel(w, index=False, sheet_name="coupons")
        offers_df.to_excel(w, index=False, sheet_name="offers")
        amort_df.to_excel(w, index=False, sheet_name="amortizations")

    logger.info(
        f"Detail Excel saved: {out_path} | summary={len(summary_df)} | pivot={len(pivot_df)} | "
        f"desc={len(description_df)} | events={len(events_df)} | coupons={len(coupons_df)} | "
        f"offers={len(offers_df)} | amort={len(amort_df)} | emitents={len(emitents_df)}"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MOEX ISS bonds + detail + SQLite cache")
    p.add_argument("--sample-size", type=int, default=10, help="How many random bonds to sample for detail")
    p.add_argument("--seed", type=int, default=42, help="Random seed for sampling")
    p.add_argument("--force-refresh-bonds", action="store_true", help="Ignore daily cache for bonds_list and refetch")
    p.add_argument("--force-refresh-detail", action="store_true", help="Ignore TTL for description/bondization/emitents")

    p.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    p.add_argument("--retries", type=int, default=5, help="HTTP retries for 429/5xx")
    p.add_argument("--backoff", type=float, default=0.8, help="Backoff base seconds")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--boardgroup", type=int, default=58, help="MOEX bonds boardgroup id for list endpoint")

    # purge TTL
    p.add_argument("--purge-bond-raw-days", type=int, default=30, help="Delete bond_raw older than N days (0=disable)")
    p.add_argument("--purge-requests-days", type=int, default=30, help="Delete requests_log older than N days (0=disable)")
    p.add_argument("--purge-bonds-list-days", type=int, default=60, help="Delete bonds_list older than N days (0=disable)")
    p.add_argument("--purge-emitents-days", type=int, default=0, help="Delete emitents older than N days (0=disable)")

    # parallel detail
    p.add_argument("--detail-workers", type=int, default=8, help="ThreadPool workers for detail fetch")
    p.add_argument("--detail-rps", type=float, default=8.0, help="Global requests/sec rate limit for detail fetch")

    # parse debug
    p.add_argument("--parse-snippet-chars", type=int, default=800, help="How many chars to log on parse failures")

    return p.parse_args()


def _process_one_secid(
    secid: str,
    *,
    cache: SQLiteCache,
    client: IssClient,
    logger,
    sample_df: pd.DataFrame,
    force_refresh: bool,
) -> Tuple[str, List[pd.DataFrame], List[pd.DataFrame], List[pd.DataFrame], List[pd.DataFrame], List[pd.DataFrame], Dict[str, Any]]:
    """
    Возвращает (secid, [desc], [events], [coupons], [offers], [amortizations], emitent_row_dict)
    """
    desc_rows: List[pd.DataFrame] = []
    ev_rows: List[pd.DataFrame] = []
    cp_rows: List[pd.DataFrame] = []
    of_rows: List[pd.DataFrame] = []
    am_rows: List[pd.DataFrame] = []

    desc = fetch_description_ttl(cache, client, logger, secid, force_refresh=force_refresh)
    if not desc.empty:
        ddf = desc.copy()
        ddf["SECID"] = secid
        desc_rows.append(ddf)

    bz = fetch_bondization_ttl(cache, client, logger, secid, force_refresh=force_refresh)
    for block, sink in [
        ("events", ev_rows),
        ("coupons", cp_rows),
        ("offers", of_rows),
        ("amortizations", am_rows),
    ]:
        df = bz.get(block)
        if df is not None and not df.empty:
            x = df.copy()
            x["SECID"] = secid
            sink.append(x)

    emitter_id_int: Optional[int] = None
    try:
        r = sample_df[sample_df["SECID"].astype(str) == str(secid)].iloc[0].to_dict()
        emitter_id = r.get("EMITTER_ID")
        if emitter_id is not None and str(emitter_id).strip() != "":
            emitter_id_int = int(emitter_id)
    except Exception:
        emitter_id_int = None

    emitent_row: Dict[str, Any] = {"SECID": secid, "EMITTER_ID": emitter_id_int}
    if emitter_id_int:
        e = try_fetch_emitent(
            cache,
            client,
            logger,
            emitter_id_int,
            secid_hint=secid,
            force_refresh=force_refresh,
        )
        if e:
            emitent_row.update(
                {
                    "INN": e.get("inn"),
                    "TITLE": e.get("title"),
                    "SHORT_TITLE": e.get("short_title"),
                    "OGRN": e.get("ogrn"),
                    "OKPO": e.get("okpo"),
                    "KPP": e.get("kpp"),
                    "OKVED": e.get("okved"),
                    "ADDRESS": e.get("address"),
                    "PHONE": e.get("phone"),
                    "SITE": e.get("site"),
                    "EMAIL": e.get("email"),
                    "UPDATED_UTC": e.get("updated_utc"),
                }
            )

    return secid, desc_rows, ev_rows, cp_rows, of_rows, am_rows, emitent_row


def main():
    args = parse_args()

    lp = ensure_logs_dir("logs")
    import logging

    logger = setup_logger(
        "Moex_API",
        lp.logfile,
        level=getattr(logging, args.log_level),
        clear=True,
        also_console=True,
    )
    cache_logger = setup_logger(
        "SQLiteCache",
        lp.logfile,
        level=getattr(logging, args.log_level),
        clear=False,
        also_console=False,
    )

    start_utc = _utc_iso()
    logger.info(f"START | utc={start_utc} | log={lp.logfile.resolve()}")

    cache = SQLiteCache("moex_cache.sqlite", logger=cache_logger)

    # purge TTL
    try:
        n1 = cache.purge_bond_raw(args.purge_bond_raw_days)
        n2 = cache.purge_requests_log(args.purge_requests_days)
        n3 = cache.purge_bonds_list(args.purge_bonds_list_days)
        n4 = cache.purge_emitents(args.purge_emitents_days)
        if any(x > 0 for x in (n1, n2, n3, n4)):
            logger.info(
                f"PURGE done | bond_raw={n1} | requests_log={n2} | bonds_list={n3} | emitents={n4}"
            )
    except Exception as e:
        logger.warning(f"PURGE failed | err={e}")

    # общий rate-limit для всех потоков
    rate = RateLimiter(args.detail_rps) if args.detail_rps and args.detail_rps > 0 else None

    client = IssClient(
        cache,
        logger,
        timeout=args.timeout,
        max_retries=args.retries,
        backoff_base=args.backoff,
        rate_limiter=rate,
    )

    t0 = time.perf_counter()
    try:
        with Timer(logger, "total"):
            bonds = get_bonds_list_daily(cache, client, logger, force_refresh=args.force_refresh_bonds)
            save_excel_bonds_list(bonds, "Moex_Bonds.xlsx", logger)

            df_bonds = pd.DataFrame(bonds)
            if df_bonds.empty or "SECID" not in df_bonds.columns:
                logger.warning("No bonds fetched or missing SECID, stop.")
                return

            df_bonds = df_bonds.dropna(subset=["SECID"]).copy()
            secids = df_bonds["SECID"].astype(str).unique().tolist()
            k = min(max(0, int(args.sample_size)), len(secids))
            if k == 0:
                logger.warning("sample-size=0 or empty list.")
                return

            rnd = random.Random(int(args.seed))
            sample_secids = rnd.sample(secids, k)

            logger.info(
                f"DETAIL sample | k={k} | seed={args.seed} | force_refresh_detail={args.force_refresh_detail} | "
                f"workers={args.detail_workers} | rps={args.detail_rps}"
            )

            sample_df = df_bonds[df_bonds["SECID"].astype(str).isin(sample_secids)].copy()
            sample_df = sample_df.sort_values("SECID").reset_index(drop=True)

            desc_rows_all: List[pd.DataFrame] = []
            ev_rows_all: List[pd.DataFrame] = []
            cp_rows_all: List[pd.DataFrame] = []
            of_rows_all: List[pd.DataFrame] = []
            am_rows_all: List[pd.DataFrame] = []
            em_rows_all: List[Dict[str, Any]] = []

            # --- parallel detail fetch ---
            with Timer(logger, "detail_fetch"):
                from concurrent.futures import ThreadPoolExecutor, as_completed

                workers = max(1, int(args.detail_workers))
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futs = []
                    for i, secid in enumerate(sample_secids, 1):
                        logger.info(f"DETAIL queued {i}/{k} | {secid}")
                        futs.append(
                            ex.submit(
                                _process_one_secid,
                                secid,
                                cache=cache,
                                client=client,
                                logger=logger,
                                sample_df=sample_df,
                                force_refresh=args.force_refresh_detail,
                            )
                        )

                    done = 0
                    for fut in as_completed(futs):
                        done += 1
                        try:
                            secid, drows, erows, crows, orows, arows, emrow = fut.result()
                            logger.info(f"DETAIL done {done}/{k} | {secid}")
                            desc_rows_all.extend(drows)
                            ev_rows_all.extend(erows)
                            cp_rows_all.extend(crows)
                            of_rows_all.extend(orows)
                            am_rows_all.extend(arows)
                            em_rows_all.append(emrow)
                        except Exception as e:
                            logger.warning(f"DETAIL task failed | err={e}")

            summ = cache.requests_summary(start_utc)
            logger.info(f"REQUESTS summary since start | total={summ['total']} | errors={summ['errors']}")

            desc_df = pd.concat(desc_rows_all, ignore_index=True) if desc_rows_all else pd.DataFrame()
            ev_df = pd.concat(ev_rows_all, ignore_index=True) if ev_rows_all else pd.DataFrame()
            cp_df = pd.concat(cp_rows_all, ignore_index=True) if cp_rows_all else pd.DataFrame()
            of_df = pd.concat(of_rows_all, ignore_index=True) if of_rows_all else pd.DataFrame()
            am_df = pd.concat(am_rows_all, ignore_index=True) if am_rows_all else pd.DataFrame()
            em_df = pd.DataFrame(em_rows_all) if em_rows_all else pd.DataFrame()

            if not desc_df.empty:
                cols = list(desc_df.columns)
                if "SECID" in cols:
                    cols = ["SECID"] + [c for c in cols if c != "SECID"]
                    desc_df = desc_df[cols]

            save_excel_detail(
                bonds_sample=sample_df,
                description_df=desc_df,
                events_df=ev_df,
                coupons_df=cp_df,
                offers_df=of_df,
                amort_df=am_df,
                emitents_df=em_df,
                out_path="Moex_Bonds_Detail.xlsx",
                logger=logger,
            )

    finally:
        cache.close()

    elapsed = time.perf_counter() - t0
    logger.info(f"FINISH | elapsed={elapsed:.3f}s")


if __name__ == "__main__":
    main()