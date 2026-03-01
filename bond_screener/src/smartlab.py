from __future__ import annotations

import asyncio
import hashlib
import re
import time
from datetime import datetime

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

from .checkpoint import CheckpointStore
from .writer_queue import AsyncWriter


CAPTCHA_MARKERS = ["captcha", "cloudflare", "verify you are human"]


class AsyncTokenBucket:
    def __init__(self, rps_limit: float, burst: int):
        self.enabled = rps_limit > 0
        self.rate = float(rps_limit)
        self.capacity = max(1.0, float(burst))
        self.tokens = self.capacity
        self.updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if not self.enabled:
            return
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait_s = max((1.0 - self.tokens) / self.rate, 0.01)
            await asyncio.sleep(wait_s)


def _extract_number(text: str) -> float | None:
    if not text:
        return None
    s = re.sub(r"[^0-9,.-]", "", text).replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _extract_rating(text: str) -> tuple[str | None, str | None]:
    m = re.search(r"\b(AAA|AA\+|AA-|AA|A\+|A-|A|BBB\+|BBB-|BBB|BB\+|BB-|BB|B\+|B-|B|CCC|CC|C|D)\b", text.upper())
    if not m:
        return None, None
    src = None
    for s in ["АКРА", "ЭКСПЕРТ РА", "НКР"]:
        if s.lower() in text.lower():
            src = s
            break
    return m.group(1), src


def _parse_bond_page(secid: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    rating, rating_src = _extract_rating(text)
    return {
        "secid": secid,
        "sl_price_rub": _extract_number(text[text.lower().find("цена") : text.lower().find("цена") + 80]) if "цена" in text.lower() else None,
        "sl_price_pct": _extract_number(text[text.lower().find("%") - 20 : text.lower().find("%") + 20]) if "%" in text else None,
        "sl_ytm": _extract_number(text[text.lower().find("ytm") : text.lower().find("ytm") + 60]) if "ytm" in text.lower() else None,
        "sl_nkd_rub": _extract_number(text[text.lower().find("нкд") : text.lower().find("нкд") + 80]) if "нкд" in text.lower() else None,
        "sl_coupon_rub": _extract_number(text[text.lower().find("купон") : text.lower().find("купон") + 80]) if "купон" in text.lower() else None,
        "sl_coupon_rate_pct": None,
        "sl_coupon_freq_per_year": None,
        "sl_next_coupon_date_ddmmyyyy": None,
        "sl_maturity_date_ddmmyyyy": None,
        "sl_offer_date_ddmmyyyy": None,
        "sl_is_qual": "да" if "квал" in text.lower() and "да" in text.lower() else None,
        "sl_credit_rating": rating,
        "sl_rating_source": rating_src,
        "warning_text": "",
    }


def _is_blocked_response(resp: httpx.Response, trip_statuses: set[int], trip_keywords: list[str]) -> bool:
    text = resp.text.lower()
    return resp.status_code in trip_statuses or any(k in text for k in trip_keywords)


async def _load_rating_map(client: httpx.AsyncClient) -> dict[str, tuple[str | None, str | None]]:
    m: dict[str, tuple[str | None, str | None]] = {}
    resp = await client.get("https://smart-lab.ru/q/bonds/")
    if resp.status_code >= 400:
        return m
    soup = BeautifulSoup(resp.text, "html.parser")
    for tr in soup.select("tr"):
        t = tr.get_text(" ", strip=True)
        secid_m = re.search(r"\b[A-Z0-9]{4,20}\b", t)
        if not secid_m:
            continue
        rating, source = _extract_rating(t)
        if rating:
            m[secid_m.group(0)] = (rating, source)
    return m


async def fetch_smartlab(universe: pd.DataFrame, config: dict, writer: AsyncWriter, checkpoints: CheckpointStore, logger) -> dict[str, int | str | float]:
    cfg = config["smartlab"]
    if not cfg.get("enabled", True):
        return {"done": 0, "failed": 0, "skipped": 0, "status": "disabled", "avg_rps": 0.0}
    if not cfg.get("per_secid_enabled", True):
        return {"done": 0, "failed": 0, "skipped": 0, "status": "disabled_per_secid", "avg_rps": 0.0}

    concurrency = int(cfg.get("concurrency", 4))
    max_connections = max(int(cfg.get("max_connections", max(20, concurrency))), concurrency)
    max_keepalive = int(cfg.get("max_keepalive", min(concurrency, 20)))
    timeout_s = float(cfg.get("timeout_s", 30))
    ttl_h = float(cfg.get("ttl_hours", 12))
    min_delay_s = float(cfg.get("min_delay_s", 0.0))
    max_retries = int(cfg.get("max_retries", 2))
    backoff_initial_s = float(cfg.get("backoff_initial_s", 0.2))
    backoff_max_s = float(cfg.get("backoff_max_s", 2.0))

    cb_cfg = cfg.get("circuit_breaker", {})
    cb_enabled = bool(cb_cfg.get("enabled", True))
    trip_statuses = set(cb_cfg.get("trip_statuses", [403, 429]))
    trip_keywords = [str(v).lower() for v in cb_cfg.get("trip_on_captcha_keywords", CAPTCHA_MARKERS)]
    disable_for_run = bool(cb_cfg.get("disable_for_run", True))

    limiter = AsyncTokenBucket(float(cfg.get("rps_limit", 0)), int(cfg.get("burst", 1)))
    sem = asyncio.Semaphore(concurrency)
    now = datetime.utcnow()
    secids = [str(s).upper() for s in universe["secid"].dropna().astype(str).unique()]

    state = checkpoints.state
    smartlab_disabled = False
    disabled_reason = ""
    done = 0
    failed = 0
    skipped = 0
    t_start = time.monotonic()

    limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_keepalive)
    timeout = httpx.Timeout(timeout_s)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, limits=limits) as client:
        rating_map = await _load_rating_map(client)

        async def one(secid: str) -> None:
            nonlocal smartlab_disabled, disabled_reason, done, failed, skipped
            st = state.get(secid, {})
            if st.get("status") == "done" and st.get("fetched_at"):
                age = (now - datetime.fromisoformat(st["fetched_at"])).total_seconds() / 3600
                if age <= ttl_h:
                    skipped += 1
                    return
            if smartlab_disabled:
                checkpoints.set(secid, {"status": "skipped", "last_error": "disabled_rate_limited", "fetched_at": now.isoformat()})
                skipped += 1
                return

            async with sem:
                if min_delay_s > 0:
                    await asyncio.sleep(min_delay_s)
                for attempt in range(max_retries + 1):
                    await limiter.acquire()
                    try:
                        resp = await client.get(f"https://smart-lab.ru/q/bonds/{secid}/")
                        if cb_enabled and _is_blocked_response(resp, trip_statuses, trip_keywords):
                            if disable_for_run:
                                smartlab_disabled = True
                            disabled_reason = f"rate_limited_or_captcha status={resp.status_code}"
                            logger.warning("Smart-Lab disabled for run: %s", disabled_reason)
                            checkpoints.set(secid, {"status": "skipped", "last_error": disabled_reason, "fetched_at": datetime.utcnow().isoformat()})
                            skipped += 1
                            return
                        resp.raise_for_status()

                        parsed = _parse_bond_page(secid, resp.text)
                        if not parsed.get("sl_credit_rating") and secid in rating_map:
                            parsed["sl_credit_rating"], parsed["sl_rating_source"] = rating_map[secid]
                        if not parsed.get("sl_credit_rating"):
                            parsed["warning_text"] = "no_smartlab_rating"
                        parsed["fetched_at"] = datetime.utcnow().isoformat()
                        parsed["source_hash"] = hashlib.sha256(resp.text.encode("utf-8", errors="ignore")).hexdigest()
                        await writer.put("smartlab_bond", [parsed])
                        checkpoints.set(secid, {"status": "done", "last_error": "", "fetched_at": parsed["fetched_at"]})
                        done += 1
                        return
                    except Exception as exc:
                        if attempt >= max_retries:
                            failed += 1
                            checkpoints.set(
                                secid,
                                {"status": "failed", "last_error": str(exc), "fetched_at": datetime.utcnow().isoformat()},
                            )
                            return
                        backoff = min(backoff_max_s, backoff_initial_s * (2**attempt))
                        await asyncio.sleep(backoff)

        tasks = [asyncio.create_task(one(s)) for s in secids]
        pbar = tqdm(total=len(tasks), desc="Smart-Lab", position=1, leave=False, dynamic_ncols=True)
        for f in asyncio.as_completed(tasks):
            await f
            pbar.update(1)
        pbar.close()

    elapsed = max(time.monotonic() - t_start, 1e-9)
    status = "disabled_rate_limited" if smartlab_disabled else ("ok" if failed == 0 else "partial")
    return {
        "done": done,
        "failed": failed,
        "skipped": skipped,
        "status": status,
        "avg_rps": round((done + failed) / elapsed, 3),
    }
