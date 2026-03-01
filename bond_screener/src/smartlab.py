from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

from .checkpoint import CheckpointStore
from .writer_queue import AsyncWriter


CAPTCHA_MARKERS = ["captcha", "cloudflare", "verify you are human"]


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
    res = {
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
    return res


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


async def fetch_smartlab(universe: pd.DataFrame, config: dict, writer: AsyncWriter, checkpoints: CheckpointStore, logger) -> dict[str, int | str]:
    cfg = config["smartlab"]
    if not cfg.get("enabled", True):
        return {"done": 0, "failed": 0, "status": "disabled"}

    sem = asyncio.Semaphore(int(cfg.get("concurrency", 4)))
    delay = float(cfg.get("min_delay_s", 0.3))
    timeout_s = int(cfg.get("timeout_s", 30))
    ttl_h = float(cfg.get("ttl_hours", 12))
    now = datetime.utcnow()
    secids = [str(s).upper() for s in universe["secid"].dropna().astype(str).unique()]

    state = checkpoints.state
    smartlab_disabled = False
    disabled_reason = ""
    attempts = 0
    done = 0
    failed = 0

    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        rating_map = await _load_rating_map(client)

        async def one(secid: str) -> None:
            nonlocal smartlab_disabled, disabled_reason, attempts, done, failed
            st = state.get(secid, {})
            if st.get("status") == "done" and st.get("fetched_at"):
                age = (now - datetime.fromisoformat(st["fetched_at"])).total_seconds() / 3600
                if age <= ttl_h:
                    return
            if smartlab_disabled:
                checkpoints.set(secid, {"status": "skipped", "last_error": disabled_reason, "fetched_at": now.isoformat()})
                return
            async with sem:
                await asyncio.sleep(delay)
                for i in range(2):
                    attempts += 1
                    try:
                        resp = await client.get(f"https://smart-lab.ru/q/bonds/{secid}/")
                        text = resp.text
                        blocked = resp.status_code in (403, 429) or any(x in text.lower() for x in CAPTCHA_MARKERS)
                        if blocked:
                            smartlab_disabled = True
                            disabled_reason = f"rate_limited_or_captcha status={resp.status_code} attempts={attempts}"
                            logger.warning("Smart-Lab disabled for run: %s", disabled_reason)
                            checkpoints.set(secid, {"status": "skipped", "last_error": disabled_reason, "fetched_at": datetime.utcnow().isoformat()})
                            return
                        resp.raise_for_status()
                        parsed = _parse_bond_page(secid, text)
                        if not parsed.get("sl_credit_rating") and secid in rating_map:
                            parsed["sl_credit_rating"], parsed["sl_rating_source"] = rating_map[secid]
                        if not parsed.get("sl_credit_rating"):
                            parsed["warning_text"] = "no_smartlab_rating"
                        parsed["fetched_at"] = datetime.utcnow().isoformat()
                        parsed["source_hash"] = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
                        await writer.put("smartlab_bond", [parsed])
                        checkpoints.set(secid, {"status": "done", "last_error": "", "fetched_at": parsed["fetched_at"]})
                        done += 1
                        return
                    except Exception as exc:
                        if i == 1:
                            failed += 1
                            checkpoints.set(
                                secid,
                                {"status": "failed", "last_error": str(exc), "fetched_at": datetime.utcnow().isoformat()},
                            )

        tasks = [asyncio.create_task(one(s)) for s in secids]
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Smart-Lab"):
            await f

    status = "disabled_rate_limited" if smartlab_disabled else ("ok" if failed == 0 else "partial")
    return {"done": done, "failed": failed, "status": status}
