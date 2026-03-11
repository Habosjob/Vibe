import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from tqdm import tqdm
from lxml import etree

# =========================
# CONFIG (настройки скрипта)
# =========================
# Таймаут HTTP-запросов к внешним API (секунды). По умолчанию: 20.
HTTP_TIMEOUT_SECONDS = 20
# Пауза между сетевыми этапами (секунды), чтобы не «ддосить» источники. По умолчанию: 0.05.
STEP_SLEEP_SECONDS = 0.05
# Прогноз ключевой ставки ЦБ по корзинам лет до денежного потока.
# Ключ: порог лет (0, 1, 2, 3...), значение: ставка в % годовых. По умолчанию: консервативный сценарий.
KEY_RATE_FORECAST = {0: 21.0, 1: 19.0, 2: 16.0, 3: 14.0, 5: 12.0}
# Прогноз инфляции для линкеров (ОФЗ-ИН/инфляционных выпусков).
INFLATION_FORECAST = {0: 8.0, 1: 7.0, 2: 6.0, 3: 5.0, 5: 4.0}
# Порог sanity-check для НКД: доля от номинала. Если НКД выше — игнорируем как аномалию.
NCD_FACEVALUE_SANITY_RATIO = 0.2
# Точность отображения доходностей в процентах.
YTM_OUTPUT_PRECISION = 2
# Логи в файл (перезапись на каждый запуск): True/False.
ENABLE_FILE_LOG = True
# Имя лог-файла.
LOG_FILENAME = "ytm_calc.log"
# Включить полуавтоматический режим ручных правок (формула/цена/НКД/частота): True/False.
ENABLE_MANUAL_OVERRIDE = True
# Префикс URL карточки бумаги на Corpbonds (источник формулы купона).
CORPBONDS_BOND_URL_PREFIX = "https://corpbonds.ru/bond/"
# Таймаут запросов к Corpbonds, секунды.
CORPBONDS_REQUEST_TIMEOUT_SECONDS = 30
# User-Agent для Corpbonds (чтобы сайт не резал пустые агенты).
CORPBONDS_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("ytm_calc")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    if ENABLE_FILE_LOG:
        Path("logs").mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(Path("logs") / LOG_FILENAME, mode="w", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    return logger


def _parse_decimal_value(raw_value: object) -> float | None:
    if raw_value is None:
        return None
    value = (
        str(raw_value)
        .replace("\xa0", " ")
        .replace("\u202f", " ")
        .replace("\u2009", " ")
        .replace("−", "-")
        .strip()
    )
    if not value:
        return None
    cleaned = re.sub(r"[^0-9,\.\-]", "", value.replace(" ", ""))
    if not cleaned or cleaned in {"-", ".", ",", "-.", "-,"}:
        return None

    last_comma = cleaned.rfind(",")
    last_dot = cleaned.rfind(".")
    decimal_pos = max(last_comma, last_dot)
    if decimal_pos >= 0:
        int_part = re.sub(r"[^0-9\-]", "", cleaned[:decimal_pos])
        frac_part = re.sub(r"[^0-9]", "", cleaned[decimal_pos + 1 :])
        if not int_part or int_part == "-":
            int_part = "0" if int_part == "" else int_part
        normalized = f"{int_part}.{frac_part}" if frac_part else int_part
    else:
        normalized = re.sub(r"[^0-9\-]", "", cleaned)

    try:
        return float(normalized)
    except ValueError:
        return None


def _parse_bond_date(raw_value: object) -> datetime | None:
    if raw_value is None:
        return None
    date_part = str(raw_value).split("T", 1)[0].strip()
    if not date_part:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y", "%Y.%m.%d"):
        try:
            return datetime.strptime(date_part, fmt)
        except ValueError:
            continue
    return None


def _resolve_coupon_frequency_per_year(raw_coupon_period: object) -> float | None:
    parsed_value = _parse_decimal_value(raw_coupon_period)
    if parsed_value is None or parsed_value <= 0:
        return None
    if parsed_value > 12:
        return 365.25 / parsed_value
    return parsed_value


def _is_fixed_coupon_type(raw_value: object) -> bool:
    value = str(raw_value or "").strip().casefold()
    return value.startswith("фикс")


def _is_floater_coupon_type(raw_value: object) -> bool:
    return "флоат" in str(raw_value or "").strip().casefold()


def _is_other_coupon_type(raw_value: object) -> bool:
    return "проч" in str(raw_value or "").strip().casefold()


def _pick_price_for_ytm(*prices: object) -> float | None:
    for raw_price in prices:
        parsed = _parse_decimal_value(raw_price)
        if parsed is not None and parsed > 0:
            return parsed
    return None


def _normalize_purchase_price(price_percent: float, facevalue: float, nkd: float) -> float:
    return (facevalue * (price_percent / 100.0)) + nkd


def _nominal_periodic_to_effective_annual(rate_nominal: float, periods_per_year: float) -> float:
    if periods_per_year <= 0:
        return rate_nominal
    base = 1.0 + rate_nominal / periods_per_year
    if base <= 0:
        return rate_nominal
    return (base**periods_per_year) - 1.0


def _format_ytm_percent(ytm_decimal: float) -> str:
    return f"{ytm_decimal * 100:.{YTM_OUTPUT_PRECISION}f}"


def _forecast_by_bucket(forecast_cfg: dict[int, float], bucket: int) -> float:
    keys = sorted(forecast_cfg.keys())
    best = keys[0] if keys else 0
    for key in keys:
        if bucket >= key:
            best = key
    return float(forecast_cfg.get(best, 0.0))


def _year_bucket(target_date, valuation_date) -> int:
    delta_days = max(0, (target_date - valuation_date).days)
    return int(delta_days / 365.25)


def _normalize_label(raw_label: str) -> str:
    cleaned = str(raw_label or "").replace("\xa0", " ").replace("?", " ")
    return " ".join(cleaned.split()).casefold()


def parse_corpbonds_page_fields(raw_html: str) -> dict[str, str]:
    parser = etree.HTMLParser(recover=True)
    root = etree.HTML(raw_html, parser=parser)
    parsed = {
        "Цена последняя": "",
        "Тип купона": "",
        "Ставка купона": "",
        "НКД": "",
        "Формула купона": "",
        "Дата ближайшего купона": "",
        "Дата ближайшей оферты": "",
        "Наличие амортизации": "",
        "Купон лесенкой": "",
    }
    if root is None:
        return parsed

    target_tables = root.xpath(
        "//h1[contains(translate(normalize-space(string(.)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'доходность')]/following::table[1]"
        " | //*[contains(translate(normalize-space(string(.)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'купонные выплаты')]/following::table[1]"
        " | //*[contains(translate(normalize-space(string(.)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'доходность и цена')]/following::table[1]"
    )
    if not target_tables:
        target_tables = root.xpath("//table")

    for row in [r for table in target_tables for r in table.xpath('.//tr[td]')]:
        tds = row.xpath("./td")
        if len(tds) < 2:
            continue
        label = " ".join(" ".join(tds[0].itertext()).split())
        value = " ".join(" ".join(tds[-1].itertext()).split())
        if not label:
            continue
        normalized = _normalize_label(label)
        if normalized.startswith("цена последняя"):
            parsed["Цена последняя"] = value
        elif normalized.startswith("тип купона"):
            parsed["Тип купона"] = value
        elif normalized.startswith("ставка купона"):
            parsed["Ставка купона"] = value
        elif normalized.startswith("накопленный купонный доход (нкд)") or normalized == "нкд":
            parsed["НКД"] = value
        elif normalized.startswith("формула купона") or normalized.startswith("формула флоатера"):
            parsed["Формула купона"] = value
        elif normalized.startswith("дата ближайшего купона"):
            parsed["Дата ближайшего купона"] = value
        elif normalized.startswith("дата ближайшей оферты"):
            parsed["Дата ближайшей оферты"] = value
        elif normalized.startswith("наличие амортизации"):
            parsed["Наличие амортизации"] = value
        elif normalized.startswith("купон лесенкой"):
            parsed["Купон лесенкой"] = value
    return parsed


def _fetch_corpbonds_payload(secid: str, logger: logging.Logger) -> dict[str, str]:
    url = f"{CORPBONDS_BOND_URL_PREFIX}{secid}"
    logger.info("GET %s", url)
    response = requests.get(
        url,
        timeout=CORPBONDS_REQUEST_TIMEOUT_SECONDS,
        headers={
            "User-Agent": CORPBONDS_USER_AGENT,
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    response.raise_for_status()
    return parse_corpbonds_page_fields(response.text)


def _solve_nominal_periodic_ytm_bisection(dirty_price: float, coupon_frequency: float, cashflows: list[tuple[float, float]]) -> float | None:
    if not cashflows:
        return None

    def npv(rate: float) -> float:
        discount = 1.0 + rate / coupon_frequency
        if discount <= 0:
            return float("inf")
        total = 0.0
        for years, amount in cashflows:
            total += amount / (discount ** (years * coupon_frequency))
        return total - dirty_price

    left, right = -0.95, 5.0
    left_val, right_val = npv(left), npv(right)
    if left_val * right_val > 0:
        return None
    for _ in range(120):
        mid = (left + right) / 2
        mid_val = npv(mid)
        if abs(mid_val) < 1e-8:
            return mid
        if left_val * mid_val <= 0:
            right = mid
        else:
            left, left_val = mid, mid_val
    return (left + right) / 2


def parse_floater_terms(formula_raw: object) -> tuple[str, float | None, float] | None:
    formula = str(formula_raw or "")
    if not formula.strip():
        return None
    normalized = formula.casefold().replace("ё", "е")
    zcyc_match = re.search(r"(?:g\s*[- ]?curve|gcurve|zcyc)\s*([0-9]+(?:\.[0-9]+)?)\s*(?:y|yr|year|лет|год(?:а|ов)?)", normalized)
    if zcyc_match:
        index_type = "zcyc"
        tenor_years = float(zcyc_match.group(1))
    elif "ruonia" in normalized:
        index_type = "ruonia"
        tenor_years = None
    elif "ключ" in normalized or "цб" in normalized or "key rate" in normalized or re.search(r"(^|[^a-zа-я])(kc|кс)([^a-zа-я]|$)", normalized):
        index_type = "key"
        tenor_years = None
    else:
        return None

    prem = re.search(r"([+\-])\s*([0-9]+(?:[\.,][0-9]+)?)\s*%", normalized)
    premium = 0.0
    if prem:
        sign = -1.0 if prem.group(1) == "-" else 1.0
        val = _parse_decimal_value(prem.group(2)) or 0.0
        premium = sign * val
    return index_type, tenor_years, premium


def pick_zcyc_point(zcyc: dict[object, object], tenor_years: float | None) -> float | None:
    if tenor_years is None or not zcyc:
        return None
    normalized = {}
    for k, v in zcyc.items():
        kk, vv = _parse_decimal_value(k), _parse_decimal_value(v)
        if kk is not None and vv is not None:
            normalized[float(kk)] = float(vv)
    if not normalized:
        return None
    points = sorted(normalized.items())
    if tenor_years <= points[0][0]:
        return points[0][1]
    if tenor_years >= points[-1][0]:
        return points[-1][1]
    for i in range(1, len(points)):
        l_t, l_y = points[i - 1]
        r_t, r_y = points[i]
        if l_t <= tenor_years <= r_t:
            alpha = (tenor_years - l_t) / (r_t - l_t)
            return l_y + alpha * (r_y - l_y)
    return None


@dataclass
class BondData:
    isin: str
    secid: str
    shortname: str
    coupon_type: str
    coupon_formula: str
    coupon_percent: float
    coupon_period_days: float
    coupon_frequency: float
    next_coupon_date: str
    facevalue: float
    faceunit: str
    nkd: float
    matdate: str
    offerdate: str
    price_percent: float
    amort_schedule: list[tuple[datetime, float]]
    corpbonds_coupon_type: str
    corpbonds_coupon_formula: str
    corpbonds_coupon_rate: float
    corpbonds_nkd: float
    corpbonds_price: float
    corpbonds_next_coupon_date: str
    corpbonds_offerdate: str
    corpbonds_has_amort: str


def _http_get_json(url: str, logger: logging.Logger) -> dict[str, Any]:
    logger.info("GET %s", url)
    resp = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()


def _fetch_cbr_reference_data(logger: logging.Logger) -> dict[str, object]:
    payload: dict[str, object] = {"key_rate": None, "ruonia": None, "zcyc": {}}

    try:
        key_json = _http_get_json("https://www.cbr-xml-daily.ru/daily_json.js", logger)
        payload["key_rate"] = _parse_decimal_value(key_json.get("KeyRate"))
    except Exception:
        logger.exception("Не удалось получить key rate")

    try:
        ruonia_text = requests.get("https://cbr.ru/hd_base/ruonia/dynamics/", timeout=HTTP_TIMEOUT_SECONDS).text
        ruonia_match = re.search(r"ruonia[^0-9]*([0-9]+,[0-9]+)", ruonia_text, flags=re.IGNORECASE)
        payload["ruonia"] = _parse_decimal_value(ruonia_match.group(1)) if ruonia_match else None
    except Exception:
        logger.exception("Не удалось получить RUONIA")

    payload["zcyc"] = {}
    return payload


def _get_table(json_data: dict[str, Any], name: str) -> list[dict[str, Any]]:
    block = json_data.get(name, {})
    cols = block.get("columns", [])
    rows = block.get("data", [])
    result = []
    for row in rows:
        result.append({cols[i]: row[i] if i < len(row) else None for i in range(len(cols))})
    return result




def _description_to_dict(json_data: dict[str, Any]) -> dict[str, Any]:
    block = json_data.get("description", {})
    rows = block.get("data", [])
    out: dict[str, Any] = {}
    for row in rows:
        if len(row) >= 3:
            out[str(row[0])] = row[2]
    return out


def _pick_primary_board(json_data: dict[str, Any]) -> str | None:
    boards = _get_table(json_data, "boards")
    for row in boards:
        if str(row.get("market", "")).lower() == "bonds" and int(row.get("is_primary") or 0) == 1:
            return str(row.get("boardid") or "")
    for row in boards:
        if str(row.get("market", "")).lower() == "bonds":
            return str(row.get("boardid") or "")
    return None


def _fetch_latest_history_price(secid: str, boardid: str, logger: logging.Logger) -> float | None:
    url = (
        f"https://iss.moex.com/iss/history/engines/stock/markets/bonds/boards/{boardid}/securities/{secid}.json"
        "?iss.meta=off&history.columns=CLOSE,LEGALCLOSEPRICE,MARKETPRICE3&start=0"
    )
    js = _http_get_json(url, logger)
    rows = _get_table(js, "history")
    if not rows:
        return None
    last = rows[-1]
    return _pick_price_for_ytm(last.get("CLOSE"), last.get("MARKETPRICE3"), last.get("LEGALCLOSEPRICE"))

def _fetch_bond_by_isin(isin: str, logger: logging.Logger) -> BondData:
    search_url = f"https://iss.moex.com/iss/securities.json?q={isin}&iss.meta=off&securities.columns=secid,isin,shortname"
    search = _http_get_json(search_url, logger)
    candidates = _get_table(search, "securities")
    match = next((x for x in candidates if str(x.get("isin", "")).upper() == isin.upper()), None)
    if not match:
        raise ValueError("ISIN не найден в MOEX ISS")
    secid = str(match.get("secid") or "").strip()
    if not secid:
        raise ValueError("Не удалось определить SECID")

    card = _http_get_json(f"https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off", logger)
    desc = _description_to_dict(card)

    detail_url = (
        f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json?iss.meta=off"
        "&securities.columns=SECID,SHORTNAME,FACEVALUE,FACEUNIT,MATDATE,OFFERDATE,COUPONPERCENT,COUPONPERIOD,ACCRUEDINT"
        "&marketdata.columns=SECID,LAST,MARKETPRICE3,LEGALCLOSEPRICE"
    )
    detail = _http_get_json(detail_url, logger)
    sec_rows = _get_table(detail, "securities")
    md_rows = _get_table(detail, "marketdata")
    sec_row = sec_rows[0] if sec_rows else {}
    md_row = md_rows[0] if md_rows else {}

    bondization_url = f"https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{secid}.json?iss.meta=off"
    bondization = _http_get_json(bondization_url, logger)
    coupons = _get_table(bondization, "coupons")
    amorts = _get_table(bondization, "amortizations")

    corpbonds_payload = _fetch_corpbonds_payload(secid, logger)

    next_coupon_date = ""
    formula = ""
    coupon_type = "Фиксированный"
    if coupons:
        future = [c for c in coupons if (_parse_bond_date(c.get("coupondate")) or datetime.min).date() >= datetime.now().date()]
        first = future[0] if future else coupons[0]
        next_coupon_date = str(first.get("coupondate") or "")
        formula = str(first.get("formula") or "")
        if formula:
            coupon_type = "Флоатер"

    amort_schedule = []
    for row in amorts:
        dt = _parse_bond_date(row.get("amortdate"))
        val = _parse_decimal_value(row.get("value"))
        if dt and val and val > 0:
            amort_schedule.append((dt, val))

    price_percent = _pick_price_for_ytm(md_row.get("LAST"), md_row.get("MARKETPRICE3"), md_row.get("LEGALCLOSEPRICE"))
    if price_percent is None:
        boardid = _pick_primary_board(card)
        if boardid:
            price_percent = _fetch_latest_history_price(secid, boardid, logger)
    if price_percent is None:
        logger.warning("Не найдена рыночная цена для %s, fallback=100%%", isin)
        price_percent = 100.0

    coupon_period_days = _parse_decimal_value(sec_row.get("COUPONPERIOD") or desc.get("COUPONPERIOD")) or 182.0
    coupon_frequency = _resolve_coupon_frequency_per_year(coupon_period_days) or 2.0

    return BondData(
        isin=isin.upper(),
        secid=secid,
        shortname=str(sec_row.get("SHORTNAME") or desc.get("SHORTNAME") or match.get("shortname") or ""),
        coupon_type=coupon_type,
        coupon_formula=formula,
        coupon_percent=_parse_decimal_value(sec_row.get("COUPONPERCENT") or desc.get("COUPONPERCENT")) or 0.0,
        coupon_period_days=coupon_period_days,
        coupon_frequency=coupon_frequency,
        next_coupon_date=next_coupon_date,
        facevalue=_parse_decimal_value(sec_row.get("FACEVALUE") or desc.get("FACEVALUE")) or 1000.0,
        faceunit=str(sec_row.get("FACEUNIT") or desc.get("FACEUNIT") or "RUB"),
        nkd=_parse_decimal_value(sec_row.get("ACCRUEDINT") or desc.get("ACCRUEDINT")) or 0.0,
        matdate=str(sec_row.get("MATDATE") or desc.get("MATDATE") or ""),
        offerdate=str(sec_row.get("OFFERDATE") or desc.get("OFFERDATE") or ""),
        price_percent=price_percent,
        amort_schedule=amort_schedule,
        corpbonds_coupon_type=str(corpbonds_payload.get("Тип купона", "") or ""),
        corpbonds_coupon_formula=str(corpbonds_payload.get("Формула купона", "") or ""),
        corpbonds_coupon_rate=_parse_decimal_value(corpbonds_payload.get("Ставка купона")) or 0.0,
        corpbonds_nkd=_parse_decimal_value(corpbonds_payload.get("НКД")) or 0.0,
        corpbonds_price=_parse_decimal_value(corpbonds_payload.get("Цена последняя")) or 0.0,
        corpbonds_next_coupon_date=str(corpbonds_payload.get("Дата ближайшего купона", "") or ""),
        corpbonds_offerdate=str(corpbonds_payload.get("Дата ближайшей оферты", "") or ""),
        corpbonds_has_amort=str(corpbonds_payload.get("Наличие амортизации", "") or ""),
    )


def _build_coupon_dates(target_date: datetime, coupon_frequency: float, coupon_period_days: object, next_coupon_date: object) -> list[datetime.date]:
    period_days = int(_parse_decimal_value(coupon_period_days) or (365.25 / coupon_frequency))
    first_coupon_dt = _parse_bond_date(str(next_coupon_date or ""))
    first_coupon = first_coupon_dt.date() if first_coupon_dt is not None else None
    today = datetime.now().date()
    target = target_date.date()
    coupon_dates = []

    if first_coupon is not None and first_coupon > today:
        current = first_coupon
        while current <= target and len(coupon_dates) < 500:
            coupon_dates.append(current)
            current = current.fromordinal(current.toordinal() + period_days)
    else:
        current = today.fromordinal(today.toordinal() + period_days)
        while current <= target and len(coupon_dates) < 500:
            coupon_dates.append(current)
            current = current.fromordinal(current.toordinal() + period_days)
    return coupon_dates


def _calc_result(data: BondData, cbr_data: dict[str, object], logger: logging.Logger) -> dict[str, str]:
    target_date = _parse_bond_date(data.corpbonds_offerdate) or _parse_bond_date(data.offerdate) or _parse_bond_date(data.matdate)
    if not target_date:
        raise ValueError("Нет даты оферты/погашения")

    if data.facevalue > 0 and data.nkd > data.facevalue * NCD_FACEVALUE_SANITY_RATIO:
        logger.warning("Suspicious NKD=%s for %s", data.nkd, data.isin)
        data.nkd = 0.0

    input_price = data.corpbonds_price if data.corpbonds_price > 0 else data.price_percent
    input_nkd = data.corpbonds_nkd if data.corpbonds_nkd > 0 else data.nkd
    dirty_price = _normalize_purchase_price(input_price, data.facevalue, input_nkd)
    coupon_freq = data.coupon_frequency
    next_coupon_dt = data.corpbonds_next_coupon_date or data.next_coupon_date
    coupon_dates = _build_coupon_dates(target_date, coupon_freq, data.coupon_period_days, next_coupon_dt)

    principal = data.facevalue
    amort_map = {d.date(): p for d, p in data.amort_schedule if p > 0}
    cashflows = []

    effective_coupon_type = data.corpbonds_coupon_type or data.coupon_type
    effective_formula = data.corpbonds_coupon_formula or data.coupon_formula
    effective_coupon_percent = data.corpbonds_coupon_rate if data.corpbonds_coupon_rate > 0 else data.coupon_percent

    if _is_floater_coupon_type(effective_coupon_type):
        terms = parse_floater_terms(effective_formula)
        if not terms:
            raise ValueError("Не удалось распарсить формулу флоатера. Введите формулу вручную.")
        index_type, tenor, premium = terms
        key_rate = _parse_decimal_value(cbr_data.get("key_rate"))
        if key_rate is None:
            key_rate = float(KEY_RATE_FORECAST.get(0, 0.0))
        if index_type == "key":
            spread_to_key = 0.0
        elif index_type == "ruonia":
            ruonia = _parse_decimal_value(cbr_data.get("ruonia"))
            if ruonia is None:
                raise ValueError("Не найдена RUONIA")
            spread_to_key = key_rate - ruonia
        else:
            idx = pick_zcyc_point(cbr_data.get("zcyc", {}), tenor)
            if idx is None:
                spread_to_key = 0.0
            else:
                spread_to_key = key_rate - idx

        event_dates = sorted(set(coupon_dates) | set(amort_map.keys()) | {target_date.date()})
        for dt in event_dates:
            if dt <= datetime.now().date():
                continue
            amount = 0.0
            if dt in coupon_dates:
                bucket = _year_bucket(dt, datetime.now().date())
                idx_forecast = _forecast_by_bucket(KEY_RATE_FORECAST, bucket) - spread_to_key
                coupon_rate = idx_forecast + premium
                amount += principal * (coupon_rate / 100.0) / coupon_freq
            if dt in amort_map:
                pay = min(principal, amort_map[dt])
                amount += pay
                principal -= pay
            if dt == target_date.date() and principal > 0:
                amount += principal
            years = (dt - datetime.now().date()).days / 365.25
            if amount > 0 and years > 0:
                cashflows.append((years, amount))
    else:
        period_coupon = data.facevalue * (effective_coupon_percent / 100.0) / coupon_freq
        event_dates = sorted(set(coupon_dates) | set(amort_map.keys()) | {target_date.date()})
        for dt in event_dates:
            if dt <= datetime.now().date():
                continue
            amount = 0.0
            if dt in coupon_dates:
                amount += principal * (effective_coupon_percent / 100.0) / coupon_freq
            if dt in amort_map:
                pay = min(principal, amort_map[dt])
                amount += pay
                principal -= pay
            if dt == target_date.date() and principal > 0:
                amount += principal
            years = (dt - datetime.now().date()).days / 365.25
            if amount > 0 and years > 0:
                cashflows.append((years, amount))

    if not cashflows:
        raise ValueError("Пустые денежные потоки")

    ytm_nominal = _solve_nominal_periodic_ytm_bisection(dirty_price, coupon_freq, cashflows)
    if ytm_nominal is None:
        raise ValueError("Не удалось решить YTM")
    ytm_effective = _nominal_periodic_to_effective_annual(ytm_nominal, coupon_freq)

    years_to_target = (target_date.date() - datetime.now().date()).days / 365.25
    total_income = sum(cf for _, cf in cashflows) - dirty_price
    simple_yield_to_maturity = (total_income / dirty_price) / years_to_target if years_to_target > 0 else 0.0
    annual_coupon_money = data.facevalue * (effective_coupon_percent / 100.0)
    current_yield = annual_coupon_money / dirty_price if dirty_price > 0 else 0.0

    simple_margin = ""
    if _is_floater_coupon_type(effective_coupon_type):
        discount_spread = ((data.facevalue - dirty_price) / data.facevalue) / years_to_target * 100 if years_to_target > 0 else 0.0
        simple_margin = f"{discount_spread:.2f}%"

    return {
        "ISIN": data.isin,
        "Короткое наименование": data.shortname,
        "Цена с учетом НКД": f"{dirty_price:.2f}",
        "YTM": f"{_format_ytm_percent(ytm_effective)}%",
        "Доходность к погашению": f"{simple_yield_to_maturity * 100:.2f}%",
        "Периодичность платежей": f"{coupon_freq:.2f}",
        "ТКД": f"{current_yield * 100:.2f}%",
        "Simple margin для флоатеров": simple_margin,
        "Формула": effective_formula or "-",
    }


def _manual_override(data: BondData) -> BondData:
    if not ENABLE_MANUAL_OVERRIDE:
        return data
    choice = input("\nВключить ручную корректировку данных? (y/N): ").strip().lower()
    if choice not in {"y", "yes", "д", "да"}:
        return data

    shown_formula = data.corpbonds_coupon_formula or data.coupon_formula
    formula = input(f"Формула купона [{shown_formula}]: ").strip()
    price = input(f"Цена, % [{data.price_percent}]: ").strip()
    nkd = input(f"НКД [{data.nkd}]: ").strip()
    ctype = input(f"Тип купона (Фиксированный/Флоатер/Прочее) [{data.coupon_type}]: ").strip()
    freq = input(f"Частота выплат в год [{data.coupon_frequency}]: ").strip()

    if formula:
        data.corpbonds_coupon_formula = formula
        data.coupon_formula = formula
    if price:
        data.price_percent = _parse_decimal_value(price) or data.price_percent
    if nkd:
        data.nkd = _parse_decimal_value(nkd) or data.nkd
    if ctype:
        data.coupon_type = ctype
    if freq:
        data.coupon_frequency = _parse_decimal_value(freq) or data.coupon_frequency
    return data


def _print_table(result: dict[str, str]) -> None:
    headers = list(result.keys())
    values = [result[h] for h in headers]
    width = max(len(h) for h in headers) + 2
    print("\n" + "=" * 90)
    for h, v in zip(headers, values):
        print(f"{h:<{width}} {v}")
    print("=" * 90)


def _run_stage(title: str, steps: int, fn):
    print(f"\n=====\n{title}")
    start = time.perf_counter()
    with tqdm(total=steps, desc=title, position=0, leave=False) as pbar:
        result = fn(pbar)
    elapsed = time.perf_counter() - start
    return result, elapsed


def main() -> None:
    logger = _setup_logger()
    total_start = time.perf_counter()
    stage_stats: list[tuple[str, float]] = []

    print("Введите ISIN (или 'exit' для выхода):")
    while True:
        isin = input("> ").strip()
        if not isin:
            continue
        if isin.lower() in {"exit", "quit", "q"}:
            break

        try:
            cbr_data, t1 = _run_stage(
                "Этап 1. Загрузка референсов ЦБ",
                3,
                lambda p: (
                    p.update(1),
                    time.sleep(STEP_SLEEP_SECONDS),
                    p.update(1),
                    _fetch_cbr_reference_data(logger),
                    p.update(1),
                )[-2],
            )
            stage_stats.append(("Этап 1", t1))

            bond_data, t2 = _run_stage(
                "Этап 2. Загрузка данных облигации",
                3,
                lambda p: (
                    p.update(1),
                    _fetch_bond_by_isin(isin, logger),
                    p.update(1),
                    time.sleep(STEP_SLEEP_SECONDS),
                    p.update(1),
                )[1],
            )
            stage_stats.append(("Этап 2", t2))

            bond_data, t3 = _run_stage(
                "Этап 3. Ручная корректировка",
                2,
                lambda p: (
                    p.update(1),
                    _manual_override(bond_data),
                    p.update(1),
                )[1],
            )
            stage_stats.append(("Этап 3", t3))

            result, t4 = _run_stage(
                "Этап 4. Расчет доходностей",
                2,
                lambda p: (
                    p.update(1),
                    _calc_result(bond_data, cbr_data, logger),
                    p.update(1),
                )[1],
            )
            stage_stats.append(("Этап 4", t4))
            _print_table(result)

        except Exception as exc:
            logger.exception("Ошибка расчета по %s", isin)
            print(f"Ошибка: {exc}")

        print("\nВведите следующий ISIN (или 'exit' для выхода):")

    total_elapsed = time.perf_counter() - total_start
    print("\n=====\nSummary по этапам")
    for name, seconds in stage_stats:
        print(f"- {name}: {seconds:.2f} сек")
    print(f"- Весь скрипт: {total_elapsed:.2f} сек")


if __name__ == "__main__":
    main()
