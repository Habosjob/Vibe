from __future__ import annotations

import json
import logging
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
import requests
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter
from tqdm import tqdm

BASE_URL = "https://iss.moex.com/iss"
OUTPUT_DIR = Path(__file__).resolve().parent
LOG_FILE = OUTPUT_DIR / "main.log"
SHARES_FILE = OUTPUT_DIR / "moex_shares.xlsx"
BONDS_FILE = OUTPUT_DIR / "moex_bonds.xlsx"
EMITTERS_FILE = OUTPUT_DIR / "moex_emitters.xlsx"
REQUEST_TIMEOUT = 30
MAX_WORKERS = 24
CACHE_FILE = OUTPUT_DIR / "emitter_cache.json"
HEADER_FILL = PatternFill(fill_type="solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
ZEBRA_FILL = PatternFill(fill_type="solid", fgColor="E8F2FF")


def progress(total: int, desc: str, unit: str):
    return tqdm(total=total, desc=desc, unit=unit, position=0, leave=False, dynamic_ncols=True)


def load_cache(logger: logging.Logger) -> dict[str, dict[str, Any]]:
    if not CACHE_FILE.exists():
        return {"secid_to_emitter": {}, "emitters": {}}

    try:
        with CACHE_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, dict):
            return {
                "secid_to_emitter": data.get("secid_to_emitter", {}),
                "emitters": data.get("emitters", {}),
            }
    except Exception as error:
        logger.exception("Cache load failed: %s", error)

    return {"secid_to_emitter": {}, "emitters": {}}


def save_cache(cache: dict[str, dict[str, Any]], logger: logging.Logger) -> None:
    try:
        with CACHE_FILE.open("w", encoding="utf-8") as file:
            json.dump(cache, file, ensure_ascii=False, indent=2)
    except Exception as error:
        logger.exception("Cache save failed: %s", error)


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("moex_export")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


class MoexClient:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/5.0"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{BASE_URL}{endpoint}"
        response = self.session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        self.logger.info("GET %s params=%s status=%s", url, params, response.status_code)
        return response.json()

    def fetch_market_securities(self, market: str, columns: list[str]) -> pd.DataFrame:
        with progress(total=1, desc=f"MOEX {market}", unit="запрос") as pbar:
            data = self._get(
                f"/engines/stock/markets/{market}/securities.json",
                params={"iss.meta": "off", "iss.only": "securities", "securities.columns": ",".join(columns)},
            )
            pbar.update(1)

        return pd.DataFrame(data.get("securities", {}).get("data", []), columns=data.get("securities", {}).get("columns", []))

    def fetch_emitter_id_by_secid(self, secid: str) -> int | None:
        data = self._get(
            f"/securities/{secid}.json",
            params={"iss.meta": "off", "iss.only": "description"},
        )
        rows = data.get("description", {}).get("data", [])
        mapping = {row[0]: row[2] for row in rows if len(row) >= 3}
        emitter_id = mapping.get("EMITTER_ID") or mapping.get("EMITENT_ID")
        try:
            return int(emitter_id) if emitter_id is not None else None
        except (TypeError, ValueError):
            return None

    def fetch_emitter_info(self, emitter_id: int) -> dict[str, Any]:
        data = self._get(
            f"/emitters/{emitter_id}.json",
            params={"iss.meta": "off", "iss.only": "emitter", "emitter.columns": "EMITTER_ID,SHORT_TITLE,INN"},
        )
        row = data.get("emitter", {}).get("data", [])
        if not row:
            return {"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None}
        return {"EMITTER_ID": int(row[0][0]), "EMITTER_NAME": row[0][1], "INN": row[0][2]}


def enrich_emitters(
    client: MoexClient,
    shares: pd.DataFrame,
    bonds: pd.DataFrame,
    logger: logging.Logger,
    cache: dict[str, dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "EMITTER_ID" not in shares.columns:
        shares["EMITTER_ID"] = pd.NA
    if "EMITTER_ID" not in bonds.columns:
        bonds["EMITTER_ID"] = pd.NA

    existing_pairs = pd.concat([shares[["SECID", "EMITTER_ID"]], bonds[["SECID", "EMITTER_ID"]]], ignore_index=True)
    cached_pairs = pd.DataFrame(
        [{"SECID": secid, "EMITTER_ID": emitter_id} for secid, emitter_id in cache.get("secid_to_emitter", {}).items()]
    )
    existing_pairs = pd.concat([existing_pairs, cached_pairs], ignore_index=True)
    existing_pairs = existing_pairs.dropna(subset=["EMITTER_ID"]).drop_duplicates(subset=["SECID"], keep="first")
    existing_secids = set(existing_pairs["SECID"].tolist())

    secids = sorted((set(shares["SECID"].tolist()) | set(bonds["SECID"].tolist())) - existing_secids)
    logger.info("Emitter enrichment start for secids=%s", len(secids))

    secid_rows: list[dict[str, Any]] = existing_pairs.to_dict("records")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(client.fetch_emitter_id_by_secid, secid): secid for secid in secids}
        with progress(total=len(futures), desc="Определение EMITTER_ID", unit="бумага") as pbar:
            for future in as_completed(futures):
                secid = futures[future]
                try:
                    emitter_id = future.result()
                except requests.RequestException as error:
                    logger.exception("Emitter id fetch failed secid=%s: %s", secid, error)
                    emitter_id = None
                except Exception as error:
                    logger.exception("Unexpected emitter id error secid=%s: %s", secid, error)
                    emitter_id = None
                secid_rows.append({"SECID": secid, "EMITTER_ID": emitter_id})
                if emitter_id is not None:
                    cache.setdefault("secid_to_emitter", {})[secid] = int(emitter_id)
                pbar.update(1)

    secid_map = pd.DataFrame(secid_rows).drop_duplicates(subset=["SECID"], keep="first")
    secid_map["EMITTER_ID"] = pd.to_numeric(secid_map["EMITTER_ID"], errors="coerce")
    emitter_ids = sorted({int(x) for x in secid_map["EMITTER_ID"].dropna().tolist()})
    logger.info("Resolved emitter ids=%s", len(emitter_ids))

    cached_emitters = cache.get("emitters", {})
    emitter_rows: list[dict[str, Any]] = []
    missing_emitter_ids = []
    for emitter_id in emitter_ids:
        cached = cached_emitters.get(str(emitter_id))
        if cached:
            emitter_rows.append(cached)
        else:
            missing_emitter_ids.append(emitter_id)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(client.fetch_emitter_info, emitter_id): emitter_id for emitter_id in missing_emitter_ids}
        with progress(total=len(futures), desc="Дозагрузка эмитентов", unit="эмитент") as pbar:
            for future in as_completed(futures):
                emitter_id = futures[future]
                try:
                    emitter_info = future.result()
                    emitter_rows.append(emitter_info)
                    cache.setdefault("emitters", {})[str(emitter_id)] = emitter_info
                except requests.RequestException as error:
                    logger.exception("Emitter info failed id=%s: %s", emitter_id, error)
                    emitter_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})
                except Exception as error:
                    logger.exception("Unexpected emitter info error id=%s: %s", emitter_id, error)
                    emitter_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})
                pbar.update(1)

    emitters_df = pd.DataFrame(emitter_rows).drop_duplicates(subset=["EMITTER_ID"], keep="first")

    secid_map = secid_map.rename(columns={"EMITTER_ID": "EMITTER_ID_RESOLVED"})

    shares = shares.merge(secid_map, on="SECID", how="left")
    shares_existing = pd.to_numeric(shares["EMITTER_ID"], errors="coerce")
    shares_resolved = pd.to_numeric(shares["EMITTER_ID_RESOLVED"], errors="coerce")
    shares["EMITTER_ID"] = shares_existing.where(shares_existing.notna(), shares_resolved)
    shares = shares.drop(columns=["EMITTER_ID_RESOLVED"])

    bonds = bonds.merge(secid_map, on="SECID", how="left")
    bonds_existing = pd.to_numeric(bonds["EMITTER_ID"], errors="coerce")
    bonds_resolved = pd.to_numeric(bonds["EMITTER_ID_RESOLVED"], errors="coerce")
    bonds["EMITTER_ID"] = bonds_existing.where(bonds_existing.notna(), bonds_resolved)
    bonds = bonds.drop(columns=["EMITTER_ID_RESOLVED"])

    shares = shares.merge(emitters_df, on="EMITTER_ID", how="left")
    bonds = bonds.merge(emitters_df, on="EMITTER_ID", how="left")

    logger.info(
        "Emitter fill ratio: shares(name=%s inn=%s), bonds(name=%s inn=%s)",
        shares["EMITTER_NAME"].notna().mean(),
        shares["INN"].notna().mean(),
        bonds["EMITTER_NAME"].notna().mean(),
        bonds["INN"].notna().mean(),
    )
    return shares, bonds


def build_emitters_table(shares: pd.DataFrame, bonds: pd.DataFrame) -> pd.DataFrame:
    shares_grouped = (
        shares.dropna(subset=["EMITTER_ID"])
        .groupby("EMITTER_ID")["SECID"]
        .apply(lambda v: ", ".join(sorted(set(v))))
        .reset_index(name="TRADED_SHARES")
    )
    bonds_grouped = (
        bonds.dropna(subset=["EMITTER_ID"])
        .groupby("EMITTER_ID")["SECID"]
        .apply(lambda v: ", ".join(sorted(set(v))))
        .reset_index(name="TRADED_BONDS")
    )

    emitters = shares_grouped.merge(bonds_grouped, on="EMITTER_ID", how="outer")
    details = pd.concat([shares[["EMITTER_ID", "EMITTER_NAME", "INN"]], bonds[["EMITTER_ID", "EMITTER_NAME", "INN"]]], ignore_index=True)
    details = details.dropna(subset=["EMITTER_ID"]).drop_duplicates(subset=["EMITTER_ID"], keep="first")

    emitters = emitters.merge(details, on="EMITTER_ID", how="left")
    return emitters[["EMITTER_NAME", "INN", "TRADED_SHARES", "TRADED_BONDS", "EMITTER_ID"]].sort_values(
        by=["EMITTER_NAME", "EMITTER_ID"], na_position="last"
    )


def _fit_column_widths(worksheet: Any, df: pd.DataFrame) -> None:
    for col_idx, column_name in enumerate(df.columns, start=1):
        values = df[column_name]
        if values.empty:
            max_len = len(str(column_name))
        else:
            series_len = values.map(lambda value: len(str(value)) if pd.notna(value) else 0)
            max_len = max(len(str(column_name)), int(series_len.max()))

        adjusted_width = min(max_len + 2, 80)
        worksheet.column_dimensions[get_column_letter(col_idx)].width = max(10, adjusted_width)


def save_to_excel(df: pd.DataFrame, path: Path, logger: logging.Logger) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        worksheet = writer.sheets["Data"]

        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions

        for cell in worksheet[1]:
            cell.fill = HEADER_FILL
            cell.font = HEADER_FONT

        if worksheet.max_row >= 2:
            max_col_letter = get_column_letter(worksheet.max_column)
            zebra_range = f"A2:{max_col_letter}{worksheet.max_row}"
            zebra_rule = FormulaRule(formula=["MOD(ROW(),2)=0"], fill=ZEBRA_FILL)
            worksheet.conditional_formatting.add(zebra_range, zebra_rule)

        _fit_column_widths(worksheet, df)

    logger.info("Saved %s rows=%s", path, len(df))


def run() -> None:
    logger = setup_logging()
    logger.info("Script started")

    interrupted = {"value": False}
    stage_times: dict[str, float] = {}
    script_started_at = perf_counter()

    def handle_sigint(signum: int, frame: Any) -> None:
        _ = (signum, frame)
        interrupted["value"] = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigint)
    client = MoexClient(logger)
    cache = load_cache(logger)

    try:
        print("=====\nЭтап 1: Сбор акций")
        stage_started_at = perf_counter()
        shares = client.fetch_market_securities("shares", ["SECID", "BOARDID", "SHORTNAME", "ISIN", "LISTLEVEL", "STATUS", "EMITTER_ID"])
        shares = shares[(shares["BOARDID"] == "TQBR") & (shares["STATUS"].fillna("") != "N")].copy()
        stage_times["Этап 1: Сбор акций"] = perf_counter() - stage_started_at

        print("Этап 2: Сбор облигаций")
        stage_started_at = perf_counter()
        bonds = client.fetch_market_securities("bonds", ["SECID", "BOARDID", "SHORTNAME", "ISIN", "MATDATE", "LISTLEVEL", "STATUS", "EMITTER_ID"])
        bonds = bonds[bonds["BOARDID"].isin(["TQCB", "TQOB", "TQOD", "TQIR", "TQOE"])].copy()
        bonds = bonds[bonds["STATUS"].fillna("") != "N"].copy()
        bonds["MATDATE"] = pd.to_datetime(bonds["MATDATE"], errors="coerce").dt.date
        bonds = bonds[(bonds["MATDATE"].isna()) | (bonds["MATDATE"] >= date.today())].copy()
        stage_times["Этап 2: Сбор облигаций"] = perf_counter() - stage_started_at

        print("Этап 3: Обогащение эмитентов")
        stage_started_at = perf_counter()
        shares, bonds = enrich_emitters(client, shares, bonds, logger, cache)
        stage_times["Этап 3: Обогащение эмитентов"] = perf_counter() - stage_started_at

        print("Этап 4: Формирование Excel")
        stage_started_at = perf_counter()
        emitters = build_emitters_table(shares, bonds)

        excel_exports = [
            (shares, SHARES_FILE),
            (bonds, BONDS_FILE),
            (emitters, EMITTERS_FILE),
        ]
        with progress(total=len(excel_exports), desc="Экспорт Excel", unit="файл") as pbar:
            for df, output_path in excel_exports:
                save_to_excel(df, output_path, logger)
                pbar.update(1)

        stage_times["Этап 4: Формирование Excel"] = perf_counter() - stage_started_at

        print("=====\nГотово")
        logger.info("Script completed successfully")
    except KeyboardInterrupt:
        logger.warning("Script interrupted by Ctrl+C")
        print("\nОстановлено пользователем (Ctrl+C)")
    except Exception as error:
        logger.exception("Script failed: %s", error)
        raise
    finally:
        save_cache(cache, logger)
        logger.info("Script finished. interrupted=%s", interrupted["value"])

        total_time = perf_counter() - script_started_at
        print("=====\nSummary")
        for stage_name, duration in stage_times.items():
            print(f"{stage_name}: {duration:.2f} сек")
        print(f"Всего: {total_time:.2f} сек")
        print("=====")


if __name__ == "__main__":
    run()
