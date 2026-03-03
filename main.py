from __future__ import annotations

import logging
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm

BASE_URL = "https://iss.moex.com/iss"
OUTPUT_DIR = Path(__file__).resolve().parent
LOG_FILE = OUTPUT_DIR / "main.log"
SHARES_FILE = OUTPUT_DIR / "moex_shares.xlsx"
BONDS_FILE = OUTPUT_DIR / "moex_bonds.xlsx"
EMITTERS_FILE = OUTPUT_DIR / "moex_emitters.xlsx"
REQUEST_TIMEOUT = 30
MAX_WORKERS = 24


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

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{BASE_URL}{endpoint}"
        response = self.session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        self.logger.info("GET %s params=%s status=%s", url, params, response.status_code)
        return response.json()

    def fetch_market_securities(self, market: str, columns: list[str]) -> pd.DataFrame:
        for _ in tqdm(range(1), desc=f"MOEX {market}", unit="запрос"):
            data = self._get(
                f"/engines/stock/markets/{market}/securities.json",
                params={"iss.meta": "off", "iss.only": "securities", "securities.columns": ",".join(columns)},
            )
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


def enrich_emitters(client: MoexClient, shares: pd.DataFrame, bonds: pd.DataFrame, logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame]:
    secids = sorted(set(shares["SECID"].tolist()) | set(bonds["SECID"].tolist()))
    logger.info("Emitter enrichment start for secids=%s", len(secids))

    secid_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(client.fetch_emitter_id_by_secid, secid): secid for secid in secids}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Определение EMITTER_ID", unit="бумага"):
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

    secid_map = pd.DataFrame(secid_rows).drop_duplicates(subset=["SECID"], keep="first")
    emitter_ids = sorted({int(x) for x in secid_map["EMITTER_ID"].dropna().tolist()})
    logger.info("Resolved emitter ids=%s", len(emitter_ids))

    emitter_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(client.fetch_emitter_info, emitter_id): emitter_id for emitter_id in emitter_ids}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Дозагрузка эмитентов", unit="эмитент"):
            emitter_id = futures[future]
            try:
                emitter_rows.append(future.result())
            except requests.RequestException as error:
                logger.exception("Emitter info failed id=%s: %s", emitter_id, error)
                emitter_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})
            except Exception as error:
                logger.exception("Unexpected emitter info error id=%s: %s", emitter_id, error)
                emitter_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})

    emitters_df = pd.DataFrame(emitter_rows).drop_duplicates(subset=["EMITTER_ID"], keep="first")

    shares = shares.merge(secid_map, on="SECID", how="left").merge(emitters_df, on="EMITTER_ID", how="left")
    bonds = bonds.merge(secid_map, on="SECID", how="left").merge(emitters_df, on="EMITTER_ID", how="left")

    logger.info(
        "Emitter fill ratio: shares(name=%s inn=%s), bonds(name=%s inn=%s)",
        shares["EMITTER_NAME"].notna().mean(),
        shares["INN"].notna().mean(),
        bonds["EMITTER_NAME"].notna().mean(),
        bonds["INN"].notna().mean(),
    )
    return shares, bonds


def build_emitters_table(shares: pd.DataFrame, bonds: pd.DataFrame) -> pd.DataFrame:
    shares_grouped = shares.dropna(subset=["EMITTER_ID"]).groupby("EMITTER_ID")["SECID"].apply(lambda v: ", ".join(sorted(set(v)))).reset_index(name="TRADED_SHARES")
    bonds_grouped = bonds.dropna(subset=["EMITTER_ID"]).groupby("EMITTER_ID")["SECID"].apply(lambda v: ", ".join(sorted(set(v)))).reset_index(name="TRADED_BONDS")

    emitters = shares_grouped.merge(bonds_grouped, on="EMITTER_ID", how="outer")
    details = pd.concat([shares[["EMITTER_ID", "EMITTER_NAME", "INN"]], bonds[["EMITTER_ID", "EMITTER_NAME", "INN"]]], ignore_index=True)
    details = details.dropna(subset=["EMITTER_ID"]).drop_duplicates(subset=["EMITTER_ID"], keep="first")

    emitters = emitters.merge(details, on="EMITTER_ID", how="left")
    return emitters[["EMITTER_NAME", "INN", "TRADED_SHARES", "TRADED_BONDS", "EMITTER_ID"]].sort_values(by=["EMITTER_NAME", "EMITTER_ID"], na_position="last")


def save_to_excel(df: pd.DataFrame, path: Path, logger: logging.Logger) -> None:
    df.to_excel(path, index=False)
    logger.info("Saved %s rows=%s", path, len(df))


def run() -> None:
    logger = setup_logging()
    logger.info("Script started")

    interrupted = {"value": False}

    def handle_sigint(signum: int, frame: Any) -> None:
        _ = (signum, frame)
        interrupted["value"] = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigint)
    client = MoexClient(logger)

    try:
        print("=====\nЭтап 1: Сбор акций")
        shares = client.fetch_market_securities("shares", ["SECID", "BOARDID", "SHORTNAME", "ISIN", "LISTLEVEL", "STATUS"])
        shares = shares[(shares["BOARDID"] == "TQBR") & (shares["STATUS"].fillna("") != "N")].copy()

        print("Этап 2: Сбор облигаций")
        bonds = client.fetch_market_securities("bonds", ["SECID", "BOARDID", "SHORTNAME", "ISIN", "MATDATE", "LISTLEVEL", "STATUS"])
        bonds = bonds[bonds["BOARDID"].isin(["TQCB", "TQOB", "TQOD", "TQIR", "TQOE"])].copy()
        bonds = bonds[bonds["STATUS"].fillna("") != "N"].copy()
        bonds["MATDATE"] = pd.to_datetime(bonds["MATDATE"], errors="coerce").dt.date
        bonds = bonds[(bonds["MATDATE"].isna()) | (bonds["MATDATE"] >= date.today())].copy()

        print("Этап 3: Обогащение эмитентов")
        shares, bonds = enrich_emitters(client, shares, bonds, logger)

        print("Этап 4: Формирование Excel")
        emitters = build_emitters_table(shares, bonds)

        save_to_excel(shares, SHARES_FILE, logger)
        save_to_excel(bonds, BONDS_FILE, logger)
        save_to_excel(emitters, EMITTERS_FILE, logger)

        print("=====\nГотово")
        logger.info("Script completed successfully")
    except KeyboardInterrupt:
        logger.warning("Script interrupted by Ctrl+C")
        print("\nОстановлено пользователем (Ctrl+C)")
    except Exception as error:
        logger.exception("Script failed: %s", error)
        raise
    finally:
        logger.info("Script finished. interrupted=%s", interrupted["value"])


if __name__ == "__main__":
    run()
