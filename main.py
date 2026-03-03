from __future__ import annotations

import logging
import signal
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
PAGE_SIZE = 100


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

    def fetch_securities_reference(self) -> pd.DataFrame:
        rows: list[list[Any]] = []
        columns: list[str] = []
        start = 0

        with tqdm(desc="Справочник ISS", unit="стр") as progress:
            while True:
                data = self._get(
                    "/securities.json",
                    params={
                        "iss.meta": "off",
                        "iss.only": "securities",
                        "securities.columns": "secid,emitent_id,emitent_title,emitent_inn",
                        "start": start,
                    },
                )
                sec = data.get("securities", {})
                page = sec.get("data", [])
                if not page:
                    break
                if not columns:
                    columns = sec.get("columns", [])
                rows.extend(page)
                start += PAGE_SIZE
                progress.update(1)

        frame = pd.DataFrame(rows, columns=columns)
        self.logger.info("Reference loaded rows=%s", len(frame))
        return frame

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
    reference = client.fetch_securities_reference()
    reference = reference.rename(
        columns={
            "secid": "SECID",
            "emitent_id": "EMITTER_ID",
            "emitent_title": "EMITTER_NAME",
            "emitent_inn": "INN",
        }
    )
    reference = reference.drop_duplicates(subset=["SECID"], keep="first")

    shares = shares.merge(reference[["SECID", "EMITTER_ID", "EMITTER_NAME", "INN"]], on="SECID", how="left")
    bonds = bonds.merge(reference[["SECID", "EMITTER_ID", "EMITTER_NAME", "INN"]], on="SECID", how="left")

    combined = pd.concat([shares[["EMITTER_ID", "EMITTER_NAME", "INN"]], bonds[["EMITTER_ID", "EMITTER_NAME", "INN"]]], ignore_index=True)
    missing_ids = sorted({int(x) for x in combined[(combined["EMITTER_ID"].notna()) & ((combined["EMITTER_NAME"].isna()) | (combined["INN"].isna()))]["EMITTER_ID"].tolist()})

    filled_rows: list[dict[str, Any]] = []
    for emitter_id in tqdm(missing_ids, desc="Дозагрузка эмитентов", unit="эмитент"):
        try:
            filled_rows.append(client.fetch_emitter_info(emitter_id))
        except requests.RequestException as error:
            logger.exception("Emitter info failed id=%s: %s", emitter_id, error)
            filled_rows.append({"EMITTER_ID": emitter_id, "EMITTER_NAME": None, "INN": None})

    if filled_rows:
        fill_df = pd.DataFrame(filled_rows).drop_duplicates(subset=["EMITTER_ID"], keep="first")

        shares = shares.merge(fill_df, on="EMITTER_ID", how="left", suffixes=("", "_FILL"))
        shares["EMITTER_NAME"] = shares["EMITTER_NAME"].fillna(shares["EMITTER_NAME_FILL"])
        shares["INN"] = shares["INN"].fillna(shares["INN_FILL"])
        shares = shares.drop(columns=["EMITTER_NAME_FILL", "INN_FILL"])

        bonds = bonds.merge(fill_df, on="EMITTER_ID", how="left", suffixes=("", "_FILL"))
        bonds["EMITTER_NAME"] = bonds["EMITTER_NAME"].fillna(bonds["EMITTER_NAME_FILL"])
        bonds["INN"] = bonds["INN"].fillna(bonds["INN_FILL"])
        bonds = bonds.drop(columns=["EMITTER_NAME_FILL", "INN_FILL"])

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
