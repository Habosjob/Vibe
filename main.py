from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

BASE_URL = "https://iss.moex.com/iss"
OUTPUT_DIR = Path(__file__).resolve().parent
SHARES_FILE = OUTPUT_DIR / "moex_shares.xlsx"
BONDS_FILE = OUTPUT_DIR / "moex_bonds.xlsx"
EMITTERS_FILE = OUTPUT_DIR / "moex_emitters.xlsx"
REQUEST_TIMEOUT = 30


class MoexClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Vibe-MOEX-Collector/1.0"})

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        response = self.session.get(f"{BASE_URL}{endpoint}", params=params or {}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def fetch_board_data(self, market: str, board: str, columns: list[str]) -> pd.DataFrame:
        for _ in tqdm(range(1), desc=f"MOEX {market}:{board}", unit="запрос"):
            data = self._get(
                f"/engines/stock/markets/{market}/boards/{board}/securities.json",
                params={
                    "iss.meta": "off",
                    "iss.only": "securities,marketdata",
                    "securities.columns": ",".join(columns),
                    "marketdata.columns": "SECID,TRADINGSTATUS",
                },
            )

        sec = pd.DataFrame(data.get("securities", {}).get("data", []), columns=data.get("securities", {}).get("columns", []))
        mkt = pd.DataFrame(data.get("marketdata", {}).get("data", []), columns=data.get("marketdata", {}).get("columns", []))
        if sec.empty:
            return sec
        return sec.merge(mkt, on="SECID", how="left")

    def fetch_security_emitent_info(self, secid: str) -> dict:
        data = self._get(
            f"/securities/{secid}.json",
            params={
                "iss.meta": "off",
                "iss.only": "description",
                "description.columns": "name,value",
            },
        )
        description = data.get("description", {}).get("data", [])
        mapping = {row[0]: row[1] for row in description if len(row) == 2}
        return {
            "SECID": secid,
            "EMITTER_ID": mapping.get("EMITTER_ID") or mapping.get("EMITENT_ID"),
            "EMITTER_NAME": mapping.get("EMITTER_FULL_NAME") or mapping.get("EMITENT_TITLE"),
            "INN": mapping.get("INN") or mapping.get("EMITENT_INN"),
        }


def enrich_with_emitents(client: MoexClient, shares: pd.DataFrame, bonds: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    secids = sorted(set(shares["SECID"].tolist()) | set(bonds["SECID"].tolist()))
    emitter_rows = []

    for secid in tqdm(secids, desc="Загрузка эмитентов по бумагам", unit="бумага"):
        try:
            emitter_rows.append(client.fetch_security_emitent_info(secid))
        except requests.RequestException:
            emitter_rows.append({"SECID": secid, "EMITTER_ID": None, "EMITTER_NAME": None, "INN": None})

    emitter_df = pd.DataFrame(emitter_rows)
    for df in (shares, bonds):
        df.drop(columns=[col for col in ["EMITTER_ID", "EMITTER_NAME", "INN"] if col in df.columns], inplace=True, errors="ignore")

    shares = shares.merge(emitter_df, on="SECID", how="left")
    bonds = bonds.merge(emitter_df, on="SECID", how="left")
    return shares, bonds


def build_emitters_file(shares: pd.DataFrame, bonds: pd.DataFrame) -> pd.DataFrame:
    shares_grouped = shares.groupby("EMITTER_ID")["SECID"].apply(lambda x: ", ".join(sorted(set(x)))).reset_index(name="TRADED_SHARES")
    bonds_grouped = bonds.groupby("EMITTER_ID")["SECID"].apply(lambda x: ", ".join(sorted(set(x)))).reset_index(name="TRADED_BONDS")

    emitters = pd.merge(shares_grouped, bonds_grouped, on="EMITTER_ID", how="outer")
    details = pd.concat([
        shares[["EMITTER_ID", "EMITTER_NAME", "INN"]],
        bonds[["EMITTER_ID", "EMITTER_NAME", "INN"]],
    ]).dropna(subset=["EMITTER_ID"]).drop_duplicates(subset=["EMITTER_ID"]) 

    emitters = emitters.merge(details, on="EMITTER_ID", how="left")
    return emitters[["EMITTER_NAME", "INN", "TRADED_SHARES", "TRADED_BONDS", "EMITTER_ID"]].sort_values(
        by=["EMITTER_NAME", "EMITTER_ID"], na_position="last"
    ).reset_index(drop=True)


def save_to_excel(df: pd.DataFrame, path: Path) -> None:
    df.to_excel(path, index=False)


def main() -> None:
    client = MoexClient()

    for _ in tqdm(range(1), desc="Сбор акций", unit="этап"):
        shares = client.fetch_board_data("shares", "TQBR", ["SECID", "SHORTNAME", "ISIN", "LISTLEVEL"])
        shares = shares[shares["TRADINGSTATUS"].fillna("") != "N"].copy()

    for _ in tqdm(range(1), desc="Сбор облигаций", unit="этап"):
        bonds_tqcb = client.fetch_board_data("bonds", "TQCB", ["SECID", "SHORTNAME", "ISIN", "MATDATE", "LISTLEVEL"])
        bonds_tqob = client.fetch_board_data("bonds", "TQOB", ["SECID", "SHORTNAME", "ISIN", "MATDATE", "LISTLEVEL"])
        bonds_tqcb["BOARDID"] = "TQCB"
        bonds_tqob["BOARDID"] = "TQOB"
        bonds = pd.concat([bonds_tqcb, bonds_tqob], ignore_index=True).drop_duplicates(subset=["SECID", "BOARDID"])
        bonds["MATDATE"] = pd.to_datetime(bonds["MATDATE"], errors="coerce").dt.date
        bonds = bonds[(bonds["MATDATE"].isna()) | (bonds["MATDATE"] >= date.today())]
        bonds = bonds[bonds["TRADINGSTATUS"].fillna("") != "N"].copy()

    for _ in tqdm(range(1), desc="Обогащение эмитентов", unit="этап"):
        shares, bonds = enrich_with_emitents(client, shares, bonds)

    emitters = build_emitters_file(shares, bonds)

    save_to_excel(shares, SHARES_FILE)
    save_to_excel(bonds, BONDS_FILE)
    save_to_excel(emitters, EMITTERS_FILE)

    print(f"Готово. Файлы сохранены:\n- {SHARES_FILE}\n- {BONDS_FILE}\n- {EMITTERS_FILE}")


if __name__ == "__main__":
    main()
