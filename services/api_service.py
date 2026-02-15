from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_DB_PATH = BASE_DIR / "DB" / "moex_cache.sqlite3"
DETAILS_PARQUET_DIR = BASE_DIR / "details_parquet"

app = FastAPI(title="MOEX Bonds API", version="1.0.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/bonds")
def get_bonds(limit: int = Query(default=200, ge=1, le=5000), secid: str | None = None) -> list[dict]:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        query = "SELECT * FROM bonds_read_model"
        params: list[str | int] = []
        if secid:
            query += " WHERE SECID = ?"
            params.append(secid)
        query += " LIMIT ?"
        params.append(limit)
        try:
            df = pd.read_sql_query(query, connection, params=params)
        except Exception:  # noqa: BLE001
            query = "SELECT * FROM bonds_enriched"
            if secid:
                query += " WHERE SECID = ?"
                params = [secid, limit]
            else:
                params = [limit]
            query += " LIMIT ?"
            try:
                df = pd.read_sql_query(query, connection, params=params)
            except Exception as error:  # noqa: BLE001
                raise HTTPException(status_code=500, detail=str(error)) from error
    return df.fillna("").to_dict(orient="records")


@app.get("/batches")
def get_batches(export_date: str | None = None, source: str | None = None, limit: int = Query(default=200, ge=1, le=5000)) -> list[dict]:
    with sqlite3.connect(CACHE_DB_PATH) as connection:
        query = "SELECT batch_id, export_date, exported_at, source, row_json FROM bonds_enriched_incremental WHERE 1=1"
        params: list[str | int] = []
        if export_date:
            query += " AND export_date = ?"
            params.append(export_date)
        if source:
            query += " AND source = ?"
            params.append(source)
        query += " ORDER BY exported_at DESC LIMIT ?"
        params.append(limit)
        rows = connection.execute(query, params).fetchall()
    return [
        {
            "batch_id": row[0],
            "export_date": row[1],
            "exported_at": row[2],
            "source": row[3],
            "row": json.loads(row[4]),
        }
        for row in rows
    ]


@app.get("/details/{endpoint}")
def get_details(endpoint: str, secid: str | None = None, limit: int = Query(default=500, ge=1, le=10000)) -> list[dict]:
    parquet_path = DETAILS_PARQUET_DIR / f"{endpoint}.parquet"
    if not parquet_path.exists():
        raise HTTPException(status_code=404, detail=f"Unknown endpoint parquet: {endpoint}")

    df = pd.read_parquet(parquet_path)
    if secid and "secid" in df.columns:
        df = df[df["secid"] == secid]
    return df.head(limit).fillna("").to_dict(orient="records")
