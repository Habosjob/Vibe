from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


class DetailsRepository:
    """SQL слой для массового чтения/записи details-cache."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def load_cached_records_bulk(
        self,
        secids: list[str],
        endpoints: list[str],
        details_ttl_hours: int,
    ) -> tuple[dict[tuple[str, str], tuple[dict, str]], dict[tuple[str, str], tuple[dict, str]]]:
        if not secids or not endpoints:
            return {}, {}

        secid_placeholders = ",".join(["?"] * len(secids))
        endpoint_placeholders = ",".join(["?"] * len(endpoints))
        cutoff = (datetime.now() - timedelta(hours=details_ttl_hours)).isoformat(timespec="seconds")

        fresh_query = f"""
            SELECT endpoint, secid, response_json, fetched_at
            FROM details_cache
            WHERE fetched_at >= ?
              AND endpoint IN ({endpoint_placeholders})
              AND secid IN ({secid_placeholders})
        """

        latest_query = f"""
            SELECT endpoint, secid, response_json, fetched_at
            FROM (
                SELECT
                    endpoint,
                    secid,
                    response_json,
                    fetched_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY endpoint, secid
                        ORDER BY fetched_at DESC
                    ) AS rn
                FROM details_cache
                WHERE endpoint IN ({endpoint_placeholders})
                  AND secid IN ({secid_placeholders})
            )
            WHERE rn = 1
        """

        fresh_params = [cutoff, *endpoints, *secids]
        latest_params = [*endpoints, *secids]

        fresh_records: dict[tuple[str, str], tuple[dict, str]] = {}
        latest_records: dict[tuple[str, str], tuple[dict, str]] = {}
        with sqlite3.connect(self.db_path) as connection:
            for endpoint, secid, payload_raw, fetched_at in connection.execute(fresh_query, fresh_params).fetchall():
                fresh_records[(endpoint, secid)] = (json.loads(payload_raw), fetched_at)

            for endpoint, secid, payload_raw, fetched_at in connection.execute(latest_query, latest_params).fetchall():
                latest_records[(endpoint, secid)] = (json.loads(payload_raw), fetched_at)

        return fresh_records, latest_records
