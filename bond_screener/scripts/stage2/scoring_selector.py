from __future__ import annotations

import time
from dataclasses import dataclass

import pandas as pd

from core.db import get_connection, utc_now_iso
from core.excel_debug import export_dataframe, should_export
from core.logging import get_script_logger
from core.progress import progress_iter
from core.settings import AppSettings


@dataclass(frozen=True)
class ScoringSelectorStats:
    greenlist_emitents: int
    candidates_before_drop: int
    duration_s: float


class ScoringSelector:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.logger = get_script_logger(
            settings.paths.logs_dir / "stage2_scoring_selector.log",
            "stage2.scoring_selector",
        )

    def run(self) -> ScoringSelectorStats:
        started = time.perf_counter()
        self._ensure_tables()

        with get_connection(self.settings.paths.db_file) as conn:
            greenlist_emitents = [
                row["issuer_key"]
                for row in conn.execute(
                    """
                    SELECT issuer_key
                    FROM emitents_effective
                    WHERE scoring_flag = 'Greenlist'
                    ORDER BY issuer_key
                    """
                ).fetchall()
            ]

            candidates = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT s.isin, s.secid, s.issuer_key
                    FROM securities_raw s
                    JOIN emitents_effective e ON e.issuer_key = s.issuer_key
                    WHERE e.scoring_flag = 'Greenlist'
                    GROUP BY s.secid, s.isin, s.issuer_key
                    ORDER BY s.issuer_key, s.secid
                    """
                ).fetchall()
            ]

            now_iso = utc_now_iso()
            conn.execute("DELETE FROM candidate_bonds")
            for row in progress_iter(candidates, desc="Stage2/ScoringSelector", total=len(candidates)):
                conn.execute(
                    """
                    INSERT INTO candidate_bonds (isin, secid, issuer_key, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (row.get("isin"), row["secid"], row["issuer_key"], now_iso, now_iso),
                )

        self._export_debug_if_needed()

        duration_s = time.perf_counter() - started
        self.logger.info(
            "ScoringSelector завершён: greenlist_emitents=%s, candidates_before_drop=%s, duration=%.2fs",
            len(greenlist_emitents),
            len(candidates),
            duration_s,
        )
        print(
            "[STAGE2][scoring_selector] "
            f"greenlist_emitents={len(greenlist_emitents)} | candidates_before_drop={len(candidates)}"
        )
        return ScoringSelectorStats(
            greenlist_emitents=len(greenlist_emitents),
            candidates_before_drop=len(candidates),
            duration_s=duration_s,
        )

    def _ensure_tables(self) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS candidate_bonds (
                    isin TEXT,
                    secid TEXT NOT NULL PRIMARY KEY,
                    issuer_key TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def _export_debug_if_needed(self) -> None:
        if not should_export(self.settings, "stage2"):
            return
        with get_connection(self.settings.paths.db_file) as conn:
            rows = [dict(r) for r in conn.execute("SELECT * FROM candidate_bonds ORDER BY issuer_key, secid").fetchall()]
        out = export_dataframe(
            self.settings,
            filename="stage2_debug_candidate_bonds.xlsx",
            df=pd.DataFrame(rows),
            export_name="stage2",
        )
        if out:
            self.logger.info("Excel debug выгрузка создана: %s", out)
