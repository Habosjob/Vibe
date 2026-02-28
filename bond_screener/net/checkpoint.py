from __future__ import annotations

from datetime import datetime, timezone

from core.db import get_connection
from core.settings import AppSettings


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class CheckpointStore:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings

    def start_job(self, job_name: str, items: list[str]) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            for item in items:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO job_items (job_name, item_key, status, error_text, updated_at)
                    VALUES (?, ?, ?, NULL, ?)
                    """,
                    (job_name, item, "pending", _utc_now_iso()),
                )

    def mark_done(self, job_name: str, item: str) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            conn.execute(
                """
                UPDATE job_items
                SET status = ?, error_text = NULL, updated_at = ?
                WHERE job_name = ? AND item_key = ?
                """,
                ("done", _utc_now_iso(), job_name, item),
            )

    def mark_failed(self, job_name: str, item: str, error: str) -> None:
        with get_connection(self.settings.paths.db_file) as conn:
            conn.execute(
                """
                UPDATE job_items
                SET status = ?, error_text = ?, updated_at = ?
                WHERE job_name = ? AND item_key = ?
                """,
                ("failed", error[:2000], _utc_now_iso(), job_name, item),
            )

    def resume_pending(self, job_name: str) -> list[str]:
        with get_connection(self.settings.paths.db_file) as conn:
            rows = conn.execute(
                """
                SELECT item_key
                FROM job_items
                WHERE job_name = ? AND status IN ('pending', 'failed')
                ORDER BY item_key
                """,
                (job_name,),
            ).fetchall()
        return [row["item_key"] for row in rows]

    def clear_all(self) -> int:
        with get_connection(self.settings.paths.db_file) as conn:
            before = conn.execute("SELECT COUNT(*) AS cnt FROM job_items").fetchone()["cnt"]
            conn.execute("DELETE FROM job_items")
        return int(before)
