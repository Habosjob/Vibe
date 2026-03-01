from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from .db import Database


@dataclass
class WriterItem:
    table: str
    rows: list[dict]


class AsyncWriter:
    def __init__(self, db: Database, heartbeat_s: int = 7, commit_rows: int = 2000, commit_every_s: float = 2.0):
        self.db = db
        self.queue: asyncio.Queue[WriterItem | None] = asyncio.Queue()
        self.heartbeat_s = heartbeat_s
        self.commit_rows = commit_rows
        self.commit_every_s = commit_every_s
        self.total_rows = 0
        self._interval_rows = 0

    async def put(self, table: str, rows: list[dict]) -> None:
        if rows:
            await self.queue.put(WriterItem(table=table, rows=rows))

    async def run(self, logger) -> None:
        last_hb = datetime.utcnow()
        last_commit = datetime.utcnow()
        pending: dict[str, list[dict]] = defaultdict(list)
        pending_rows = 0

        def flush() -> None:
            nonlocal pending_rows, last_commit
            if pending_rows == 0:
                return
            wrote = 0
            for table, rows in list(pending.items()):
                if not rows:
                    continue
                wrote += self.db.upsert_many(table, rows, commit=False)
            self.db.commit()
            self.total_rows += wrote
            self._interval_rows += wrote
            pending.clear()
            pending_rows = 0
            last_commit = datetime.utcnow()

        while True:
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                item = None
            now = datetime.utcnow()

            if item is not None:
                if item.table == "__STOP__":
                    flush()
                    break
                pending[item.table].extend(item.rows)
                pending_rows += len(item.rows)

            if pending_rows >= self.commit_rows or (now - last_commit).total_seconds() >= self.commit_every_s:
                flush()

            if (now - last_hb).total_seconds() >= self.heartbeat_s:
                msg = f"writer wrote {self._interval_rows} rows last interval; queue size {self.queue.qsize()}"
                logger.info(msg)
                self._interval_rows = 0
                last_hb = now

    async def stop(self) -> None:
        await self.queue.put(WriterItem(table="__STOP__", rows=[]))
