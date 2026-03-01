from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime

from .db import Database


@dataclass
class WriterItem:
    table: str
    rows: list[dict]


class AsyncWriter:
    def __init__(self, db: Database, heartbeat_s: int = 7):
        self.db = db
        self.queue: asyncio.Queue[WriterItem | None] = asyncio.Queue()
        self.heartbeat_s = heartbeat_s
        self.total_rows = 0
        self._interval_rows = 0

    async def put(self, table: str, rows: list[dict]) -> None:
        if rows:
            await self.queue.put(WriterItem(table=table, rows=rows))

    async def run(self, logger) -> None:
        last_hb = datetime.utcnow()
        while True:
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                item = None
            now = datetime.utcnow()
            if (now - last_hb).total_seconds() >= self.heartbeat_s:
                logger.info("writer wrote %s rows last interval; queue size %s", self._interval_rows, self.queue.qsize())
                self._interval_rows = 0
                last_hb = now
            if item is None:
                continue
            if item.table == "__STOP__":
                break
            wrote = self.db.upsert_many(item.table, item.rows)
            self.total_rows += wrote
            self._interval_rows += wrote

    async def stop(self) -> None:
        await self.queue.put(WriterItem(table="__STOP__", rows=[]))
