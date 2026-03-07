# Monitoring (автономный монолит)

## Что это
`monitoring/main.py` — единый автономный скрипт мониторинга (монолит), который не импортирует и не меняет корневой `main.py`.

Контур использует только read-only входы из корня проекта:
- `Emitents.xlsx`
- пользовательский файл портфеля (если найден)

И создает свои артефакты внутри `monitoring/`:
- `DB/monitoring.sqlite3`
- `logs/monitoring.log`
- `Reports_monitoring.xlsx`
- `Portfolio.xlsx`
- `BaseSnapshots/emitents_snapshot.xlsx`
- `BaseSnapshots/portfolio_snapshot.xlsx`
- `cache/` и `raw/`

## Запуск
Windows / Linux одинаково:
```bash
python monitoring/main.py
```

## Как реализовано
Монолит содержит в одном файле:
1. bootstrap директорий и логгера;
2. bootstrap SQLite и операции идемпотентной записи;
3. ускоренный web-flow клиент e-disclosure (fixed worker pool, простой semaphore только на `files.aspx`, без global/thread warmup, fast incremental/full sync режимы, preview top-rows, hot path по known company_id, batch DB flush, ttl cache + safe fallback);
4. сравнение snapshot рейтингов (`Изменен рейтинг/прогноз/отозван`);
5. загрузчик портфеля (поиск по маскам + устойчивый парсинг листов);
6. сбор новостей Smartlab в 2 стратегии (ticker → fallback tag);
7. экспорт `Reports_monitoring.xlsx` и `Portfolio.xlsx`.

## Настройка
Все настройки находятся в `monitoring/config.py` и подробно прокомментированы:
- пути,
- timeout/retry/backoff (включая лимит max backoff и поддержку Retry-After для 429/503),
- ttl кэшей,
- параметры фиксированной параллельности e-disclosure (`EDISCLOSURE_FETCH_WORKERS`, `EDISCLOSURE_FILES_SEMAPHORE`),
- retry-политика e-disclosure (retry только на 429/5xx/timeout/connection reset; fast/retry jitter),
- warmup-флаги e-disclosure (по умолчанию выключены и не участвуют в hot path),
- режимы обхода e-disclosure (`incremental` / `full_sync`, периодичность полного скана),
- порог stale alert,
- оформление Excel.

## Вывод в консоль
Скрипт выводит только этапы, прогресс-бары `tqdm` и финальный Summary по времени этапов.
Техническая диагностика пишется в `monitoring/logs/monitoring.log` (перезаписываемый файл).
