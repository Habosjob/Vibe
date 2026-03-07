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
3. ускоренный web-flow клиент e-disclosure (scheduled incremental monitoring, event-gate перед `files.aspx`, fixed worker pool, простой semaphore только на `files.aspx`, без загрузки `FileLoad.ashx`, preview top-rows + fingerprint, batch DB flush, ttl cache + safe fallback);
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
- scheduler и full-scan параметры e-disclosure (`EDISCLOSURE_FORCE_FULL_SCAN`, интервалы recheck, max emitents per run),
- порог stale alert,
- оформление Excel.

## Вывод в консоль
Скрипт выводит только этапы, прогресс-бары `tqdm` и финальный Summary по времени этапов.
Техническая диагностика пишется в `monitoring/logs/monitoring.log` (перезаписываемый файл).


## Новые таблицы SQLite
- `emitent_schedule`: расписание due-проверок эмитентов (next_check_at, stable_run_count, режим последнего запуска).
- `report_state`: расширен полями `top_row_hash` и `page_checked_at` для preview/fingerprint skip.

## Ключевые изменения stage_reports
- Scheduler отбирает только due эмитентов (`skipped_not_due` без сети).
- Для due эмитентов сначала делается events gate (`/api/events/page`), и только при признаке изменений запускается deep scan `files.aspx`.
- `files.aspx` парсится как HTML-индекс; `FileLoad.ashx` НЕ скачивается, URL просто сохраняется в событие.
- Повторные прогоны активно переиспользуют `company_map` и кэши страниц/событий.
