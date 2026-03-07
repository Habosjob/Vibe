# Monitoring (автономный монолит)

## Что это
`monitoring/main.py` — единый автономный скрипт мониторинга (монолит), который не импортирует и не меняет корневой `main.py`.

Контур использует только read-only входы из корня проекта:
- `Emitents.xlsx`
- `monitoring/Portfolio.xlsx`

И создает свои артефакты внутри `monitoring/`:
- `DB/monitoring.sqlite3`
- `logs/monitoring.log`
- `Reports_monitoring.xlsx`
- `Portfolio.xlsx`
- `BaseSnapshots/emitents_snapshot.xlsx`
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
3. ускоренный web-flow клиент e-disclosure (full-universe run без cap по эмитентам, event-gate перед `files.aspx`, fixed worker pool + autotune workers/files semaphore, простой semaphore только на `files.aspx`, без загрузки `FileLoad.ashx`, fingerprint верхней строки, batch DB flush, ttl cache + safe fallback);
4. сравнение snapshot рейтингов (`Изменен рейтинг/прогноз/отозван`);
5. загрузчик портфеля только из `monitoring/Portfolio.xlsx` (ручные листы `Акции`/`Облигации` не перезаписываются);
6. сбор новостей Smartlab в 2 стратегии (ticker → fallback tag);
7. экспорт `Reports_monitoring.xlsx` и `Portfolio.xlsx`.

## Настройка
Все настройки находятся в `monitoring/config.py` и подробно прокомментированы:
- пути,
- timeout/retry/backoff (включая лимит max backoff и поддержку Retry-After для 429/503),
- ttl кэшей,
- параметры параллельности e-disclosure с autotune (`EDISCLOSURE_FETCH_WORKERS_MIN/DEFAULT/MAX`, `EDISCLOSURE_FILES_SEMAPHORE_MIN/DEFAULT/MAX`),
- retry-политика e-disclosure (retry только на 429/5xx/timeout/connection reset; fast/retry jitter),
- scheduler и full-scan параметры e-disclosure (`EDISCLOSURE_FORCE_FULL_SCAN`, интервалы recheck),
- порог stale alert,
- оформление Excel.

## Вывод в консоль
Скрипт выводит только этапы, прогресс-бары `tqdm` и финальный Summary по времени этапов.
Техническая диагностика пишется в `monitoring/logs/monitoring.log` (перезаписываемый файл).


## Новые таблицы SQLite
- `emitent_schedule`: расписание due-проверок эмитентов (next_check_at, stable_run_count, режим последнего запуска).
- `report_state`: расширен полями `top_row_hash` и `page_checked_at` для preview/fingerprint skip.

## Ключевые изменения stage_reports
- Каждый run логически обрабатывает весь universe эмитентов (без `max emitents per run` cap).
- Для эмитентов с валидным state сначала делается events gate (`/api/events/page`), и только при признаке изменений запускается deep scan `files.aspx`.
- `files.aspx` парсится как HTML-индекс; `FileLoad.ashx` НЕ скачивается, URL просто сохраняется в событие.
- На тип отчета выполняется один парс страницы: извлекаются новые строки и одновременно сохраняется fingerprint (`top_row_hash`) для быстрых повторных прогонов без двойного parse-pass.
- Повторные прогоны активно переиспользуют `company_map` и кэши страниц/событий.


## Portfolio.xlsx (один файл)
- Скрипт работает только с `monitoring/Portfolio.xlsx`.
- Если файла нет, он создается с ручными листами `Акции` и `Облигации` и заголовками.
- Ручные листы никогда не очищаются и не перезаписываются кодом.
- Автоматически пересобираются только листы `Portfolio_All`, `Portfolio_UniqueEmitents`, `News`.
