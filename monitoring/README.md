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
4. сравнение snapshot рейтингов из отдельной БД рейтинговых агентств `DB/raitings.sqlite3` (`Изменен рейтинг/прогноз/отозван`);
5. загрузчик портфеля только из `monitoring/Portfolio.xlsx` (ручные листы `Акции`/`Облигации` не перезаписываются);
6. сбор новостей Smartlab в 2 стратегии (ticker → fallback tag);
7. экспорт `Reports_monitoring.xlsx` и `Portfolio.xlsx`.

## Настройка
Все настройки находятся в `monitoring/config.py` и подробно прокомментированы:
- пути,
- timeout/retry/backoff (включая лимит max backoff и поддержку Retry-After для 429/503),
- ttl кэшей,
- параметры параллельности e-disclosure с autotune (`EDISCLOSURE_FETCH_WORKERS_MIN/DEFAULT/MAX`, `EDISCLOSURE_FILES_SEMAPHORE_MIN/DEFAULT/MAX`) и агрессивным cold-start (`EDISCLOSURE_AUTOTUNE_COLD_START_MAX`),
- retry-политика e-disclosure (retry только на 429/5xx/timeout/connection reset; fast/retry jitter),
- scheduler и full-scan параметры e-disclosure (`EDISCLOSURE_FORCE_FULL_SCAN`, интервалы recheck),
- оформление Excel.
- источник stage рейтингов: внешняя БД `RATINGS_DB_FILE` (по умолчанию `../DB/raitings.sqlite3`) и таблицы из `RATINGS_SOURCE_TABLES`;
- флаг `RATINGS_MONITORING_ENABLED` для включения/отключения формирования рейтинговых событий в monitoring;


## Оптимизация скорости (этап 2: Сбор отчетности)
- Увеличены базовые лимиты параллельности: workers до диапазона `16..64` (дефолт `32`) и semaphore для `files.aspx` до `8..24` (дефолт `14`).
- Для первого запуска (когда в `meta` еще нет autotune-значений) добавлен `cold-start` режим: старт сразу с максимальных значений, чтобы не разгоняться несколько прогонов.
- Обновлен autotune: 
  - быстрое масштабирование вверх при низкой доле 429/timeout;
  - более агрессивное снижение при перегрузе;
  - сохранение выбранных значений в SQLite `meta` для следующих запусков.

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
- Проверка новизны событий отчетности выполняется batch-запросом только по хэшам текущего прогона вместо полного чтения historical hash-set из БД, что ускоряет flush на больших объемах.
- Повторные прогоны активно переиспользуют `company_map` и кэши страниц/событий.

## Изменения в Portfolio.xlsx
- В листы `Portfolio_All` и `Portfolio_UniqueEmitents` добавлена колонка `Источник события`.
- Теперь в портфеле сохраняется источник последнего события отчетности (`e-disclosure`, `NRA`, `ACRA`, `NKR`, `RAEX`), а не только детали новости Smartlab.


## Portfolio.xlsx (один файл)
- Скрипт работает только с `monitoring/Portfolio.xlsx`.
- Если файла нет, он создается с ручными листами `Акции` и `Облигации` и заголовками.
- Ручные листы никогда не очищаются и не перезаписываются кодом.
- Автоматически пересобираются только листы `Portfolio_All`, `Portfolio_UniqueEmitents`, `News`.
- Лист `News` теперь содержит объединенную ленту: Smartlab + monitoring-события отчетности/рейтингов (`e-disclosure`, `NRA`, `ACRA`, `NKR`, `RAEX`).

## Stage «События по рейтингам»
- Stage формирует monitoring-снимок по рейтингам из отдельной БД рейтинговых агентств (`RATINGS_DB_FILE`).
- Для каждого агентства берется последняя запись по ИНН и дате присвоения (`assigned_date/rating_date`).
- В `Reports_monitoring.xlsx` попадают события `Изменен рейтинг`, `Изменен прогноз`, `Рейтинг отозван / снят` по источникам `NRA/ACRA/NKR/RAEX`.
- Для сравнения между прогонами используется таблица `ratings_monitoring_snapshot` в `monitoring.sqlite3`.
