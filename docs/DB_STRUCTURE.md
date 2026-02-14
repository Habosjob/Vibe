# Структура SQLite (MOEX API)

## Основные таблицы

### `bonds_cache`
Кэш дневного CSV-среза из MOEX.
- `fetch_date` — дата выгрузки (PK, `YYYY-MM-DD`)
- `csv_data` — сырой CSV-текст
- `created_at` — время записи в кэш

### `details_cache`
Кэш JSON-ответов по endpoint/бумаге.
- `endpoint` — имя endpoint
- `secid` — идентификатор бумаги
- `response_json` — JSON-ответ целиком
- `fetched_at` — время загрузки
- PK: (`endpoint`, `secid`)

### `details_rows`
Нормализованное построчное представление payload.
- `fetched_at` — время загрузки
- `endpoint` — имя endpoint
- `secid` — идентификатор бумаги
- `block_name` — имя блока данных в payload
- `row_json` — строка блока в JSON
- Индекс: `idx_details_rows_endpoint_secid_block(endpoint, secid, block_name)`

### `endpoint_health_history`
История доступности и качества endpoint.
- `checked_at` — время проверки
- `endpoint` — имя endpoint
- `secid` — бумага, по которой шла проверка
- `status` — `ok` / `empty` / `error`
- `source` — `network` или `cache`
- `http_status` — HTTP-код
- `latency_ms` — latency запроса
- `blocks` — список блоков в ответе
- `error_text` — текст ошибки
- Индекс: `idx_endpoint_health_history_checked_endpoint(checked_at, endpoint)`

### `endpoint_circuit_breaker`
Состояние circuit-breaker по endpoint.
- `endpoint` — endpoint (PK)
- `failure_count` — число подряд ошибок
- `state` — `closed` / `half_open` / `open`
- `opened_at` — когда breaker открылся
- `updated_at` — последнее обновление

### `endpoint_health_mv`
Материализованные метрики стабильности endpoint по окнам `1d` и `7d`.
- `window`, `endpoint` — составной PK
- `total_requests` — всего запросов
- `error_requests` — число ошибок
- `error_rate` — доля ошибок
- `avg_latency_ms` — средняя latency
- `p95_latency_ms` — p95 latency
- `updated_at` — время пересчёта

### `dq_run_history`
История lightweight data-quality проверок.
- `run_id` — ID прогона (PK)
- `run_at` — время проверки
- `source` — источник данных (`network`, `cache`, `cache_fallback:*`)
- `row_count` — число строк в базовом датасете
- `empty_secid_ratio` — доля пустого `SECID`
- `empty_isin_ratio` — доля пустого `ISIN`
- `row_count_delta_ratio` — относительное отклонение от предыдущего прогона
- `notes` — предупреждения качества

### `dq_metrics_daily_mv`
Материализованный дневной агрегат DQ-метрик (для быстрых дашбордов/алертов).
- `run_day` — день (`YYYY-MM-DD`, PK)
- `runs_count` — число прогонов за день
- `avg_row_count` / `max_row_count` / `min_row_count` — агрегаты по `row_count`
- `avg_empty_secid_ratio` / `avg_empty_isin_ratio` — средние DQ-доли
- `max_row_count_delta_ratio` — максимальное отклонение за день
- `warning_runs_count` — число прогонов с warning-нотой
- `updated_at` — время пересчёта MV

### `intraday_quotes_snapshot`
Снапшоты внутридневных котировок (быстрый сбор по всем бумагам).
- `snapshot_at` — время снапшота
- `secid` — бумага
- `boardid` — торговый режим
- `tradingstatus` — статус торгов
- `last`, `numtrades`, `volvalue`, `updatetime` — поля marketdata
- PK: (`snapshot_at`, `secid`, `boardid`)

### `bonds_enriched`
Актуальный full snapshot финальной таблицы (перезаписывается на каждом прогоне).

### `bonds_enriched_incremental`
Инкрементальная история batch-экспортов finish.
- `batch_id` — ID батча
- `export_date` — дата бизнес-выгрузки
- `exported_at` — фактическое время экспорта
- `source` — источник данных
- `row_json` — строка `bonds_enriched` в JSON
- Индекс: `idx_bonds_enriched_incremental_export_date_batch(export_date, batch_id)`

## Экспортные файлы
- `Moex_Bonds.xlsx` — базовый CSV-экспорт (только в режиме `--debug`).
- `Moex_Bonds_Details.xlsx` — endpoint/block-листы + `details_transposed` (только в `--debug`).
- `Moex_Bonds_Finish.xlsx` — последний финальный срез (всегда генерируется).
- `finish_batches/Moex_Bonds_Finish_<date>_<batch>.xlsx` — инкрементальные batch-экспорты.
