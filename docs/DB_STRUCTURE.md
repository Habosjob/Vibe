# Структура SQLite (MOEX API)

## Основные таблицы

### `bonds_cache`
Кэш дневного CSV-среза из MOEX (`iss/apps/infogrid/stock/rates.csv`).
- `fetch_date` — дата выгрузки (PK, `YYYY-MM-DD`).
- `csv_data` — сырой CSV-текст ответа MOEX.
- `created_at` — время записи в кэш.

### `details_cache`
Кэш JSON-ответов по endpoint/бумаге (`iss/securities/*`).
- `endpoint` — имя endpoint (`security_overview`, `bondization`, ...).
- `secid` — идентификатор бумаги.
- `response_json` — JSON-ответ endpoint.
- `fetched_at` — время загрузки.
- PK: (`endpoint`, `secid`).

### `details_rows`
Нормализованное построчное представление payload.
- `fetched_at` — время загрузки.
- `endpoint` — имя endpoint.
- `secid` — идентификатор бумаги.
- `block_name` — имя блока данных в payload.
- `row_json` — строка блока в JSON.
- Индекс: `idx_details_rows_endpoint_secid_block(endpoint, secid, block_name)`.

### `details_update_watermark`
Watermark обновлений details для идемпотентности.
- `endpoint`, `secid`, `fetched_at` — уникальный ключ обновления.
- `updated_at` — когда watermark зафиксирован.

### `endpoint_health_history`
История доступности и качества endpoint.
- `checked_at` — время проверки.
- `endpoint` — endpoint.
- `secid` — бумага проверки.
- `status` — `ok` / `empty` / `error`.
- `source` — `network` / `cache` / `stale_cache` / `precheck`.
- `http_status` — HTTP-код.
- `latency_ms` — latency запроса.
- `blocks` — список блоков в ответе.
- `error_text` — текст ошибки.

### `endpoint_circuit_breaker`
Состояние circuit-breaker по endpoint.
- `endpoint` — endpoint (PK).
- `failure_count` — число подряд ошибок.
- `state` — `closed` / `half_open` / `open`.
- `opened_at` — время открытия breaker.
- `updated_at` — последнее обновление.

### `endpoint_health_mv`
Материализованные метрики стабильности endpoint по окнам `1d` и `7d`.
- `window`, `endpoint` — составной PK.
- `total_requests` / `error_requests` / `error_rate` — надежность.
- `avg_latency_ms` / `p95_latency_ms` — производительность.
- `updated_at` — время пересчета.

### `dq_run_history`
История data-quality baseline-проверок.
- `run_id` — ID прогона (PK).
- `run_at` — время проверки.
- `source` — источник данных (`network`, `cache`, `cache_fallback:*`).
- `row_count` — число строк.
- `empty_secid_ratio` — доля пустого `SECID`.
- `empty_isin_ratio` — доля пустого `ISIN`.
- `row_count_delta_ratio` — отклонение от предыдущего прогона.
- `notes` — предупреждения качества.

### `dq_metrics_daily_mv`
Дневной агрегат DQ-метрик.
- `run_day` — день (`YYYY-MM-DD`, PK).
- `runs_count`, `avg_row_count`, `max_row_count`, `min_row_count` — агрегаты объема.
- `avg_empty_secid_ratio`, `avg_empty_isin_ratio` — агрегаты полноты.
- `max_row_count_delta_ratio` — максимальный скачок.
- `warning_runs_count` — число warning-ранов.
- `updated_at` — время пересчета.

### `etl_stage_sla`
SLA/тайминги этапов ETL.
- `run_id`, `stage` — PK.
- `started_at`, `finished_at`, `duration_ms` — длительность этапа.
- `status` — `ok` / `error`.
- `source` — источник данных.
- `details` — служебные детали этапа.

### `intraday_quotes_snapshot`
Снапшоты внутридневных котировок.
- `snapshot_at` — время снапшота.
- `secid` — бумага.
- `boardid` — торговый режим.
- `tradingstatus` — статус торгов.
- `open` — цена открытия дня.
- `close` — цена закрытия/расчетная из marketdata.
- `lclose` — предыдущая цена закрытия.
- `last` — актуальная цена на момент снапшота.
- `numtrades` — число сделок.
- `volvalue` — оборот.
- `updatetime` — биржевое время обновления.
- PK: (`snapshot_at`, `secid`, `boardid`).

### `bonds_enriched`
Актуальный full snapshot обогащенной таблицы (перезаписывается на каждом прогоне).

### `bonds_enriched_incremental`
Инкрементальная история batch-экспортов finish.
- `batch_id` — ID батча.
- `export_date` — бизнес-дата.
- `exported_at` — фактическое время экспорта.
- `source` — источник данных.
- `row_json` — строка `bonds_enriched` в JSON.

## Экспортные файлы
- `Moex_Bonds.xlsx` — базовый CSV-экспорт (только `--debug`).
- `Moex_Bonds_Details.xlsx` — endpoint/block-листы + `details_transposed` (только `--debug`).
- `Moex_Bonds_Finish.xlsx` — человекочитаемый финальный срез (без тех.полей, которые остаются в БД).
- `MOEX_Bonds_Finish_Price.xlsx` — цены `open/close/now/last_today`, динамика %, TOP10 падения/роста + диаграммы.
- `finish_batches/Moex_Bonds_Finish_<date>_<batch>.xlsx` — инкрементальные batch-экспорты.
