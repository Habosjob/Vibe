# Stage3 — параллельный экспорт MOEX + DOHOD

## Что делает
Stage3 читает `candidate_bonds` (результат Stage2) и запускает два источника:
- `moex_export` по `secid` (MOEX ISS);
- `dohod_export` по `isin` (страница `https://analytics.dohod.ru/bond/{isin}`).

Каждый источник регистрируется в `runs` отдельно (`stage=stage3`, `script=moex_export|dohod_export`).

## Параллельный запуск источников
В `config/config.yaml -> stage3.run_sources_in_parallel`:
- `true` — источники запускаются параллельно (`asyncio.gather` + отдельные потоки);
- `false` — последовательный запуск.

Правило итогового статуса `scripts/stage3/run.py`:
- **OK**, если успешно отработал хотя бы один источник;
- **FAIL**, если оба источника завершились с ошибкой.

Ошибка одного источника не останавливает второй.

## DOHOD: входные данные и валидация
Вход — список `isin` из `candidate_bonds`.
- пустой `isin` → `INFO` и skip;
- `isin`, не похожий на `RU**********` (паттерн `^RU[A-Z0-9]{10}$`) → `WARN` и skip.

## DOHOD: что парсится
Парсинг делается через `BeautifulSoup` по схеме `label -> value` (таблицы, `dt/dd`, fallback по `key:value`).

Минимальные поля:
- `isin`
- `bond_name`
- `status`
- `currency`
- `issue_date` (`DD.MM.YYYY`)
- `maturity_date` (`DD.MM.YYYY`)
- `ytm_percent` (`REAL`)
- `price_last` (`REAL`)
- `nkd` (`REAL`)
- `current_nominal` (`REAL`)
- `coupon_freq_per_year` (`INTEGER`)
- `coupon_type`
- `coupon_formula_text`
- `next_payment_date` (`DD.MM.YYYY`)
- `internal_rating`
- `liquidity_score` (`REAL`)
- `warning_text`
- `fetched_at` (ISO)
- `source_hash`

Если поле не найдено — сохраняется `NULL`, а в `warning_text` добавляется `missing:<field>`.

## Таблицы Stage3
### MOEX
- `moex_security_info`
- `moex_marketdata`
- `moex_coupons`
- `moex_amortizations`
- `moex_offers`
- `moex_export_items`

### DOHOD
- `dohod_bond_profile`
- `dohod_export_items` (`status`, `last_error`, `fetched_at`)

## TTL и checkpoint
### MOEX
- TTL: `stage3.moex.ttl_hours`.
- Если `moex_export_items.status=done` и запись свежая — skip.

### DOHOD
- TTL: `stage3.dohod.ttl_hours`.
- Если `dohod_export_items.status=done` и запись свежая — skip.
- `failed` пробуется повторно, максимум **2 попытки** за один запуск для каждого ISIN.

## Сеть, кэш, retry, polite режим (DOHOD)
- Используется общий `HttpClient` + `HttpCache`.
- `User-Agent` берется из `stage3.dohod.user_agent`.
- Параллелизм: `stage3.dohod.concurrency` (по умолчанию 5).
- Пауза в каждом worker: `await sleep(stage3.dohod.min_delay_s)`.
- Позиция progressbar в консоли: `stage3.dohod.progressbar_position` (по умолчанию 1).
- Retry только для сетевых ошибок/timeout/5xx.
- На `403/404` retry не делается.
- На `404` пишется `WARN`, checkpoint получает `status=failed`.

## SQLite и параллелизм (обязательные требования)
Для устойчивой параллельной записи MOEX + DOHOD:
- включен `WAL` (`PRAGMA journal_mode=WAL`);
- каждый источник открывает **свои** соединения к SQLite;
- вставки/обновления выполняются батчами (`executemany`) внутри транзакции;
- при `database is locked` используется retry с backoff.

## Логи и прогресс

## Progressbar в параллельном режиме
Чтобы progressbar не «ломался» при одновременной работе источников, у каждого источника фиксируется своя строка в терминале:
- `stage3.moex.progressbar_position` (обычно `0`),
- `stage3.dohod.progressbar_position` (обычно `1`).

Это позволяет видеть обновление обеих полос прогресса одновременно без взаимного перезаписывания.
Для DOHOD прогресс обновляется инкрементально по мере завершения каждого ISIN (`asyncio.as_completed`), поэтому полоса не «застывает» до конца всей пачки.

- `logs/stage3_run.log`
- `logs/stage3_moex_export.log`
- `logs/stage3_dohod_export.log`

Логи перезаписываются на каждом запуске.
В консоли показываются progressbar (`tqdm`) и финальные метрики.

## Excel debug витрины
`excel_debug` влияет **только** на debug-файлы `stageX_debug_*.xlsx`.
Ручные UI-файлы (`Emitents.xlsx`, `Dropped_bonds.xlsx`) не зависят от debug.

Если `excel_debug=true` и `stage3` присутствует в `excel_debug_exports`, создаются:
- `source/xlsx/stage3_debug_moex_security_info.xlsx`
- `source/xlsx/stage3_debug_moex_marketdata.xlsx`
- `source/xlsx/stage3_debug_moex_coupons.xlsx`
- `source/xlsx/stage3_debug_moex_amortizations.xlsx`
- `source/xlsx/stage3_debug_moex_offers.xlsx` (если включены оферты)
- `source/xlsx/stage3_debug_dohod_bond_profile.xlsx`

Форматирование: bold headers, autofilter, freeze row, автоширина, даты `DD.MM.YYYY`.

## Что настраивается в `config/config.yaml`
`stage3`:
- `enabled`
- `run_sources_in_parallel`
- `ttl_hours`
- `batch_size`

`stage3.moex`:
- `enabled`, `ttl_hours`, `concurrency`, `progressbar_position`
- `engine`, `market`, `boards`, `page_size`
- `bondization.enabled`, `include_offers`, `from`, `till`

`stage3.dohod`:
- `enabled`, `ttl_hours`, `concurrency`, `progressbar_position`
- `min_delay_s`, `page_timeout_s`
- `base_url`, `user_agent`, `use_playwright`
