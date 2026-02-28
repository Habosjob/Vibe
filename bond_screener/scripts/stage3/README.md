# Stage3 — MOEX export (security/marketdata/coupons/amortizations/offers)

## Что делает
Stage3 читает `candidate_bonds` (результат Stage2) и по каждой бумаге (`secid`) загружает:
- `moex_security_info` — идентификаторы и параметры бумаги;
- `moex_marketdata` — торговые поля (цены, доходности, НКД и т.п.);
- `moex_coupons` — график купонов;
- `moex_amortizations` — график амортизаций (**обязательно**);
- `moex_offers` — оферты (если включено в конфиге);
- `moex_export_items` — checkpoint/статус по каждой бумаге.

## Endpoint'ы
- Security + marketdata:
  - `/iss/engines/{engine}/markets/{market}/securities/{SECID}.json`
- Купоны/амортизации/оферты (основной):
  - `/iss/statistics/engines/{engine}/markets/{market}/bondization/{SECID}.json`
  - параметры: `iss.meta=off`, `iss.json=extended`, `iss.only=amortizations,coupons[,offers]`, опционально `from`, `till`.

Если `bondization` вернул 404 или пусто по `coupons+amortizations`, в `moex_export_items.last_error` пишется `bondization_unavailable`, но security/marketdata всё равно сохраняются.


## Почему раньше могли быть пустые `coupons/amortizations/offers`
MOEX `bondization` при `iss.json=extended` отдает ответ в виде массива:
`[{"charsetinfo": ...}, {"coupons": [...], "amortizations": [...], "offers": [...]}]`.

Теперь Stage3 корректно разбирает **оба** формата (и стандартный, и `extended`),
поэтому таблицы/Excel-файлы `stage3_debug_moex_coupons`, `stage3_debug_moex_amortizations`,
`stage3_debug_moex_offers` заполняются данными, если они есть на стороне MOEX.

## Checkpoints и TTL
- Таблица `moex_export_items` хранит `status`, `fetched_at`, флаги `info_ok/market_ok/bondization_ok/offers_ok`.
- Если `status=done` и запись свежее `stage3.ttl_hours`, бумага пропускается.
- Повторный запуск продолжает обработку с `pending/failed` и просроченных `done`.

## Защита от зацикливания в пагинации
Для любых таблиц с курсором используется только `<table>.cursor`:
- продолжение по `INDEX + PAGESIZE < TOTAL`;
- anti-loop: если повторилась сигнатура страницы `(start, first_row)` — цикл прерывается и пишется WARN в лог.

## Что можно настроить в config/config.yaml
`stage3`:
- `enabled` — запуск Stage3;
- `ttl_hours` — TTL checkpoint;
- `batch_size` — размер пачки бумаг;
- `concurrency` — число параллельных задач;
- `moex.engine`, `moex.market`, `moex.boards`, `moex.page_size`;
- `moex.bondization.enabled`, `include_offers`, `from`, `till`.

Логи:
- `logs/stage3_run.log`
- `logs/stage3_moex_export.log`
