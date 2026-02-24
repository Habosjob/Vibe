# DATA_SOURCES

## MOEX ISS (реализовано)

Для загрузки торгового универсума облигаций используется провайдер `moex_iss`:

- Endpoint: `https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json`.
- Формат ответа: JSON-блок `securities` (`columns` + `data`).
- Параметр `q` — поиск по коду/названию/ISIN.
- Параметры `start` и `limit` — пагинация (страничный обход всего списка).

Скрипт `scripts/sync_moex_universe.py`:

- запускается без аргументов (берет конфиг из `config/config.yml`);
- выгружает `out/universe.xlsx` и `out/universe.csv`;
- сохраняет инструменты в таблицу `instruments` SQLite;
- показывает этапы и прогресс загрузки.

## MOEX ISS bondization (реализовано)

Для расписания выплат по бумаге используется endpoint:

- `https://iss.moex.com/iss/securities/{SECID}/bondization.json`.

Скрипт `scripts/sync_moex_cashflows.py`:

- запускается без аргументов;
- читает бумаги из таблицы `instruments`;
- загружает доступные блоки `coupons`/`amortizations`/`redemptions`;
- сохраняет cashflows в `cashflows` с типами `coupon` / `amort` / `redemption`;
- вычисляет и сохраняет в `instrument_fields`:
  - `maturity_date`;
  - `next_coupon_date`;
  - `amort_start_date`;
  - `has_amortization`.

## Модули/скрипты

- `bond_screener/providers/moex_iss.py`: загрузка облигаций через ISS, пагинация и нормализация полей.
- `bond_screener/providers/moex_cashflows.py`: загрузка и нормализация расписания выплат, расчет derived-полей.
- `scripts/sync_moex_universe.py`: синхронизация универсума в файлы `out/` и SQLite.
- `scripts/sync_moex_cashflows.py`: синхронизация cashflows и derived-полей в SQLite + sample Excel.

## Входные данные

- Конфиги из `config/`.
- Таблица `instruments` (для `sync_moex_cashflows.py`).

## Выходные данные

- Лог этапов в `logs/latest.log`.
- Файлы `out/universe.xlsx` и `out/universe.csv` (universe sync).
- Файлы `out/cashflows_sample.xlsx` и `out/derived_sample.xlsx` (cashflows sync).
- Записи `instruments`, `cashflows`, `instrument_fields` в SQLite.
- В debug-режиме — raw-дампы в `raw/<provider>/<date>/...`.

## Как менять

- Изменяйте параметры в `config/config.yml`:
  - `providers.moex_iss.limit`;
  - `providers.moex_iss.q`;
  - `providers.moex_iss.cache_ttl_seconds`;
  - `providers.moex_iss.cashflows_cache_ttl_seconds`;
  - `providers.moex_iss.cashflows_concurrency`;
  - `providers.moex_iss.rate_limit_per_sec`;
  - `database.path`.

## Как мы бережем сайты

- Используем `bond_screener/http_client.py` с `httpx.AsyncClient` и connection pooling: меньше лишних TCP-соединений.
- На каждый домен действует ограничение `max_concurrency` и `rate_limit_per_sec`, чтобы не создавать избыточную нагрузку.
- Для временных сбоев включены ретраи (timeout, HTTP 429 и 5xx) с экспоненциальным backoff и jitter.
- Ответы кэшируются в SQLite (`cache/http_cache.sqlite` или другой путь из конфигурации) по ключу `method + url + params + body_hash` с TTL.
- В debug-режиме можно сохранять raw-ответы в `raw/<provider>/<date>/...` для диагностики, в обычном режиме это отключается.
