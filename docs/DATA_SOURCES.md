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

## Модуль/скрипт
- `bond_screener/providers/moex_iss.py`: загрузка облигаций через ISS, пагинация и нормализация полей.
- `scripts/sync_moex_universe.py`: синхронизация универсума в файлы `out/` и SQLite.

## Входные данные
- Конфиги из `config/`.

## Выходные данные
- Лог этапов в `logs/latest.log`.
- Файлы `out/universe.xlsx` и `out/universe.csv`.
- Записи `instruments` в SQLite.
- В debug-режиме — raw-дампы в `raw/moex_iss/<date>/...`.

## Как менять
- Изменяйте параметры в `config/config.yml`:
  - `providers.moex_iss.limit`;
  - `providers.moex_iss.q`;
  - `providers.moex_iss.cache_ttl_seconds`;
  - `database.path`.

## Как мы бережем сайты
- Используем `bond_screener/http_client.py` с `httpx.AsyncClient` и connection pooling: меньше лишних TCP-соединений.
- На каждый домен действует ограничение `max_concurrency` и `rate_limit_per_sec`, чтобы не создавать избыточную нагрузку.
- Для временных сбоев включены ретраи (timeout, HTTP 429 и 5xx) с экспоненциальным backoff и jitter.
- Ответы кэшируются в SQLite (`cache/http_cache.sqlite` или другой путь из конфигурации) по ключу `method + url + params + body_hash` с TTL.
- В debug-режиме можно сохранять raw-ответы в `raw/<provider>/<date>/...` для диагностики, в обычном режиме это отключается.
