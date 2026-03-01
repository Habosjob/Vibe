# bond_screener

Запуск: `python main.py`.
Конфиг: `config/config.yaml`.
Итог: `source/Screener.xlsx`.

## Что сделано в v2

- Монолит сохранен в `src/pipeline.py`, логика вынесена в `src/*`.
- SORTER с таблицей `dropped_bonds` (перманентные/временные причины, TTL для оферты).
- MOEX bondization переведен на bulk endpoint с пагинацией, anti-loop и checkpoint `cache/checkpoints/moex_bondization_bulk.json`.
- Smart-Lab источник по SECID + кредитный рейтинг, включая fallback на общую таблицу `/q/bonds/`.
- Smart-Lab circuit breaker: при 403/429/captcha источник выключается до конца запуска, в Excel ставится `smartlab_status=disabled_rate_limited`.
- Параллельный запуск MOEX bulk и Smart-Lab + единый writer queue в SQLite (WAL, retry на lock, heartbeat).
- Инкрементальность по `fetched_at` и TTL/checkpoints.

## SORTER / dropped

Причины:
- `AMORT_STARTED` — current_nominal < initial_nominal (перманентно).
- `AMORT_LT_1Y` — амортизация < 365 дней (перманентно).
- `MAT_LT_1Y` — погашение < 365 дней (перманентно).
- `OFFER_LT_1Y` — оферта < 365 дней (временно, `until = offer_date + 1 day`).

В Excel добавляются поля:
`dropped_flag`, `dropped_reason_code`, `dropped_until`, `dropped_is_permanent`, `amort_started_flag`, `amort_lt_1y`, `mat_lt_1y`, `offer_lt_1y`.

## MOEX bulk bondization

- Endpoint: `.../statistics/engines/stock/markets/bonds/bondization`.
- Горизонт по умолчанию: `today-30`..`today+400`.
- Страницы пишутся сразу в БД (`получил -> записал`).
- Таблицы: `moex_coupons`, `moex_amortizations`, агрегат `moex_amort_agg`.

## Smart-Lab

- Страница бумаги: `https://smart-lab.ru/q/bonds/{SECID}/`.
- Парсятся котировки/даты/признаки/рейтинг.
- Если рейтинг не найден, применяется fallback mapping из общей таблицы `/q/bonds/`.
- Чекпоинт: `cache/checkpoints/smartlab_items.json`.

## Writer queue + WAL

- SQLite: `PRAGMA journal_mode=WAL`.
- Один writer (`asyncio.Queue`) делает `executemany`/upsert.
- Heartbeat в логах каждые ~7 секунд: сколько строк записано и размер очереди.

## Проверка результата

1. Запустить `python main.py`.
2. Проверить `logs/app.log` (summary, top-10 YTM, dropped counts, smartlab статус).
3. Проверить `source/Screener.xlsx` (форматы, подсветка, все колонки).
