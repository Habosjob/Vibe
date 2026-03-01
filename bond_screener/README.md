# bond_screener

Монолитный пайплайн для сборки скринера облигаций из MOEX + DOHOD + bondization.

## Установка

```bash
cd bond_screener
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

## Запуск

```bash
python main.py
```

Запуск без аргументов. Конфиг готов для первого запуска: `config/config.yaml`.

## Что делает pipeline

1. Скачивает `rates.csv` MOEX (cp1251, `;`), кеширует в `cache/http`, пишет в SQLite:
   - `moex_rates_raw` (все колонки)
   - `moex_rates_norm` (нормализованные ключевые поля)
2. Качает Excel с DOHOD через Playwright (`Скачать Excel`) в `source/dohod_export.xlsx` с TTL.
   - Пишет `dohod_raw` и `dohod_norm`.
3. Делает per-SECID запросы bondization (`amortizations,coupons`) с TTL + чекпоинтами (`cache/checkpoints`).
   - Пишет `moex_coupons`, `moex_amortizations`, `moex_amort_start`.
4. Мержит все колонки (конфликты с префиксами `moex_`/`dohod_`), считает `filter_amort_ok`.
5. Считает `ytm_calc` (ACT/365, dirty price = clean + НКД, купоны + амортизации + остаток номинала).
   - Для FRN используется rolling horizon по `key_rate_avg_percent` + премия из DOHOD.
   - Для линкеров применяется rolling inflation по `linker_inflation_percent`.
6. Экспортирует `source/Screener.xlsx`:
   - bold header, серый фон, autofilter, freeze header
   - автоширина (до 60)
   - формат дат `DD.MM.YYYY`

## TTL и чекпоинты

- TTL настраивается в `config/config.yaml`.
- HTTP-ответы хранятся в `cache/http`.
- Статус обработки отдельных SECID — `cache/checkpoints`.

## Основные артефакты

- `data/bonds.db`
- `source/dohod_export.xlsx`
- `source/Screener.xlsx`
- `logs/app.log`

## Что проверять в Screener.xlsx

- `ytm_calc` заполнен для бумаг с достаточными cashflow-данными.
- `warning_text` объясняет, почему `ytm_calc` может быть пустым.
- `has_amortization`, `days_to_amort`, `filter_amort_ok` рассчитаны корректно.
