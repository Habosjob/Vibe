# bond_screener

Монолитный пайплайн для построения скринера облигаций с учетом оферты/погашения, НКД, амортизаций и сценарных доходностей.

## Установка

```bash
cd bond_screener
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install
```

## Запуск

```bash
python main.py
```

Без аргументов. Конфиг по умолчанию: `config/config.yaml`.

## Что формируется

- `source/Screener.xlsx` — итоговый читабельный файл со всеми колонками и вычисленными полями.
- `source/dohod_export.xlsx` — выгрузка DOHOD.
- `data/bonds.db` — SQLite с raw/norm/merged/market таблицами.
- `logs/app.log` — единый лог.

## TTL, кэш и чекпоинты

- HTTP кэш: `cache/http` (TTL по источникам из `config.yaml`).
- Bondization чекпоинты: `cache/checkpoints/bondization.json`.
- Для SECID со статусом `done` и непротухшим TTL запрос повторно не идет.
- `failed` SECID пробуются заново на следующем запуске.

## Модель RUONIA/ZCYC (вариант B)

- `key_rate_scenario(date)` берет rolling из `scenario.key_rate_avg_percent` по году горизонта.
- `ruonia_forecast(date) = key_rate_scenario(date) + (ruonia_today - key_rate_today_percent)`.
- `zcyc_forecast(tenor, date) = zcyc_yield_today(tenor) + (key_rate_scenario(date) - key_rate_today_percent)`.

## Правило оферта vs погашение

- `target_date = offer_date`, если оферта есть.
- иначе `target_date = maturity_date`.
- Все cashflow после `target_date` не учитываются.

## Амортизация и фильтр

- Амортизации учитываются всегда в cashflow.
- Вычисляются: `has_amortization`, `days_to_amort` (до первой положительной амортизации).
- Фильтр `filter_amort_ok = days_to_amort is null OR >= min_days_to_amort`.

## Доходности

- `fixed_cashflow`: IRR по фактическим купонам/амортизациям + redemption до горизонта, с dirty price (`clean + nkd`).
- `floater_scenario`: IRR по прогнозным купонам от RUONIA/ZCYC + spread.
- `linker_scenario`: базовый сценарий индексации по `linker_inflation_percent`.
- `zero_coupon`: для дисконтных/нулевок без купонов.
- `perpetual_compounded`: капитализированная купонная доходность для perpetual/subord.

`warning_text` заполняется для всех бумаг, если были fallback/пропуски данных.
