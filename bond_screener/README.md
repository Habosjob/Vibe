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

## Как понять, что единицы цены корректны

Пайплайн теперь явно нормализует цену и НКД в валюту номинала перед расчетом YTM и сохраняет дебаг-колонки:

- `price_unit`: как интерпретирована clean price (`percent_of_nominal` или `currency`).
- `clean_price_amt`: clean price после перевода в сумму в валюте номинала.
- `nkd_amt`: НКД в той же валюте (с эвристикой для редкого случая НКД в %).
- `dirty_price_amt`: итоговая dirty price = `clean_price_amt + nkd_amt`.
- `warnings` / `warning_text`: сообщения о fallback и эвристиках (`nominal_defaulted_1000`, `nkd_assumed_percent`, `ytm_outlier` и т.д.).

Быстрая проверка в `logs/app.log`:

1. Смотри блок `Self-check top ytm rows` — там топ-10 по `ytm_calc` вместе с `price_unit`, `dirty_price_amt` и номиналом.
2. Если много `ytm_outlier`, проверь, не попали ли инструменты с ценой в % в ветку `currency` или наоборот.
3. Для обычных рублевых облигаций `dirty_price_amt` чаще всего должен быть близок к номиналу (или его разумной доле), а не на порядки меньше/больше.

## Troubleshooting: moex_rates header + price units

### `moex_rates_norm` пустой по `norm_secid` / `norm_isin`

Причина обычно в преамбуле MOEX CSV: заголовок находится не в первой строке.
Пайплайн теперь:

- читает первые строки как текст (cp1251),
- находит строку-заголовок по `SECID`/`ISIN`,
- только потом делает `read_csv` с правильным `skiprows/header`.

В `logs/app.log` проверяй:

- `MOEX rates detected header_row=...`
- `MOEX rates columns(first30)=...`
- `MOEX rates notnull: SECID=... ISIN=... NAME=...`

Если после этого `SECID` так и не найден, пайплайн аварийно завершится с явной ошибкой `ValueError` — это ожидаемое защитное поведение.

### Bondization показывает `universe_rows=0`

Теперь universe строится в первую очередь из `moex_rates_norm.norm_secid` (без фильтра по ISIN).
Если по какой-то причине `norm_secid` пустой, включается fallback из `dohod_norm.norm_isin` как временный `secid`.
Если даже после fallback universe пустой — выбрасывается `RuntimeError`, потому что дальнейшие расчёты бессмысленны.

### `ytm_calc` “космический”

Пайплайн нормализует единицы до расчетов:

- выбирает `nominal_used` (`dohod_current_nominal` → `moex_facevalue` → `1000`),
- переводит цену в сумму (`price_unit`: `% от номинала` или `currency`),
- считает `dirty_price_amt = clean_price_amt + nkd_amt`,
- ставит предупреждения `dirty_price_too_low`, `bad_dirty_price`, `ytm_outlier`.

Для аудита смотри колонки: `nominal_used`, `price_unit`, `clean_price_amt`, `nkd_amt`, `dirty_price_amt`, `ytm_is_outlier`.
