# scripts/screen_basic.py

Короткое назначение: базовый скрининг облигаций из SQLite с выгрузкой в Excel (`screen_pass`/`screen_drop`).

## Входные данные

- SQLite БД (`database.path`, по умолчанию `data/bond_screener.sqlite`).
- Таблица `instruments` (ISIN, `secid`, `name`, `tags_json`).
- Таблица `instrument_fields` с derived-полями:
  - `maturity_date`
  - `offer_date` (если неизвестна — не даёт причину)
  - `amort_date`
  - `coupon_type` (опционально, для распознавания ОФЗ-ПК)

## Правила фильтрации

Причины попадания в `screen_drop`:
- `maturity_lt_365`
- `offer_lt_365`
- `amort_lt_365`
- `ofz_pk_excluded`

Отдельное правило для ОФЗ:
- ОФЗ-ПК исключаются.
- ОФЗ-ИН остаются в скрине.

## Классификатор

Минимальный классификатор записывает в `instrument_fields` поле:
- `field=bond_class`
- `value` из множества `OFZ/Corp/Subfed/Muni/Other`

Классификация строится по `secid`, наименованию и тегам MOEX (`tags_json`).

## Выходные файлы

- `out/screen_basic.xlsx`
  - лист `screen_pass`
  - лист `screen_drop`

## Как менять конфиг

В `config/config.yml` можно изменить:
- `database.path`
- `output.screen_basic_excel`
- `logging.file` / `logging.level`
