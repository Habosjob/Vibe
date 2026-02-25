# Источники данных

## MOEX ISS API
- Базовая точка: `https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json`
- Используется постраничная загрузка через `start` и `limit`; `iss.only=securities` оставляет только таблицу облигаций, но все её поля без урезания.
- Поля: загружаются все колонки, которые MOEX возвращает в блоке `securities` (без ограничения `securities.columns`).

## Выходные данные
- `output/moex_bonds.xlsx` — итоговый Excel со всеми загруженными облигациями (основной формат).
- `output/moex_bonds.csv` — опциональный CSV (с UTF-8 BOM для корректного открытия в Excel).
- `raw/*.json` — отладочные сырые ответы (если включено).

- Дополнительная точка для обогащения амортизацией: `https://iss.moex.com/iss/securities/{SECID}/bondization.json` (`iss.only=amortizations`) — используется для поля `Amortization_start_date`; одиночное полное погашение (`VALUEPRC=100`) и запись в дату `MATDATE` не считаются амортизацией.
- Для справочника эмитентов используется 2 шага:
  - `https://iss.moex.com/iss/securities/{SECID}.json` (`iss.only=description`) — получить `EMITTER_ID` для бумаг, где он отсутствует в исходной строке.
  - `https://iss.moex.com/iss/emitters/{EMITTER_ID}.json` (`iss.only=emitter`) — получить статичные поля эмитента (`TITLE`, `INN`) для заполнения полного наименования и ИНН.
- Тикеры/ISIN и полный перечень эмитентов обновляются через рынки:
  - `https://iss.moex.com/iss/engines/stock/markets/shares/securities.json` (тикеры акций);
  - `https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json` (ISIN облигаций).
- В market-таблицах MOEX часто нет `EMITTER_ID/ISSUER_ID`, поэтому для каждого нового `SECID` выполняется fallback на `https://iss.moex.com/iss/securities/{SECID}.json` (`iss.only=description`), после чего соответствие `SECID -> EMITTER_ID` кэшируется в `state/secid_to_emitter.json`.

## Дополнительные выходные данные
- `output/emitents.xlsx` — справочник эмитентов из итогового набора облигаций: полное наименование, ИНН, тикеры акций, ISIN облигаций.
