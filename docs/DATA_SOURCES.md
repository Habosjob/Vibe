# Источники данных

## MOEX ISS API
- Базовая точка: `https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json`
- Используется постраничная загрузка через `start` и `limit`.
- Поля: `SECID`, `SHORTNAME`, `ISIN`, `FACEUNIT`, `LISTLEVEL`, `PREVLEGALCLOSEPRICE`.

## Выходные данные
- `output/moex_bonds.xlsx` — итоговый Excel со всеми загруженными облигациями (основной формат).
- `output/moex_bonds.csv` — опциональный CSV (с UTF-8 BOM для корректного открытия в Excel).
- `raw/*.json` — отладочные сырые ответы (если включено).
