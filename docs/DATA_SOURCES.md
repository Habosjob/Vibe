# Источники данных

## MOEX ISS API
- Базовая точка: `https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json`
- Используется постраничная загрузка через `start` и `limit`.
- Поля: `SECID`, `SHORTNAME`, `ISIN`, `FACEUNIT`, `LISTLEVEL`, `PREVLEGALCLOSEPRICE`.

## Выходные данные
- `output/moex_bonds.csv` — итоговый CSV со всеми загруженными облигациями.
- `raw/*.json` — отладочные сырые ответы (если включено).
