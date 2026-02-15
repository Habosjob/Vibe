# Vibe

Минимальный ingest-пайплайн для загрузки CSV `rates` из MOEX ISS (облигации) и сохранения:
- `raw` CSV для реплея/трассируемости;
- Excel-витрины с атомарной перезаписью (`rates` + `meta`).

## Запуск

```bash
python -m vibe.app moex-bond-rates \
  --out data/curated/moex/bond_rates.xlsx \
  --raw data/raw/moex/bond_rates.csv
```

По умолчанию используется статичный URL MOEX ISS из `vibe/config.py`.

## Допущения

- Данные листа `rates` сохраняются «как есть», без сложной нормализации.
- Есть базовая валидация схемы, приведение числовых/датовых полей и логирование качества (добавленных NaN).
- `data/raw/...` и `data/curated/...` создаются автоматически и не коммитятся.
