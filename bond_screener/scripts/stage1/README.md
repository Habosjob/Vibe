# Stage1 — MoexEmitentsCollector

## Что делает Stage1

Stage1 собирает с MOEX ISS:
- список активных (не погашенных) облигаций;
- справочник эмитентов из `/iss/securities.json` и связывает его с облигациями по `secid`.

После этого:
1. Обновляет raw-таблицы SQLite.
2. Полностью пересобирает ручной Excel UI `source/xlsx/Emitents.xlsx`.
3. Синхронизирует ручные поля из Excel в `emitents_manual`.
4. Обновляет витрину `emitents_effective`.

## Входы

- `db/bonds.db` (таблица `runs`, данные прошлых запусков)
- `source/xlsx/Emitents.xlsx` (если существует, из него читаются ручные поля)
- MOEX ISS API

## Выходы

- `emitents_raw`
- `securities_raw`
- `emitents_manual`
- `emitents_effective` (VIEW)
- `source/xlsx/Emitents.xlsx`
- при debug: 
  - `source/xlsx/stage1_debug_emitents_raw.xlsx`
  - `source/xlsx/stage1_debug_securities_raw.xlsx`

## Формат `Emitents.xlsx`

Колонки строго:
1. `issuer_key`
2. `inn`
3. `name`
4. `scoring_flag`
5. `scoring_date`
6. `comment`
7. `group_hint`
8. `active_bonds_count`

### Валидация `scoring_flag`
- DataValidation list: `Greenlist,Yellowlist,Redlist`
- Пустое значение разрешено.
- На серверной стороне, если значение не из списка, оно очищается и пишется WARN в лог.

### Правила `scoring_date` (DD.MM.YYYY)
- В Excel хранится как тип date (не строка), формат ячейки `DD.MM.YYYY`.
- В SQLite (`emitents_manual.scoring_date`) хранится строкой `DD.MM.YYYY`.
- Если `scoring_flag` пустой — дата очищается.
- Если `scoring_flag` не пустой и дата отсутствует — ставится текущая локальная дата.

## TTL и инкрементальность

`config/config.yaml`:

```yaml
stage1:
  ttl_hours: 24
```

Логика:
- если последний успешный run Stage1 младше `ttl_hours`, сетевой сбор пропускается;
- при этом `Emitents.xlsx` всё равно пересобирается из данных БД;
- если `Emitents.xlsx` отсутствует, он создаётся в любом случае.

## Логи и прогресс

- `logs/stage1_run.log`
- `logs/stage1_moex_emitents_collector.log`

В консоли показываются прогрессбары `tqdm`:
- пагинация облигаций MOEX;
- пагинация справочника эмитентов MOEX.
