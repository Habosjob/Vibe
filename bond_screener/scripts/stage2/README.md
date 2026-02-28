# Stage2 — ScoringSelector + DroppedManager

## Назначение

Stage2 формирует итоговый список кандидатов в облигации после двух шагов:
1. `ScoringSelector` — выбирает бумаги только по эмитентам `Greenlist`.
2. `DroppedManager` — синхронизирует ручные исключения из `Dropped_bonds.xlsx` и удаляет исключённые бумаги из `candidate_bonds`.

## База данных (SQLite)

Stage2 создаёт/поддерживает:

- `candidate_bonds`
  - `isin TEXT`
  - `secid TEXT NOT NULL PRIMARY KEY`
  - `issuer_key TEXT NOT NULL`
  - `created_at TEXT ISO`
  - `updated_at TEXT ISO`

- `dropped_manual`
  - `id INTEGER PRIMARY KEY AUTOINCREMENT`
  - `isin TEXT`
  - `secid TEXT`
  - `reason TEXT`
  - `dropped_at TEXT` (`DD.MM.YYYY`)
  - `until TEXT` (`DD.MM.YYYY` или пусто)
  - `source TEXT` (`manual`)
  - `updated_at TEXT ISO`
  - `UNIQUE(isin, secid)` для UPSERT

- `dropped_auto`
  - аналогичная структура, `source='auto'`, подготовлена для будущей автоматики

- `dropped_effective` (VIEW)
  - объединяет `dropped_manual` и `dropped_auto`
  - приоритет `manual`: если есть дубликат ключа `(isin, secid)`, запись из `auto` не попадает во view
  - учитывает TTL: записи с `until < today` автоматически исключаются из `dropped_effective`

## ScoringSelector

Источник данных:
- `emitents_effective` (из Stage1)
- `securities_raw` (из Stage1)

Логика:
- берёт только эмитентов со `scoring_flag = Greenlist`
- собирает уникальные бумаги (`secid`, `isin`, `issuer_key`)
- `candidate_bonds` **полностью пересобирается** каждый запуск (идемпотентно)

От Stage1 debug-Excel не зависит: работает только с SQLite.

## DroppedManager

### Ручной UI

Файл: `source/xlsx/Dropped_bonds.xlsx`

Если файл отсутствует, создаётся автоматически с колонками:
1. `isin`
2. `secid`
3. `reason`
4. `dropped_at`
5. `until`
6. `source`
7. `comment`
8. `updated_at`

Оформление:
- bold + серый фон заголовков
- freeze первой строки
- autofilter
- автоширина колонок (`min(max_len+2, 60)`)
- формат дат `DD.MM.YYYY`
- подсветка `source`: `manual` (светло-синий), `auto` (светло-серый)

### Синхронизация Excel → SQLite

`Dropped_bonds.xlsx` — источник manual dropped.

На каждом запуске:
- читается Excel
- валидация: заполнен хотя бы `isin` или `secid`
- даты нормализуются к `DD.MM.YYYY` (при нестандартном вводе пишется WARN в лог)
- UPSERT в `dropped_manual` по ключу `UNIQUE(isin, secid)` через `INSERT ... ON CONFLICT`

### TTL-логика

Принятый дефолт:
- история в Excel и таблицах сохраняется
- истекшие записи (`until < today`) **не удаляются физически**, но автоматически не попадают в `dropped_effective`
- это делает поведение предсказуемым и сохраняет аудит истории

### Применение исключений

После синхронизации удаляются бумаги из `candidate_bonds`, которые совпадают с `dropped_effective` по `secid` или `isin`.

## Логи

- `logs/stage2_run.log`
- `logs/stage2_scoring_selector.log`
- `logs/stage2_dropped_manager.log`

Логи перезаписываются на каждый запуск.

## Debug vs Manual Excel

- Manual UI файлы (`Emitents.xlsx`, `Dropped_bonds.xlsx`) создаются/обновляются всегда.
- Debug Excel (`stage2_debug_*.xlsx`) зависят от `excel_debug` + `excel_debug_exports` в `config/config.yaml`.
