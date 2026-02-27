# Python_Sorter + main.py — фильтрация и общий запуск пайплайна

## Обновленная логика Sorter

### `Python_Sorter.py`
Скрипт читает `Moex_Bonds.xlsx`, применяет фильтры и работает в 5 шагов с интерактивным прогресс-баром:
1. Загружает основной Excel.
2. Загружает `DropedBonds.csv` и удаляет из него **истекшие** исключения.
3. Исключает из текущего набора бумаги, которые уже в `DropedBonds` и относятся к активным фильтрам.
4. Применяет активные фильтры и формирует обновленный реестр `DropedBonds`.
5. Сохраняет Excel и CSV **только если есть изменения** (иначе пишет в консоль: `Изменений в данных нет — пересборка Excel пропущена.`).

### Сроки исключений (`DropedBonds`)
В `DropedBonds.csv` добавлена колонка `ИсключенДо`:
- `Бессрочно` — бумага исключена без срока;
- `YYYY-MM-DD` — дата, до которой бумага исключена;
- по истечении срока запись автоматически удаляется из `DropedBonds` и бумага снова проверяется в следующем запуске.

Поддерживаемые настройки срока на уровне фильтра:
- `permanent: true` — бессрочно;
- `exclude_until: YYYY-MM-DD` — фиксированная дата;
- `ttl_days: N` — дата считается как `сегодня + N дней`.

Приоритет: `permanent` → `exclude_until` → `ttl_days`.

### Фильтры по умолчанию
1. `BOND_TYPE = Структурная облигация`
   - бессрочное исключение (`permanent: true`).
2. `IS_QUALIFIED_INVESTORS = 1`
   - исключение на 30 дней (`ttl_days: 30`).
3. `HIGH_RISK = 1`
   - исключение на 30 дней (`ttl_days: 30`).

---

## `main.py`
Оркестратор пайплайна без изменений по шагам:
1. Запускает `Moex_Bonds.py`.
2. Затем запускает `Python_Sorter.py`.

Запуск:
```bash
python3 main.py --config config/moex_bonds.yaml
```

---

## Интерактивный вывод в консоль
И `Python_Sorter.py`, и `main.py` показывают прогресс-бар.
Пользователь видит, что скрипт выполняется и не завис.

---

## Логи (перезаписываемые)
Каждый скрипт пишет отдельный лог в режиме перезаписи (`mode="w"`):
- `logs/Moex_Bonds.log`
- `logs/Python_Sorter.log`
- `logs/main.log`

Логов достаточно для дебага полного сценария по шагам.

---

## Настройка через YAML
Все настройки вынесены в `config/moex_bonds.yaml`.

### Управление колонками в итоговом Excel
- В `output.drop_columns` добавлены `COUPONDATE` и `ISSUEDATE`.
- `OFFERDATE` и `MATDATE` удалены из списка `output.drop_columns`, поэтому снова остаются в итоговом Excel.

### Раздел `sorter`
```yaml
sorter:
  input:
    excel_path: "Moex_Bonds.xlsx"
    sheet_name: "MOEX_BONDS"
  output:
    excel_path: "Moex_Bonds.xlsx"
    sheet_name: "MOEX_BONDS"
    dropped_path: "DropedBonds.csv"
    dropped_encoding: "utf-8-sig"
  logging:
    path: "logs/Python_Sorter.log"
  cache:
    state_path: "logs/cache/sorter_state.json"
  performance:
    skip_rebuild_if_unchanged: true
  filters:
    - name: "exclude_structured_bonds"
      enabled: true
      column: "BOND_TYPE"
      equals: "Структурная облигация"
      reason: "BOND_TYPE = Структурная облигация"
      permanent: true
    - name: "exclude_qualified_only"
      enabled: true
      column: "IS_QUALIFIED_INVESTORS"
      equals: "1"
      reason: "IS_QUALIFIED_INVESTORS = 1"
      ttl_days: 30
    - name: "exclude_high_risk"
      enabled: true
      column: "HIGH_RISK"
      equals: "1"
      reason: "HIGH_RISK = 1"
      ttl_days: 30
```

### Как включать/выключать фильтры
У каждого фильтра есть `enabled`:
- `true` — фильтр активен;
- `false` — фильтр отключен.

---

## Отладка
Если фильтрация отрабатывает не так, как ожидается:
1. Проверьте имя `column` и значение `equals`.
2. Проверьте параметры срока (`permanent`, `exclude_until`, `ttl_days`).
3. Проверьте `enabled`.
4. Откройте `logs/Python_Sorter.log`:
   - сколько записей загружено из `DropedBonds`;
   - сколько истекших исключений очищено;
   - сколько бумаг исключено как «ранее отброшенные»;
   - сколько новых бумаг исключено фильтрами;
   - был ли пропуск пересборки из-за отсутствия изменений.
