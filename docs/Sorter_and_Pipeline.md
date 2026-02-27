# Python_Sorter + main.py — фильтрация и общий запуск пайплайна

## Что добавлено

### `Python_Sorter.py`
Скрипт читает итоговый Excel `Moex_Bonds.xlsx`, применяет фильтры и формирует 2 результата:
1. `Moex_Bonds_Filtered.xlsx` — очищенный список бумаг после исключений.
2. `DropedBonds.csv` — исключенные бумаги в формате:
   - `ISIN;SECID;Причина`

Первый фильтр уже настроен:
- исключать строку, если `BOND_TYPE == "Структурная облигация"`.

### `main.py`
Общий оркестратор пайплайна:
1. Запускает `Moex_Bonds.py`.
2. После успешного завершения запускает `Python_Sorter.py`.

Таким образом запуск теперь один:
```bash
python3 main.py --config config/moex_bonds.yaml
```

---

## Интерактивный вывод в консоль
И `Python_Sorter.py`, и `main.py` показывают прогресс-бар в консоли.
Это позволяет видеть, что процесс выполняется и не завис.

---

## Логи (перезаписываемые)
Каждый скрипт пишет отдельный лог в режиме перезаписи (`mode="w"`):
- `logs/Moex_Bonds.log`
- `logs/Python_Sorter.log`
- `logs/main.log`

Этого достаточно для дебага каждого шага по отдельности.

---

## Настройка через YAML
Все настройки вынесены в `config/moex_bonds.yaml`.

### Новые секции

#### `pipeline`
```yaml
pipeline:
  logging:
    path: "logs/main.log"
```

#### `sorter`
```yaml
sorter:
  input:
    excel_path: "Moex_Bonds.xlsx"
    sheet_name: "MOEX_BONDS"
  output:
    filtered_excel_path: "Moex_Bonds_Filtered.xlsx"
    sheet_name: "MOEX_BONDS"
    dropped_path: "DropedBonds.csv"
    dropped_encoding: "utf-8-sig"
  logging:
    path: "logs/Python_Sorter.log"
  filters:
    - name: "exclude_structured_bonds"
      enabled: true
      column: "BOND_TYPE"
      equals: "Структурная облигация"
      reason: "BOND_TYPE = Структурная облигация"
```

### Как включать/выключать фильтры
У каждого фильтра есть флаг `enabled`:
- `true` — фильтр активен.
- `false` — фильтр пропускается.

Пример отключения:
```yaml
enabled: false
```

---

## Отладка
Если фильтр не применился:
1. Проверьте название колонки в `column` (должно точно совпадать, например `BOND_TYPE`).
2. Проверьте значение в `equals`.
3. Откройте `logs/Python_Sorter.log` — там видно, был ли фильтр применен, сколько строк исключено и какие колонки найдены.
