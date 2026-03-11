# Scalp (автономный монолитный контур)

`Scalp/main.py` — **единый монолитный скрипт** (без CLI/argparse), который ищет краткосрочные dislocation-сигналы по облигациям только из листов `Green`/`Yellow` файла `Screener.xlsx`.

## Как работает
1. Создает изолированные папки и перезаписываемый лог `Scalp/logs/scalp.log`.
2. Читает `Screener.xlsx` (только `Green`/`Yellow`) + при наличии обогащает данными из `Emitents.xlsx`.
3. Загружает инструменты в отдельную SQLite `Scalp/DB/scalp.sqlite3`.
4. Собирает market snapshots из MOEX ISS (retry/backoff + TTL cache в `Scalp/cache/`).
5. Считает dirty-метрики (`dirty = clean + ACI`) и дельты к prev close/open/prev snapshot.
6. Строит сигналы `GapDown`, `IntradayDump`, `ReboundCandidate`, `PeerDislocation`.
7. Подавляет ложные сигналы через фильтры ликвидности и близости событий (купон/оферта/амортизация).
8. Пишет `Scalp/Scalp.xlsx` и `Scalp/BaseSnapshots/scalp_snapshot.xlsx`.

## Запуск
```bash
python Scalp/main.py
```

## Вывод в консоль
- только этапы;
- прогресс-бары `tqdm` внизу;
- итоговый `Summary` со временем по этапам и общим временем.

## Настройка
Все параметры находятся в `Scalp/config.py` и снабжены комментариями:
- пороги сигналов;
- фильтры ликвидности/событий;
- сетевые параметры;
- частота/TTL кэша;
- пути и имена выходных файлов.

## Изоляция от старого pipeline
- Старые `main.py` и `config.py` в корне **не изменяются**.
- Новый контур хранит свои артефакты только внутри `Scalp/`.
