# Скрипт выгрузки облигаций MOEX + CorpBonds

Скрипт загружает торгуемые облигации MOEX, объединяет данные с CorpBonds, формирует справочник эмитентов для ручного скоринга и итоговый файл Screener.

## Установка

```bash
pip install -r requirements.txt
```

## Запуск

```bash
python main.py
```

## Что делает скрипт

1. Загружает и обогащает данные облигаций (MOEX + CorpBonds).
2. Формирует `MergeBonds.xlsx` (полный объединенный набор).
3. Формирует `Emitents.xlsx` из уникальных эмитентов:
   - колонки: `Наименование Эмитента`, `ИНН эмитента`, `Рейтинг`, `ScoreList`, `DateScoreList`;
   - `ScoreList` имеет строгий выбор из `GreenList`, `YellowList`, `RedList`;
   - ранее выставленные `ScoreList`/`DateScoreList` сохраняются и не затираются.
4. Пропускает облигации через сортер (все фильтры отключаются в `config.py`).
5. Формирует `Screener.xlsx` в корне проекта с листами `Green`, `Yellow`, `Red`, `Unsorted`.

## Где менять настройки

Все параметры находятся в `config.py`:

- фильтры сортера: `SCREENER_FILTERS`;
- значения ручного скоринга: `SCORE_LIST_ALLOWED_VALUES`;
- набор колонок в Screener: `SCREENER_INCLUDE_COLUMNS`, `SCREENER_EXCLUDE_COLUMNS`;
- пути, таймауты, ретраи, параллельность, TTL кэша, имена файлов.

## Выходные файлы

- `output/MoexBonds.xlsx`
- `output/CorpBonds.xlsx`
- `output/MergeBonds.xlsx`
- `output/Emitents.xlsx`
- `Screener.xlsx` (в корне проекта)

## Папки проекта

- `output/` — Excel-выгрузки;
- `logs/` — логи с ротацией;
- `db/` — SQLite-база (снимок цен и кэши);
- `cache/` — состояние выполнения (`state.json`);
- `raw/` — сырые файлы/резерв.

## Сброс артефактов

```bash
python erase_data.py
```

## Частые проблемы

1. `ModuleNotFoundError` — зависимости не установлены.
2. Пустые поля CorpBonds — часть карточек недоступна или изменилась разметка.
3. Слишком строгая фильтрация — проверьте `SCREENER_FILTERS`.
4. Не появляются облигации на листах Green/Yellow/Red — в `Emitents.xlsx` не заполнен `ScoreList`.
5. При сетевых ошибках смотрите `logs/app.log`.
