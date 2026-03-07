# Monitoring (автономный контур)

## Назначение
Автономный контур мониторинга работает независимо от корневого `main.py`:
- ведет собственную SQLite БД `monitoring/DB/monitoring.sqlite3`;
- пишет собственный лог `monitoring/logs/monitoring.log`;
- хранит собственный cache/raw/snapshots;
- формирует витрины `monitoring/Reports_monitoring.xlsx` и `monitoring/Portfolio.xlsx`.

## Запуск
```bash
python -m monitoring.main
```
или
```bash
python monitoring/main.py
```

## Этапы выполнения
1. Загрузка `Emitents.xlsx` (read-only).
2. Поиск e-disclosure по ИНН и сбор отчетности.
3. Сравнение snapshot рейтингов (без парсинга агентств).
4. Обновление snapshot эмитентов.
5. Загрузка пользовательского портфеля.
6. Сбор новостей Smartlab по 2 стратегиям (ticker -> fallback tag).
7. Экспорт Excel-витрин.

## Основные настройки
Все флаги находятся в `monitoring/config.py` с комментариями:
- таймауты/retry/backoff;
- TTL кэшей;
- порог stale-alert;
- путь к портфелю;
- маски поиска портфеля;
- оформление Excel.

## Идемпотентность
- события отчетности и рейтингов dedup по `event_hash` в `report_events`;
- повторный запуск обновляет `last_seen_at`, но не плодит дубликаты;
- новости dedup через CSV-кэш и таблицу `news_events`.
