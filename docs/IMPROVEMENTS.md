# Предложения по доработкам и профит

## 1) Что улучшено в текущем PR
- Добавлены SLA-таймеры по стадиям `fetch/parse/details/export` в отдельную таблицу `etl_stage_sla`.
- Добавлена проверка деградации: если стадия стала заметно медленнее базовой истории, пишется `WARNING`.
- Сильно снижен шум в консоли: теперь в stdout идет только прогресс details (`Progress: XX.X%`) и ошибки/предупреждения.
- Ускорено выполнение details-фазы: `ProcessPoolExecutor` заменён на `ThreadPoolExecutor` (I/O-bound сценарий).
- Ускорено обновление parquet: хэш строк считается векторно через `pd.util.hash_pandas_object`, без медленного `DataFrame.apply(..., axis=1)`.

## 2) Рекомендованное микросервисное деление
### 2.1 rates-ingest
- Ответственность: дневной CSV (fetch + parse + DQ baseline).
- Профит: изоляция канала загрузки, независимый retry/backoff, отдельный SLA.

### 2.2 details-enricher
- Ответственность: параллельный details, cache, circuit breaker, materialization (parquet/sqlite/xlsx).
- Профит: независимое масштабирование именно CPU/I/O-heavy обогащения.

### 2.3 quotes-snapshotter
- Ответственность: intraday снимки цен (частый schedule).
- Профит: деградация MOEX по intraday не блокирует дневной ingest и details.

## 3) Почему в git попал SQL/SQLite при наличии .gitignore
- `.gitignore` не удаляет уже отслеживаемые файлы.
- Если файл уже в индексе, нужно один раз выполнить `git rm --cached <file>` и закоммитить.
