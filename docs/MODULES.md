# Модули и скрипты

## `run.py`
- Назначение: запуск полного сценария без аргументов.
- Вход: `config.yml` (опционально), сеть, состояние в `state/`.
- Выход: `output/moex_bonds.xlsx` (лист `MOEX_BONDS` + лист `SUMMARY`), `output/emitents.xlsx` (лист `EMITENTS`), `logs/latest.log`, `state/exclusions.json`, `state/exclusions_history.json`, `state/eligible_bonds.json`, `state/emitents_registry.json`, `state/secid_to_emitter.json`, чекпоинты `state/checkpoints/*.json`, опционально `raw/*.json`.
- Как менять конфиг: правьте `config.yml` (см. `docs/CONFIG.md`).

## `scripts/run_emitents.py`
- Назначение: отдельный запуск только этапа справочника эмитентов (без полного пересчета скринера).
- Вход: состояние `eligible_bonds` из выбранного бэкенда (`json` или `sqlite`), `config.yml`, сеть.
- Выход: `output/emitents.xlsx` + обновление кэша эмитентов.


## `scripts/show_runs.py`
- Назначение: быстрый просмотр истории запусков из SQLite-таблицы `runs` в табличном виде или JSON.
- Вход: `config.yml`, состояние `storage_backend=sqlite`.
- Выход: печать отчета в консоль (`table`/`json`).

## `moex_bond_screener/config.py`
- Назначение: загрузка конфигурации и дефолтов.
- Вход: `config.yml`.
- Выход: объект `AppConfig`.

## `moex_bond_screener/moex_client.py`
- Назначение: постраничный запрос облигаций с ретраями и задержками; парсит все поля `securities`, которые вернула MOEX, и обогащает бумаги полем `Amortization_start_date` по endpoint `bondization`; загрузка амортизаций выполняется параллельно с отдельным rate-limit для `bondization`, thread-local HTTP-сессиями и безопасной обработкой ошибок воркеров, а одиночное полное погашение не считается амортизацией. Также отдает `description` по конкретной бумаге и списки инструментов рынков `shares`/`bonds` для справочника эмитентов, с защитой от бесконечной пагинации при повторяющихся страницах MOEX и с урезанными колонками для рыночных запросов (быстрее кэш/JSON).
- Вход: `AppConfig`, logger.
- Выход: список облигаций + число ошибок; при обогащении — дата начала амортизации или пустое значение.

## `moex_bond_screener/exclusion_rules.py`
- Назначение: сортировщик исключений по датам (`BUYBACKDATE`, `OFFERDATE`, `CALLOPTIONDATE`, `MATDATE`) с сохранением порядка правил + бессрочное исключение по `Amortization_start_date` (< 1 года или уже началась амортизация).
- Вход: список облигаций + ранее сохраненные исключения.
- Выход: допущенные бумаги, актуальные исключения, статистика фильтрации по правилам.

## `moex_bond_screener/state_store.py`
- Назначение: инкрементальное состояние (исключения + кэш допущенных бумаг + кэш статичных данных эмитентов) и чекпоинты выполнения. Поддерживает `storage_backend=sqlite|json` (по умолчанию `sqlite`).
- Вход: список допущенных бумаг и/или словарь исключений.
- Выход: JSON-файлы в `state/` или SQLite `state/screener_state.db`; для SQLite дополнительно ведется таблица `runs` с метриками запусков.

## `moex_bond_screener/raw_store.py`
- Назначение: сохранять raw JSON и чистить папку `raw/` по TTL/размеру.
- Вход: payload строкой, настройки TTL/лимита.
- Выход: файлы `raw/*.json`.

## `moex_bond_screener/writer.py`
- Назначение: запись итогового Excel (`.xlsx`) или CSV (`.csv`) с очисткой лишних колонок, объединением дублей, нормализацией числовых строк (в т.ч. с пробелами/неразрывными пробелами), форматированием дат и группировкой столбцов (с разделителями и сворачиванием/разворачиванием в Excel). Правила группировки/порядка читаются из `excel_layout.yml`.
- Вход: список словарей с облигациями; для Excel можно передать метрики запуска (`bonds_count`, `errors_count`, `elapsed_seconds`, метрики фильтров) для листа `SUMMARY`.
- Выход: Excel/CSV-файл.

## `moex_bond_screener/emitents.py`
- Назначение: сбор справочника эмитентов. Источник — не только `eligible_bonds`, но и полный список инструментов MOEX из рынков `bonds`/`shares`, чтобы не терять эмитентов, которые не попали в итоговый фильтр. Если в market-таблицах нет `EMITTER_ID`, модуль дозапрашивает `description` по `SECID`, кэширует `SECID -> EMITTER_ID`, затем подтягивает карточки `emitters/{EMITTER_ID}` (наименование/ИНН). Для полей тикеров/ISIN поддерживается fallback по `ISSUER_ID` и нормализация `EMITTER_ID` (в т.ч. `123.0`).
- Вход: список допущенных облигаций, `MoexClient`, `ScreenerStateStore`.
- Выход: список строк для `output/emitents.xlsx`; статичные поля эмитента кэшируются в `state/emitents_registry.json`, а сопоставления `SECID -> EMITTER_ID` — в `state/secid_to_emitter.json`; в результат возвращаются таймеры подэтапов (`карточки`, `рынок bonds`, `рынок shares`).


## `moex_bond_screener/data_quality.py`
- Назначение: расчет `DATA_STATUS` (`ok`/`warning`/`error`) и причины качества данных для итоговых облигаций.
- Вход: список словарей облигаций.
- Выход: поля `DATA_STATUS` и `DATA_STATUS_REASON` в каждой записи.

## `moex_bond_screener/logging_utils.py`
- Назначение: настраивает логирование в файл и консоль.
- Вход: нет.
- Выход: объект `Logger`.


## `moex_bond_screener/progress.py`
- Назначение: человеко-понятный прогресс запуска (этапы, проценты, ETA).
- Вход: номера этапов и счетчики длинных операций.
- Выход: строки прогресса в консоли пользователя.


## `moex_bond_screener/dohod_enrichment.py`
Назначение: параллельное обогащение облигаций данными с analytics.dohod.ru с чекпоинтами и кешем на сутки.

Вход:
- список бумаг после базовой фильтрации (для запроса в ДОХОД используется идентификатор бумаги: сначала `ISIN`, затем fallback на `SECID`);
- checkpoint `state/checkpoints/dohod_enrichment` (или в SQLite).

Выход:
- `RealPrice`;
- обновленный `COUPONPERCENT` (если был пустой);
- обновленный `OFFERDATE` (если был пустой и событие не погашение);
- служебный флаг `_COUPONPERCENT_APPROX` для подсветки в Excel.


## `moex_bond_screener/ytm.py`
- Назначение: рассчитывает `YTM` для всех бумаг после этапа обогащения ДОХОД.
- Вход: список облигаций с `RealPrice`, `MATDATE`, опционально `FACEVALUE`, `ACCRUEDINT`, `COUPONPERCENT`.
- Выход: поле `YTM` (в процентах годовых) и статистика рассчитанных/пропущенных бумаг.
- Особенность: для бумаг с `COUPONPERCENT < 1` используется формула бескупонной облигации; `ACCRUEDINT` всегда учитывается в dirty price.
