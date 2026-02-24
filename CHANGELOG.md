# CHANGELOG

## 2026-02-24
- Исправлен фильтр OFZ-PK: больше не исключаются все ОФЗ по шаблону `SU*RMFS*`, исключаются только выпуски с признаками плавающего купона/серии `SU29***`.
- Исправлено чтение derived-полей в скринере: теперь используются `next_offer_date` и `amort_start_date`, поэтому оферты и амортизация корректно попадают в фильтры.
- Добавлен фильтр `maturity_in_past` для бумаг с датой погашения в прошлом.
- Улучшен парсинг дат MOEX (`YYYY-MM-DD HH:MM:SS`/`YYYY-MM-DDTHH:MM:SS`) для оферт и derived-полей.
- Исправлена классификация амортизационных выплат: одиночная частичная амортизация больше не теряется как `redemption`.
- Обновлен запуск по умолчанию: `python run.py` теперь выполняет весь пайплайн `sync_moex_universe -> sync_moex_cashflows -> screen_basic`.
- Добавлены pytest-тесты на новые правила фильтрации, парсинг оферт/амортизации и оркестрацию `run.py`.
- Оптимизирован `sync_moex_cashflows`: один HTTP-запрос на бумагу (cashflows+offers) и пакетная запись в SQLite вместо коммитов по каждой бумаге; это заметно ускоряет повторные запуски.
- Исправлен расчет `maturity_date`: теперь приоритет у дат погашения/амортизации, а купоны не сдвигают дату погашения на дальние годы.
- Добавлен парсинг оферт MOEX (`offers`/`putoffers`) с сохранением в таблицу `offers` и derived-полем `next_offer_date`.
- В `sync_moex_cashflows.py` добавлена выгрузка `out/offers_sample.xlsx` и сохранение оферт в БД.
- Добавлен sync cashflows из MOEX bondization: сохранение `coupon`/`amort`/`redemption` в таблицу `cashflows`.
- Добавлен расчет и сохранение derived-полей (`maturity_date`, `next_coupon_date`, `amort_start_date`, `has_amortization`) в `instrument_fields`.
- Добавлен скрипт `scripts/sync_moex_cashflows.py` с параллелизмом из конфига, кэшем 24 часа и выгрузкой sample-файлов `out/cashflows_sample.xlsx`, `out/derived_sample.xlsx`.
- Добавлены pytest-тесты на парсинг cashflows, расчет derived-полей и запись в БД.
- Инициализирована структура проекта `bond_screener` с запуском через `scripts/run.py`.
- Добавлено автосоздание конфигов и рабочих директорий (`config/`, `out/`, `logs/`, `raw/`).
- Добавлена очистка `raw/` по TTL и перезапись `logs/latest.log` на каждом запуске.
- Добавлен pytest-тест на автосоздание конфигов в временной директории.
- Добавлена базовая документация на русском в `docs/` и `README.md`.
- Добавлена SQLite-схема на SQLAlchemy (`instruments`, `issuers`, `cashflows`, `offers`, `ratings`, `publications`, `snapshots` и поля-расширения).
- Добавлен скрипт `scripts/db_inspect.py` для печати статистики по таблицам БД.
- Добавлен pytest-тест на создание БД, вставку и чтение данных.

