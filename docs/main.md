# Скрипт `main.py` — загрузка MOEX bonds + Доходъ bonds + рейтинги НРА/АКРА/НКР/RAEX + витрина эмитентов

## Что делает скрипт
1. Загружает CSV с MOEX по ссылке из `config.py`.
2. Сохраняет файл в папки:
   - `raw/moex_bonds.csv` (основной «сырой» файл),
   - `cache/moex_bonds_cache.csv` (кэш-копия).
3. Обновляет SQLite-базу `DB/bonds.sqlite3` (таблица `moex_bonds`) полной перезаливкой при истекшем TTL.
4. Загружает Excel-выгрузку облигаций с `https://www.dohod.ru/analytic/bonds` через Playwright:
   - нажимает кнопку `Скачать Excel`,
   - получает файл через `expect_download` или `blob:` ссылку,
   - сохраняет raw в `raw/dohod_bonds.xlsx` (перезапись при истекшем TTL),
   - инкрементально обновляет таблицу `Dohod_Bonds` в основной БД,
   - TTL 12 часов (`DOHOD_CACHE_TTL_HOURS`),
   - формирует снапшот `dohod_bonds_snapshot.xlsx`.
5. Загружает Excel-выгрузку рейтингов НРА со страницы `https://www.ra-national.ru/ratings/`:
   - автоматически находит ссылку на `.xlsx/.xls` на странице,
   - сохраняет оригинальный файл в `raw/nra_ratings.xlsx`,
   - обновляет отдельную SQLite-базу `DB/raitings.sqlite3`,
   - работает с TTL 12 часов (настраивается `NRA_CACHE_TTL_HOURS`).
6. Парсит рейтинги АКРА методом из `Acra.py` (через `playwright.sync_api` + запуск **Google Chrome** + паузы/ретраи):
   - первый этап (список) обновляется не чаще одного раза в 12 часов (`ACRA_CACHE_TTL_HOURS`),
   - извлекаются: ссылка на карточку, наименование, рейтинг, прогноз и дата,
   - карточки открываются в persistent-профиле браузера (`ACRA_PROFILE_DIR`) для устойчивости к антибот-защите,
   - обновления инкрементальные в таблицу `acra_ratings`.
7. Для АКРА реализована оптимизация карточек:
   - если ссылка эмитента уже есть в БД и по ней уже сохранен ИНН, карточка повторно не запрашивается,
   - карточка запрашивается только для новых эмитентов (или если по ссылке ранее не удалось получить ИНН).
8. В отдельной БД рейтинговых агентств:
   - НРА хранится в `nra_ratings` и `nra_latest_by_inn`,
   - АКРА хранится в `acra_ratings` с уникальностью `issuer_url + rating_date + rating` (отдельно сохраняются `rating` и `forecast`),
   - НКР хранится в `nkr_ratings` и `nkr_latest_by_tin` (уникальность истории по `tin + rating_date + rating + outlook`),
   - RAEX хранится в `raex_ratings` и `raex_latest_by_inn` (только актуальные национальные рейтинги из раздела `Рейтинги компании`).
9. Ведет таблицу `emitents` с уникальными эмитентами по `INN` в основной БД:
   - берет поля `EMITENTNAME` и `INN` из `moex_bonds`,
   - инкрементально делает upsert,
   - хранит пользовательские поля `Scoring`, `DateScoring` и столбцы `NRA_Rate`, `Acra_Rate`, `NKR_Rate`, `RAEX_Rate`.
10. Обогащает витрину эмитентов рейтингами НРА:
   - матчинг по `INN`,
   - формат значения: `Рейтинг(прогноз)`.
11. Обогащает витрину эмитентов рейтингами АКРА:
   - матчинг по `INN`,
   - сохраняет в `Acra_Rate`,
   - формат `Рейтинг(прогноз)` при наличии прогноза, иначе только рейтинг.
12. Загружает Excel-выгрузку НКР через Playwright со страницы `https://ratings.ru/ratings/issuers/`:
   - нажимает кнопку `Выгрузить в Excel`,
   - сначала пытается скачать через событие браузерной загрузки, затем по прямому `href`, и только после этого — через `blob:` ссылку,
   - в БД пишет только нужные поля: `Date`, `Rating`, `Outlook`, `TIN`,
   - делает инкрементальное обновление истории в `nkr_ratings`,
   - формирует `nkr_latest_by_tin` (уникально по ИНН с самой свежей датой).
13. Обогащает витрину эмитентов рейтингами НКР по `TIN == INN` в колонку `NKR_Rate`.
14. Парсит RAEX (Эксперт РА) по ИНН из таблицы `emitents`:
   - входная точка поиска: `https://raexpert.ru/search/`,
   - использует `requests.Session()` + CSRF-токен `CSRFToken` из HTML формы,
   - выполняет `POST /search/` с `search=<ИНН>`, затем открывает карточку `/database/companies/.../`,
   - парсит **только** раздел `Рейтинги компании` (архив не используется),
   - из таблицы `Национальная шкала / Прогноз / Дата` берет первую строку,
   - нормализует рейтинг `ruAAA -> AAA`,
   - если рейтинг отозван/пустой или раздел отсутствует — актуальный рейтинг не сохраняется,
   - обновляет таблицы `raex_ratings` (история) и `raex_latest_by_inn` (актуальные),
   - TTL 12 часов (`RAEX_CACHE_TTL_HOURS`), сбор параллелится (`RAEX_MAX_WORKERS`).
15. Обогащает витрину эмитентов рейтингами RAEX по `INN` в колонку `RAEX_Rate`.
16. На длительном RAEX-парсинге показывает отдельный прогресс-бар `RAEX INN`:
   - бар отображает обработку каждого ИНН,
   - основной бар этапа 5 продолжает жить отдельной строкой,
   - визуально этап больше не выглядит «зависшим».
17. Синхронизирует ручные оценки из Excel-витрины `Emitents.xlsx` обратно в SQL.
18. Автоматически заполняет `DateScoring`, когда есть `Scoring`, а дата еще пустая.
19. Формирует Excel-витрину `Emitents.xlsx` в корне проекта с оформлением и валидацией `Scoring`.
20. Создает снапшоты в `BaseSnapshots`:
   - `moex_bonds_snapshot.xlsx` (5 случайных строк с уникальными `SECID`),
   - `dohod_bonds_snapshot.xlsx` (5 случайных строк с уникальными `ISIN`),
   - `emitents_snapshot.xlsx` (5 случайных строк из `emitents`),
   - `nra_snapshot.xlsx` (**5 самых свежих** строк по дате рейтинга),
   - `acra_snapshot.xlsx` (**5 самых свежих** строк по дате рейтинга),
  - `nkr_snapshot.xlsx` (**5 самых свежих** строк по дате рейтинга),
  - `raex_snapshot.xlsx` (**5 самых свежих** строк по дате рейтинга RAEX).
21. Создает merge-таблицы в `DB/bonds.sqlite3` на основе `Scoring` эмитента и матчинга по `ISIN`:
   - `MergeGreenBonds` — облигации эмитентов, где `Scoring = Green`,
   - `MergeYellowBonds` — облигации эмитентов, где `Scoring = Yellow`,
   - структура включает выбранные поля из `moex_bonds` и `Dohod_Bonds`,
   - для каждой merge-таблицы формируется отдельный snapshot по 5 случайных строк.
22. Пишет технический лог в `logs/main.log` (перезаписывается каждый запуск).

## Запуск
```bash
python main.py
```

> Без CLI/argparse: просто запуск `main.py`.

## Что выводится в консоль
В консоль выводятся только этапы, прогресс-бары и итоговый summary по времени.
Техническая детализация уходит в `logs/main.log`.

## Логика НРА
- Источник: страница `NRA_RATINGS_PAGE_URL`.
- TTL загрузки: `NRA_CACHE_TTL_HOURS` (по умолчанию 12 часов).
- Raw-файл: `raw/nra_ratings.xlsx`.
- Отдельная БД: `DB/raitings.sqlite3`.
- Таблица для витрины НРА: `nra_latest_by_inn`.

## Логика АКРА
- Источник: `ACRA_RATINGS_LIST_URL`.
- Драйвер: Playwright (`playwright.sync_api`) c `p.chromium.launch_persistent_context(..., channel="chrome")`, как в оригинальном `Acra.py`.
- TTL первого этапа (список): `ACRA_CACHE_TTL_HOURS` (по умолчанию 12 часов).
- БД: `DB/raitings.sqlite3`.
- Таблица: `acra_ratings`.
- Поля записи: `issuer_url`, `issuer_name`, `rating`, `forecast`, `rating_date`, `inn`, `loaded_at_utc`.
- Если ссылка известна и ИНН по ней уже есть в БД, карточка эмитента не парсится повторно.
- Если в локальном `raw/acra_dump/issuers_list.html` есть прогнозы, они используются для backfill пустого `forecast` в SQLite без повторной загрузки сайта.
- Для нестабильных соединений используются ретраи `goto` и "человеческие" паузы между действиями.
- Дополнительно формируются дампы: `raw/acra_dump/issuers_list.html`, `raw/acra_dump/issuers_list.mhtml`, карточки в `raw/acra_dump/issuers/`, прогресс в `raw/acra_dump/progress.jsonl`.


## Логика НКР
- Источник: `NKR_RATINGS_PAGE_URL` (страница списка эмитентов НКР).
- Драйвер: Playwright, многошаговое скачивание (download event -> direct href -> blob URL).
- TTL загрузки: `NKR_CACHE_TTL_HOURS` (по умолчанию 12 часов).
- Raw-файл: `raw/nkr_ratings.xlsx`.
- БД: `DB/raitings.sqlite3`.
- История: `nkr_ratings`, агрегат последних значений: `nkr_latest_by_tin`.
- В витрину переносится `NKR_Rate` в формате `Рейтинг(прогноз)` при наличии прогноза.

## Логика RAEX
- Источник: `RAEX_SEARCH_URL` (`https://raexpert.ru/search/`).
- Поиск по ИНН: `POST /search/` с параметром `search=<ИНН>` и `CSRFToken`.
- Обязательное использование cookie-сессии через `requests.Session()`.
- Парсинг карточки эмитента: только блок `Рейтинги компании`, архив игнорируется.
- Взятие данных: первая строка таблицы рейтингов, где есть колонки `Прогноз` и `Дата`, а колонка рейтинга может называться `Национальная шкала` или `Шкала Эксперт РА`.
- Нормализация рейтинга: удаляется префикс `ru`.
- При отсутствии актуального значения (пусто/отозван) запись по ИНН не обновляется.
- TTL: `RAEX_CACHE_TTL_HOURS` (по умолчанию 12 часов); если `raex_latest_by_inn` пустая, выполняется принудительное обновление RAEX независимо от TTL.
- Производительность: параллельные запросы по ИНН (`RAEX_MAX_WORKERS`).
- Оптимизация повторных прогонов RAEX: если в `raex_latest_by_inn` уже есть `company_url` для ИНН, скрипт сразу открывает карточку эмитента и не выполняет этап поиска `/search/`.
- Snapshot: `BaseSnapshots/raex_snapshot.xlsx` (5 самых свежих записей).

## Настройка
Все настройки находятся в `config.py`.
Каждый параметр снабжен комментарием: назначение, допустимые значения и дефолт.

Ключевые параметры:
- `CACHE_TTL_HOURS` — срок жизни кэша MOEX bonds в часах.
- `NRA_CACHE_TTL_HOURS` — срок жизни кэша НРА в часах.
- `ACRA_CACHE_TTL_HOURS` — срок жизни кэша этапа списка АКРА в часах.
- `REQUEST_TIMEOUT_SECONDS` — таймаут HTTP.
- `ACRA_PROFILE_DIR` — папка persistent-профиля браузера для АКРА (по умолчанию `cache/acra_profile`).
- `ACRA_BROWSER_CHANNEL` — канал браузера (`"chrome"`, `"msedge"` или `None`).
- `ACRA_HEADLESS` — headless-режим для АКРА.
- `ACRA_LIST_GOTO_ATTEMPTS` / `ACRA_CARD_GOTO_ATTEMPTS` — число retry для переходов.
- `ACRA_DUMP_DIR` / `ACRA_ISSUERS_DUMP_DIR` — пути для HTML/MHTML дампов и карточек АКРА (по умолчанию внутри `raw/acra_dump`).
- `ACRA_PROGRESS_LOG_FILENAME` — имя JSONL-лога прогресса по карточкам.
- `NRA_RATINGS_PAGE_URL` — страница НРА с кнопкой выгрузки.
- `ACRA_RATINGS_LIST_URL` — страница списка эмитентов АКРА.
- `NKR_RATINGS_PAGE_URL` — страница списка эмитентов НКР.
- `NKR_CACHE_TTL_HOURS` — срок жизни кэша НКР в часах.
- `NKR_EXPORT_BUTTON_SELECTOR` — CSS-селектор кнопки выгрузки НКР.
- `NKR_DOWNLOAD_ATTEMPTS` — количество попыток скачивания выгрузки НКР.
- `NKR_TABLE_NAME` / `NKR_LATEST_TABLE_NAME` — таблицы НКР в БД рейтинговых агентств.
- `RAEX_SEARCH_URL` — URL поиска RAEX.
- `RAEX_CACHE_TTL_HOURS` — TTL обновления RAEX в часах.
- `RAEX_MAX_WORKERS` — число потоков параллельного парсинга RAEX (дефолт увеличен до 12).
- `EMITENTS_RATINGS_COLUMN_WIDTH` — фиксированная ширина колонок рейтингов в `Emitents.xlsx` (`NRA_Rate`, `Acra_Rate`, `NKR_Rate`, `RAEX_Rate`), по умолчанию `22.09` (примерно `250 px`).
- `RAEX_TABLE_NAME` / `RAEX_LATEST_TABLE_NAME` — таблицы RAEX в БД рейтинговых агентств.
- `RAITINGS_DB_FILENAME` — файл общей SQLite-базы рейтинговых агентств.
- `ACRA_TABLE_NAME` — таблица АКРА в БД рейтинговых агентств.
- `PRESORTER_MIN_DAYS_TO_EVENT` — порог для исключения облигаций на этапе Presorter по датам погашения/оферты.
- `PRESORTER_EXCLUDED_BOND_TYPE` — значение `BOND_TYPE`, которое исключается из Merge-таблиц на этапе Presorter.

## Примечание для Windows
Проект ориентирован на Windows-сценарий:
- пути собираются через `pathlib.Path`,
- лог/файлы пишутся в UTF-8,
- запуск без дополнительных CLI-аргументов.


## Логика Доходъ
- Источник: `DOHOD_BONDS_PAGE_URL` (страница с кнопкой `Скачать Excel`).
- Драйвер: Playwright (download event + fallback на `blob:`).
- TTL загрузки: `DOHOD_CACHE_TTL_HOURS` (по умолчанию 12 часов).
- Raw-файл: `raw/dohod_bonds.xlsx`.
- БД: `DB/bonds.sqlite3` (основная база).
- Таблица: `Dohod_Bonds` (инкрементальный upsert по `ISIN`).
- Перед записью выполняется дедупликация по `ISIN`: если в выгрузке встречается несколько строк с одним `ISIN`, итоговая строка собирается как объединение непустых полей (чтобы поздняя «пустая» строка не затирала ранее загруженные данные).
- `ISIN` нормализуется (trim + uppercase) перед сохранением.
- Snapshot: `BaseSnapshots/dohod_bonds_snapshot.xlsx` (5 случайных уникальных ISIN).


## Логика MergeGreenBonds / MergeYellowBonds
- БД: `DB/bonds.sqlite3` (основная база).
- Источники:
  - `moex_bonds` — поля: `SECID`, `ISIN`, `FACEVALUE`, `FACEUNIT`, `MATDATE`, `IS_QUALIFIED_INVESTORS`, `BOND_TYPE`, `BOND_SUBTYPE`, `YIELDATWAP`, `PRICE`;
  - `Dohod_Bonds` — поля: `Название`, `Ближайшая дата погашения/оферты (Дата)`, `Событие в дату`, `Коэф. Ликвидности (max=100)`, `Медиана дневного оборота (млн в валюте торгов)`, `Цена, % от номинала`, `НКД`, `Размер купона`, `Текущий купон, %`, `Тип купона`, `Купон (раз/год)`, `Субординированная (да/нет)`, `Базовый индекс (для FRN)`, `Премия/Дисконт к базовому индексу (для FRN)`.
- Матчинг:
  - фильтрация по `emitents.Scoring` (`Green`/`Yellow`) через связь `moex_bonds.INN -> emitents.INN`;
  - объединение с `Dohod_Bonds` по `ISIN` (LEFT JOIN).
- Таблицы пересобираются каждый запуск этапа merge (очистка + новая вставка).
- После пересборки выполняется этап **Presorter**: из `MergeGreenBonds` и `MergeYellowBonds` удаляются бумаги, если выполняется хотя бы одно условие:
  - до `MATDATE` меньше `PRESORTER_MIN_DAYS_TO_EVENT` дней;
  - до `Ближайшая дата погашения/оферты (Дата)` меньше `PRESORTER_MIN_DAYS_TO_EVENT` дней;
  - `BOND_TYPE` равен `PRESORTER_EXCLUDED_BOND_TYPE` (по умолчанию `Структурная облигация`); сравнение выполняется после нормализации пробелов (включая неразрывные) и без учета регистра.
- В консольном Summary добавляется блок `Этап Presorter` с отдельной статистикой по `MergeGreen` и `MergeYellow`:
  - `Исключено бумаг по правилам меньше 365 дней` (объединенный счетчик по `MATDATE` и `Ближайшая дата погашения/оферты (Дата)`);
  - `Исключено бумаг по правилу Bond_TYPE`.
- Snapshot-файлы (формируются **после** Presorter, поэтому содержат только прошедшие фильтр бумаги; также это выборка только из 5 случайных ISIN, а не полный состав таблиц):
  - `BaseSnapshots/merge_green_bonds_snapshot.xlsx`;
  - `BaseSnapshots/merge_yellow_bonds_snapshot.xlsx`.
