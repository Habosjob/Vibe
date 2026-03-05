# Скрипт `main.py` — загрузка MOEX bonds + Доходъ bonds + рейтинги НРА/АКРА/НКР/RAEX + витрина эмитентов + обогащение Corpbonds/MOEX Amortizations/Smartlab

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
22. Обогащает Merge-таблицы амортизациями MOEX только для бумаг, где `Corpbonds_Наличие амортизации = Да`:
   - источник: `https://iss.moex.com/.../bondization/<SECID>.json?iss.only=amortizations&iss.meta=off`,
   - кэш и инкрементальные обновления по `SECID` с TTL 12 часов (`MOEX_AMORTIZATION_CACHE_TTL_HOURS`),
   - сохраняет историю в таблицу `MoexAmortizations`,
   - находит минимальную дату амортизации и записывает ее в `AmortStarrtDate` таблиц `MergeGreenBonds`/`MergeYellowBonds`,
   - формирует snapshot `BaseSnapshots/moex_amortizations_snapshot.xlsx` (все строки по 5 случайным `SECID`).
23. Пишет технический лог в `logs/main.log` (перезаписывается каждый запуск).

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
- `PRESORTER_MIN_DAYS_TO_EVENT` — порог для исключения облигаций на этапе Presorter по датам `MATDATE` / `Offerdate` / `AmortStarrtDate` (в днях).
- `PRESORTER_EXCLUDED_BOND_TYPE` — значение `BOND_TYPE`, которое исключается из Merge-таблиц на этапе Presorter.
- `PRESORTER_USE_DOHOD_NEAREST_DATE` — включать отдельное правило Presorter по полю `Ближайшая дата погашения/оферты (Дата)` из Доходъ (`True` по умолчанию).
- `MERGE_REQUIRE_DOHOD_ISIN_MATCH` — требовать наличие бумаги одновременно в MOEX и Доходъ при сборке Merge (`True` по умолчанию).
- `CORPBONDS_CACHE_TTL_HOURS` / `CORPBONDS_MAX_WORKERS` / `CORPBONDS_REQUEST_TIMEOUT_SECONDS` — TTL, параллелизм и таймаут этапа Corpbonds.
- `MERGE_REQUIRE_CORPBONDS_SECID_MATCH` — режим JOIN Merge* c Corpbonds (`INNER` при `True`, `LEFT` при `False`).
- `MOEX_AMORTIZATION_CACHE_TTL_HOURS` / `MOEX_AMORTIZATION_MAX_WORKERS` / `MOEX_AMORTIZATION_REQUEST_TIMEOUT_SECONDS` — TTL, параллелизм и таймаут этапа MOEX Amortizations.
- `SMARTLAB_CACHE_TTL_HOURS` / `SMARTLAB_MAX_WORKERS` / `SMARTLAB_REQUEST_TIMEOUT_SECONDS` — TTL, параллелизм и таймаут этапа Smartlab.
- `MERGE_REQUIRE_SMARTLAB_SECID_MATCH` — режим JOIN Merge* c Smartlab (`INNER` при `True`, `LEFT` при `False`).
- `SCREENER_TABLE_NAME` — имя SQL-таблицы единого скринера (объединение MergeGreenBonds + MergeYellowBonds).
- `SCREENER_XLSX_FILENAME` — имя Excel-витрины скринера (листы `Green` и `Yellow`).
- `YTM_OUTPUT_PRECISION` — число знаков после запятой при записи YTM в Screener (дефолт `4`).
- `YTM_SELFCHECK_ENABLED` / `YTM_SELFCHECK_STRICT` — включение и строгость встроенной проверки корректности cashflow/NPV для YTM.

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


## Логика обогащения Corpbonds (этап 9)
- Источник: `https://corpbonds.ru/bond/<SECID>` (используется именно `SECID`).
- Входные данные: уникальные `SECID` из таблиц `MergeGreenBonds` и `MergeYellowBonds`.
- Что собирается по каждой бумаге:
  - `Цена последняя`;
  - `Тип купона`;
  - `Формула купона` (если на странице указана как `Формула купона` или `Формула флоатера`);
  - `Дата ближайшего купона`;
  - `Дата ближайшей оферты`;
  - `Наличие амортизации`;
  - `Купон лесенкой`.
- Куда сохраняется: `DB/bonds.sqlite3`, таблица `CorpbondsBonds`.
- Режим обновления: инкрементальный upsert по `SECID` (первичный ключ).
- TTL: `CORPBONDS_CACHE_TTL_HOURS` (по умолчанию 12 часов). Если запись по `SECID` свежее TTL, повторный HTTP-запрос не выполняется.
- Производительность:
  - многопоточная загрузка (`ThreadPoolExecutor`) с числом потоков `CORPBONDS_MAX_WORKERS`;
  - HTTP-сессия переиспользуется внутри каждого потока (меньше накладных расходов на TLS/соединения);
  - HTTP-таймаут настраивается `CORPBONDS_REQUEST_TIMEOUT_SECONDS`.
- После обновления `CorpbondsBonds` данные переносятся в `MergeGreenBonds` и `MergeYellowBonds`:
  - маппинг идет строго по `SECID`;
  - колонки добавляются в Merge-таблицы с префиксом `Corpbonds_`;
  - режим матчинга управляется `MERGE_REQUIRE_CORPBONDS_SECID_MATCH`:
    - `True` (по умолчанию) — `INNER JOIN` (в Merge остаются только бумаги, у которых `SECID` найден в Corpbonds);
    - `False` — `LEFT JOIN` (строки Merge сохраняются, Corpbonds-колонки заполняются при наличии совпадения).
- Snapshot: `BaseSnapshots/corpbonds_snapshot.xlsx` (5 случайных уникальных `SECID`).


## Логика обогащения MOEX Amortizations (этап 10)
- Источник: MOEX ISS `.../bondization/<SECID>.json?iss.only=amortizations&iss.meta=off`.
- Входные данные: уникальные `SECID` из Merge* c `Corpbonds_Наличие амортизации = Да`.
- Куда сохраняется: `DB/bonds.sqlite3`, таблица `MoexAmortizations`.
- Режим обновления: инкрементальный (перезапрос только просроченных по TTL `SECID`).
- TTL: `MOEX_AMORTIZATION_CACHE_TTL_HOURS` (по умолчанию 12 часов).
- Производительность: многопоточная загрузка (`MOEX_AMORTIZATION_MAX_WORKERS`) + keep-alive с retry.
- Результат в Merge*: заполняется `AmortStarrtDate` самой ранней датой амортизации по `SECID`.
- Snapshot: `BaseSnapshots/moex_amortizations_snapshot.xlsx` (все строки для 5 случайных `SECID`).

## Логика обогащения Smartlab (этап 11)
- Источник: `https://smart-lab.ru/q/bonds/<SECID>/` (используется именно `SECID`).
- Входные данные: уникальные `SECID` из таблиц `MergeGreenBonds` и `MergeYellowBonds`.
- Что собирается по каждой бумаге:
  - `Котировка облигации, %`;
  - `Изм за день, %`;
  - `Объем день, млн. руб`;
  - `Объем день, штук`;
  - `Дата оферты`;
  - `Только для квалов?`;
  - `Длительность купона, дней`.
- Куда сохраняется: `DB/bonds.sqlite3`, таблица `SmartlabBonds`.
- Режим обновления: инкрементальный upsert по `SECID` (первичный ключ).
- TTL: `SMARTLAB_CACHE_TTL_HOURS` (по умолчанию 12 часов). Если запись по `SECID` свежее TTL, повторный HTTP-запрос не выполняется.
- Производительность:
  - многопоточная загрузка (`ThreadPoolExecutor`) с числом потоков `SMARTLAB_MAX_WORKERS`;
  - HTTP-сессия переиспользуется внутри каждого потока;
  - HTTP-таймаут настраивается `SMARTLAB_REQUEST_TIMEOUT_SECONDS`.
- После обновления `SmartlabBonds` данные переносятся в `MergeGreenBonds` и `MergeYellowBonds`:
  - маппинг идет строго по `SECID`;
  - колонки добавляются в Merge-таблицы с префиксом `Smartlab_`;
  - режим матчинга управляется `MERGE_REQUIRE_SMARTLAB_SECID_MATCH`:
    - `True` (по умолчанию) — `INNER JOIN`;
    - `False` — `LEFT JOIN`.
- Snapshot: `BaseSnapshots/smartlab_snapshot.xlsx` (5 случайных уникальных `SECID`).

## Логика MergeGreenBonds / MergeYellowBonds
- БД: `DB/bonds.sqlite3` (основная база).
- Источники:
  - `moex_bonds` — поля: `SECID`, `ISIN`, `FACEVALUE`, `FACEUNIT`, `MATDATE`, `IS_QUALIFIED_INVESTORS`, `BOND_TYPE`, `BOND_SUBTYPE`, `YIELDATWAP`, `PRICE`;
  - `Dohod_Bonds` — поля: `Название`, `Ближайшая дата погашения/оферты (Дата)`, `Событие в дату`, `Коэф. Ликвидности (max=100)`, `Медиана дневного оборота (млн в валюте торгов)`, `Цена, % от номинала`, `НКД`, `Размер купона`, `Текущий купон, %`, `Тип купона`, `Купон (раз/год)`, `Субординированная (да/нет)`, поля FRN-индекса/премии исключены из витрины Screener и не участвуют в расчётах YTM.
  - в Merge-таблицах поле `Цена, % от номинала` сохраняется под новым именем `Цена Доход`.
- Матчинг:
  - фильтрация по `emitents.Scoring` (`Green`/`Yellow`) через связь `moex_bonds.INN -> emitents.INN`;
  - объединение с `Dohod_Bonds` по `ISIN` в режиме из `MERGE_REQUIRE_DOHOD_ISIN_MATCH`:
    - `True` (по умолчанию) — только пересечение источников (INNER JOIN),
    - `False` — все бумаги из MOEX с дозаполнением колонок Доходъ при совпадении (LEFT JOIN).
- Таблицы пересобираются каждый запуск этапа merge (очистка + новая вставка).
- После пересборки выполняется этап **Presorter**.
- После обогащения `Corpbonds`/`MOEX Amortizations`/`Smartlab` этап **Presorter** запускается повторно (чтобы удалить бумаги, отфильтрованные по `Offerdate`/`AmortStarrtDate`, которые появляются именно после обогащения).
- На каждом запуске Presorter из `MergeGreenBonds` и `MergeYellowBonds` удаляются **строки бумаг**, если выполняется хотя бы одно условие:
  - до `MATDATE` меньше `PRESORTER_MIN_DAYS_TO_EVENT` дней;
  - до `Ближайшая дата погашения/оферты (Дата)` меньше `PRESORTER_MIN_DAYS_TO_EVENT` дней (отдельное правило; применяется при `PRESORTER_USE_DOHOD_NEAREST_DATE = True`);
  - до `Offerdate` меньше `PRESORTER_MIN_DAYS_TO_EVENT` дней (дата берется из `Corpbonds_Дата ближайшей оферты`, при пустом значении — из `Smartlab_Дата оферты`);
  - до `AmortStarrtDate` меньше `PRESORTER_MIN_DAYS_TO_EVENT` дней (включая даты, которые уже в прошлом);
  - `BOND_TYPE` равен `PRESORTER_EXCLUDED_BOND_TYPE` (по умолчанию `Структурная облигация`); сравнение выполняется после нормализации пробелов (включая неразрывные) и без учета регистра.
- В консольном Summary добавляются блоки `Этап Presorter` и `Этап Presorter (после обогащений)` с отдельной статистикой по `MergeGreen` и `MergeYellow`:
  - `Строк до/после Presorter`;
  - `Исключено бумаг по правилу MATDATE < N дней`;
  - `Исключено бумаг по правилу Доходъ (ближайшая дата) < N дней`;
  - `Исключено бумаг по правилу Bond_TYPE`;
  - `Исключено бумаг по правилу Offerdate < N дней`;
  - `Исключено бумаг по правилу AmortStarrtDate < N дней`.
- Snapshot-файлы (пересохраняются на этапе Corpbonds, то есть отражают финальное состояние Merge после Presorter **и** после обогащения `Corpbonds_*`; это выборка только из 5 случайных ISIN, а не полный состав таблиц):
  - `BaseSnapshots/merge_green_bonds_snapshot.xlsx`;
  - `BaseSnapshots/merge_yellow_bonds_snapshot.xlsx`.

## Логика Screener (единая таблица + Excel витрина)
- SQL-таблица: `Screener` (имя управляется `SCREENER_TABLE_NAME`).
- Источники: `MergeGreenBonds` и `MergeYellowBonds` после всех этапов обогащения и Presorter.
- Правила построения:
  - все строки из `MergeGreenBonds` получают `Score=1`, `SourceList=Green`;
  - все строки из `MergeYellowBonds` получают `Score=0`, `SourceList=Yellow`;
  - `QUALIFIED` =
    - `1`, если `IS_QUALIFIED_INVESTORS=1` (значение Smartlab игнорируется);
    - иначе `1`, если `Smartlab_Только для квалов?` = `Да`/`1`;
    - иначе `0`;
  - `Offerdate` = дата из `Corpbonds_Дата ближайшей оферты`, а если она пустая/невалидная — дата из `Smartlab_Дата оферты`;
  - все даты нормализуются к формату `YYYY-MM-DD`; если значение не дата, в поле пишется пусто;
  - `YTM` рассчитывается для подэтапа «Считаем фиксы» по правилам:
    - `Тип купона` должен быть фиксированным (`Фикс*`);
    - цена берется по приоритету источников: `Цена Corpbonds` → `Цена Доход` → `Цена Smartlab` → `Цена MOEX`;
    - цена покупки считается напрямую: `dirty_price = FACEVALUE * (price / 100) + НКД`, где `price` — процентная котировка;
    - дата расчета до события: если `Offerdate` заполнена, считаем до нее; иначе до `MATDATE`;
    - `Купон, %` интерпретируется как целое процентное значение (например `18` => `0.18` в формуле);
    - для обычных выпусков cashflow строится по реальным датам: купоны только на купонные даты (`next_coupon_date + КупонПериод`), тело долга — отдельным потоком на `target_date`;
    - купон в `target_date` добавляется только если `target_date` совпадает с купонной датой (допуск ±1 день); иначе в `target_date` платится только тело долга;
    - если `Суборд = 1/Да/галочка`, используется модель «текущая купонная доходность + эффект сложного процента» (без учета дохода/убытка от возврата номинала);
    - если `Аморт = 1/Да/галочка`, YTM считается через общий IRR по потокам: купоны на купонные даты + амортизационные выплаты по `MoexAmortizations` + остаток тела на `target_date`.
- Excel-витрина: `Screener.xlsx` (имя управляется `SCREENER_XLSX_FILENAME`) с листами `Green` и `Yellow`.
- По умолчанию строки в Excel сортируются по `AmortStarrtDate` (от ранней даты к поздней), пустые/невалидные даты переносятся в конец; режим управляется флагом `SCREENER_SORT_BY_AMORT_START_DATE`.
- Оформление листов аналогично `Emitents.xlsx`:
  - жирный заголовок + цвет шапки;
  - автофильтр;
  - freeze panes на первой строке данных;
  - автоширина колонок.
- В колонках `Квал`, `Суборд`, `Аморт`, `Лесенка` используются символы `✅/❌` вместо `1/0` и `Да/Нет`.
- Даты в колонках `AmortStarrtDate`, `MATDATE`, `Offerdate`, `Ближайший купон` в Excel записываются как тип `date` (формат `yyyy-mm-dd`), поэтому сортировка и групповые фильтры работают корректно по календарю.
- В колонке `Ликвидность` значения приводятся к числу и включен градиент DataBar (чем выше число, тем длиннее заливка).

### Карта переименований полей для Screener-витрины
- `QUALIFIED` → `Квал`
- `Субординированная (да/нет)` → `Суборд`
- `Corpbonds_Наличие амортизации` → `Аморт`
- `Corpbonds_Купон лесенкой` → `Лесенка`
- `Corpbonds_Дата ближайшего купона` → `Ближайший купон`
- `Corpbonds_Тип купона` → `Тип купона`
- `Smartlab_Длительность купона, дней` → `КупонПериод`
- `Текущий купон, %` → `Купон, %`
- `Corpbonds_Формула купона` → `Формула купона`
- `Коэф. Ликвидности (max=100)` → `Ликвидность`
- `Corpbonds_Цена последняя` → `Цена Corpbonds`
- `Цена, % от номинала` (в источнике Доходъ) → `Цена Доход` (в Merge/SQL и в витрине)
- `Smartlab_Котировка облигации, %` → `Цена Smartlab`
- `PRICE` → `Цена MOEX`

## Обновления по Corpbonds/YTM (schema_v3)

- В Corpbonds-парсинг добавлено поле `Накопленный купонный доход (НКД)` с записью в колонку `Corpbonds_НКД`.
- Для колонки купона в скринере используется приоритет Corpbonds: `COALESCE(Corpbonds_Ставка купона, Текущий купон, %)`.
- Для колонки НКД в скринере используется приоритет Corpbonds: `COALESCE(Corpbonds_НКД, НКД)`.
- Добавлен одноразовый сброс TTL-кэша Corpbonds по ключу `corpbonds_schema_v3_applied`.
- Добавлены расчеты YTM для:
  - флоатеров (КС / RUONIA / G-Curve Xy) с прогнозом ключевой ставки из `KEY_RATE_FORECAST`;
  - линкеров (ОФЗ-ИН / SU52*) с индексируемым номиналом по `INFLATION_FORECAST`.
- В Excel-экспорт добавлена диагностическая DEBUG-проверка типов для первых 20 строк (YTM/Купон,%/НКД).


## Обновления расчёта YTM (актуально)
- Единая точность вывода YTM: `YTM_OUTPUT_PRECISION = 2`.
- Для `Купон, %` используется приоритет `Corpbonds_Ставка купона`.
- Для НКД применяется защита по валюте номинала: `Corpbonds_НКД` учитывается только для RUB/RUR/SUR, иначе используется fallback (или 0).
- Добавлен sanity-check НКД (`NCD_FACEVALUE_SANITY_RATIO`): подозрительно большой НКД отбрасывается и логируется warning.
- Добавлен расчёт YTM для `Тип купона = Прочие облигации` как дисконтной бумаги (zero-coupon / discount).
- Для флоатеров парсится только `Формула купона` с поддержкой `КС/ключевая ставка`, `RUONIA`, `G-Curve/ZCYC <tenor>Y`; конструкции MAX/MIN/cap/floor игнорируются.
- Добавлены флаги принудительного сброса TTL: `FORCE_CORPBONDS_TTL_RESET`, `FORCE_CBR_TTL_RESET`.
- Добавлен debug-лог `logs/ytm_debug.jsonl` (`ENABLE_YTM_DEBUG_LOG`) для пропусков и аномалий YTM.
