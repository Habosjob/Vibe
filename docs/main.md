# Скрипт `main.py` — загрузка MOEX rates + рейтинги НРА/АКРА/НКР + витрина эмитентов

## Что делает скрипт
1. Загружает CSV с MOEX по ссылке из `config.py`.
2. Сохраняет файл в папки:
   - `raw/moex_rates.csv` (основной «сырой» файл),
   - `cache/moex_rates_cache.csv` (кэш-копия).
3. Обновляет SQLite-базу `DB/moex_rates.sqlite3` (таблица `rates`) полной перезаливкой при истекшем TTL.
4. Загружает Excel-выгрузку рейтингов НРА со страницы `https://www.ra-national.ru/ratings/`:
   - автоматически находит ссылку на `.xlsx/.xls` на странице,
   - сохраняет оригинальный файл в `raw/nra_ratings.xlsx`,
   - обновляет отдельную SQLite-базу `DB/raitings.sqlite3`,
   - работает с TTL 12 часов (настраивается `NRA_CACHE_TTL_HOURS`).
5. Парсит рейтинги АКРА методом из `Acra.py` (через `playwright.sync_api` + запуск **Google Chrome** + паузы/ретраи):
   - первый этап (список) обновляется не чаще одного раза в 12 часов (`ACRA_CACHE_TTL_HOURS`),
   - извлекаются: ссылка на карточку, наименование, рейтинг, прогноз и дата,
   - карточки открываются в persistent-профиле браузера (`ACRA_PROFILE_DIR`) для устойчивости к антибот-защите,
   - обновления инкрементальные в таблицу `acra_ratings`.
6. Для АКРА реализована оптимизация карточек:
   - если ссылка эмитента уже есть в БД и по ней уже сохранен ИНН, карточка повторно не запрашивается,
   - карточка запрашивается только для новых эмитентов (или если по ссылке ранее не удалось получить ИНН).
7. В отдельной БД рейтинговых агентств:
   - НРА хранится в `nra_ratings` и `nra_latest_by_inn`,
   - АКРА хранится в `acra_ratings` с уникальностью `issuer_url + rating_date + rating` (отдельно сохраняются `rating` и `forecast`),
   - НКР хранится в `nkr_ratings` и `nkr_latest_by_tin` (уникальность истории по `tin + rating_date + rating + outlook`).
8. Ведет таблицу `emitents` с уникальными эмитентами по `INN` в основной БД:
   - берет поля `EMITENTNAME` и `INN` из `rates`,
   - инкрементально делает upsert,
   - хранит пользовательские поля `Scoring`, `DateScoring` и столбцы `NRA_Rate`, `Acra_Rate`, `NKR_Rate`.
9. Обогащает витрину эмитентов рейтингами НРА:
   - матчинг по `INN`,
   - формат значения: `Рейтинг(прогноз)`.
10. Обогащает витрину эмитентов рейтингами АКРА:
   - матчинг по `INN`,
   - сохраняет в `Acra_Rate`,
   - формат `Рейтинг(прогноз)` при наличии прогноза, иначе только рейтинг.
11. Загружает Excel-выгрузку НКР через Playwright со страницы `https://ratings.ru/ratings/issuers/`:
   - нажимает кнопку `Выгрузить в Excel`,
   - сначала пытается скачать через событие браузерной загрузки, затем по прямому `href`, и только после этого — через `blob:` ссылку,
   - в БД пишет только нужные поля: `Date`, `Rating`, `Outlook`, `TIN`,
   - делает инкрементальное обновление истории в `nkr_ratings`,
   - формирует `nkr_latest_by_tin` (уникально по ИНН с самой свежей датой).
12. Обогащает витрину эмитентов рейтингами НКР по `TIN == INN` в колонку `NKR_Rate`.
13. Синхронизирует ручные оценки из Excel-витрины `Emitents.xlsx` обратно в SQL.
14. Автоматически заполняет `DateScoring`, когда есть `Scoring`, а дата еще пустая.
15. Формирует Excel-витрину `Emitents.xlsx` в корне проекта с оформлением и валидацией `Scoring`.
16. Создает снапшоты в `BaseSnapshots`:
   - `rates_snapshot.xlsx` (5 случайных строк с уникальными `SECID`),
   - `emitents_snapshot.xlsx` (5 случайных строк из `emitents`),
   - `nra_snapshot.xlsx` (**5 самых свежих** строк по дате рейтинга),
   - `acra_snapshot.xlsx` (**5 самых свежих** строк по дате рейтинга),
  - `nkr_snapshot.xlsx` (**5 самых свежих** строк по дате рейтинга).
17. Пишет технический лог в `logs/main.log` (перезаписывается каждый запуск).

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

## Настройка
Все настройки находятся в `config.py`.
Каждый параметр снабжен комментарием: назначение, допустимые значения и дефолт.

Ключевые параметры:
- `CACHE_TTL_HOURS` — срок жизни кэша MOEX в часах.
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
- `RAITINGS_DB_FILENAME` — файл общей SQLite-базы рейтинговых агентств.
- `ACRA_TABLE_NAME` — таблица АКРА в БД рейтинговых агентств.

## Примечание для Windows
Проект ориентирован на Windows-сценарий:
- пути собираются через `pathlib.Path`,
- лог/файлы пишутся в UTF-8,
- запуск без дополнительных CLI-аргументов.
