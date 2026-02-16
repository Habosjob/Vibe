# ColumnDocs

Документация по полям для:
- `moex_bonds_to_excel.py` (лист `MOEX_BONDS`),
- `moex_bond_endpoints_probe.py` для endpoint `iss__engines__engine__markets__market__boardgroups__boardgroup__securities__security` (листы `securities`, `marketdata`, `marketdata_yields`).

> Обозначение **COMMON=✅** означает поле, совпадающее между двумя выгрузками и пригодное для последующего merge.

## 1) MOEX_BONDS (`moex_bonds_to_excel.py`)

| Поле | Описание | COMMON |
|---|---|---|
| SECID | Уникальный код инструмента в ISS MOEX. | ✅ |
| SHORTNAME | Краткое наименование бумаги. | ✅ |
| FACEVALUE | Номинал бумаги. | ✅ |
| FACEUNIT | Валюта номинала. | ✅ |
| COUPONVALUE | Размер купона за период. | ✅ |
| COUPONPERIOD | Период купона в днях. | ✅ |
| MATDATE | Дата погашения. | ✅ |
| LAST | Последняя цена сделки. | ✅ |
| WAPRICE | Средневзвешенная цена. | ✅ |
| YIELD | Доходность (рыночная). | ✅ |
| VALUE | Объем торгов в деньгах по инструменту. | ✅ |
| NUMTRADES | Количество сделок. | ✅ |

## 2) Endpoint `.../boardgroups/.../securities/[security]` — лист `securities`

| Поле | Описание | COMMON |
|---|---|---|
| REQUEST_URL | Фактический URL запроса в ISS. |  |
| SECID | Уникальный код инструмента в ISS MOEX. | ✅ |
| SHORTNAME | Краткое наименование бумаги. | ✅ |
| PREVWAPRICE | Предыдущая средневзвешенная цена. |  |
| YIELDATPREVWAPRICE | Доходность, рассчитанная от PREVWAPRICE. |  |
| COUPONVALUE | Размер купона за период. | ✅ |
| NEXTCOUPON | Дата ближайшей купонной выплаты. |  |
| ACCRUEDINT | НКД (накопленный купонный доход). |  |
| PREVPRICE | Предыдущая цена последней сделки/закрытия. |  |
| LOTSIZE | Размер лота (в бумагах). |  |
| FACEVALUE | Номинал бумаги. | ✅ |
| STATUS | Статус инструмента (`A` — активен и т.д.). |  |
| MATDATE | Дата погашения. | ✅ |
| DECIMALS | Количество знаков после запятой в котировке. |  |
| COUPONPERIOD | Период купона в днях. | ✅ |
| ISSUESIZE | Объем выпуска (шт.). |  |
| PREVLEGALCLOSEPRICE | Предыдущая официальная цена закрытия. |  |
| PREVDATE | Дата предыдущей торговой сессии/цены. |  |
| REMARKS | Служебные/текстовые пометки биржи. |  |
| MARKETCODE | Код рынка. |  |
| INSTRID | Идентификатор типа инструмента. |  |
| SECTORID | Код сектора рынка. |  |
| MINSTEP | Минимальный шаг цены. |  |
| FACEUNIT | Валюта номинала. | ✅ |
| BUYBACKPRICE | Цена оферты/выкупа (если есть). |  |
| BUYBACKDATE | Дата оферты/выкупа. |  |
| CURRENCYID | Валюта торгов/расчетов. |  |
| ISSUESIZEPLACED | Размещенный объем выпуска. |  |
| SECTYPE | Тип ценной бумаги. |  |
| COUPONPERCENT | Текущая купонная ставка, %. |  |
| OFFERDATE | Дата оферты (если применимо). |  |
| SETTLEDATE | Дата расчетов. |  |
| LOTVALUE | Стоимость лота. |  |
| FACEVALUEONSETTLEDATE | Номинал на дату расчетов. |  |
| CALLOPTIONDATE | Дата call-опциона (если есть). |  |
| PUTOPTIONDATE | Дата put-опциона (если есть). |  |
| DATEYIELDFROMISSUER | Дата доходности от эмитента. |  |
| BONDTYPE | Тип облигации. |  |
| BONDSUBTYPE | Подтип облигации. |  |

## 3) Endpoint `.../boardgroups/.../securities/[security]` — лист `marketdata`

| Поле | Описание | COMMON |
|---|---|---|
| REQUEST_URL | Фактический URL запроса в ISS. |  |
| SECID | Уникальный код инструмента в ISS MOEX. | ✅ |
| BID | Лучшая цена покупки. |  |
| BIDDEPTH | Объем на лучшей заявке покупки. |  |
| OFFER | Лучшая цена продажи. |  |
| OFFERDEPTH | Объем на лучшей заявке продажи. |  |
| SPREAD | Разница между OFFER и BID. |  |
| BIDDEPTHT | Суммарная глубина заявок на покупку. |  |
| OFFERDEPTHT | Суммарная глубина заявок на продажу. |  |
| OPEN | Цена открытия. |  |
| LOW | Минимальная цена дня. |  |
| HIGH | Максимальная цена дня. |  |
| LAST | Последняя цена сделки. | ✅ |
| LASTCHANGE | Изменение LAST к предыдущему значению. |  |
| LASTCHANGEPRCNT | Изменение LAST в процентах. |  |
| QTY | Объем последней сделки. |  |
| VALUE | Объем торгов в деньгах. | ✅ |
| YIELD | Текущая доходность. | ✅ |
| VALUE_USD | Объем торгов в USD-эквиваленте. |  |
| WAPRICE | Средневзвешенная цена. | ✅ |
| LASTCNGTOLASTWAPRICE | Отклонение LAST от LASTWAPRICE. |  |
| WAPTOPREVWAPRICEPRCNT | Изменение WAPRICE к PREVWAPRICE, %. |  |
| WAPTOPREVWAPRICE | Изменение WAPRICE к PREVWAPRICE. |  |
| YIELDATWAPRICE | Доходность на основе WAPRICE. |  |
| YIELDTOPREVYIELD | Изменение доходности к предыдущей. |  |
| CLOSEYIELD | Доходность на закрытии. |  |
| CLOSEPRICE | Цена закрытия. |  |
| MARKETPRICETODAY | Признанная рыночная цена на дату. |  |
| MARKETPRICE | Рыночная цена (MOEX). |  |
| LASTTOPREVPRICE | Изменение LAST к предыдущей цене. |  |
| NUMTRADES | Количество сделок. | ✅ |
| VOLTODAY | Оборот в инструментах (шт./лоты) за день. |  |
| VALTODAY | Денежный оборот за день. |  |
| VALTODAY_USD | Денежный оборот за день в USD-эквиваленте. |  |
| TRADINGSTATUS | Статус торгов. |  |
| UPDATETIME | Время обновления записи. |  |
| DURATION | Дюрация. |  |
| NUMBIDS | Количество заявок покупки. |  |
| NUMOFFERS | Количество заявок продажи. |  |
| CHANGE | Изменение цены к базе сравнения. |  |
| TIME | Время последней сделки/обновления. |  |
| HIGHBID | Максимальная цена среди bid. |  |
| LOWOFFER | Минимальная цена среди offer. |  |
| PRICEMINUSPREVWAPRICE | Разница цены и PREVWAPRICE. |  |
| LASTBID | Цена последней заявки покупки. |  |
| LASTOFFER | Цена последней заявки продажи. |  |
| LCURRENTPRICE | Последняя текущая признанная цена. |  |
| LCLOSEPRICE | Последняя официальная цена закрытия. |  |
| MARKETPRICE2 | Дополнительное поле рыночной цены ISS. |  |
| OPENPERIODPRICE | Цена открытия торгового периода. |  |
| SEQNUM | Порядковый номер записи (sequence). |  |
| SYSTIME | Системное время формирования строки ISS. |  |
| VALTODAY_RUR | Денежный оборот в RUB. |  |
| IRICPICLOSE | Индикатор/значение IR* на закрытии (служебное поле MOEX). |  |
| BEICLOSE | Значение break-even inflation на закрытии. |  |
| CBRCLOSE | Ориентир к ставке ЦБ на закрытии. |  |
| YIELDTOOFFER | Доходность к оферте. |  |
| YIELDLASTCOUPON | Доходность к последнему купону. |  |
| TRADINGSESSION | Идентификатор торговой сессии. |  |
| CALLOPTIONYIELD | Доходность к call-опциону. |  |
| CALLOPTIONDURATION | Дюрация к call-опциону. |  |
| ZSPREAD | Z-spread. |  |
| ZSPREADATWAPRICE | Z-spread, рассчитанный по WAPRICE. |  |

## 4) Endpoint `.../boardgroups/.../securities/[security]` — лист `marketdata_yields`

| Поле | Описание | COMMON |
|---|---|---|
| REQUEST_URL | Фактический URL запроса в ISS. |  |
| SECID | Уникальный код инструмента в ISS MOEX. | ✅ |
| PRICE | Цена, для которой рассчитаны доходности. |  |
| YIELDDATE | Дата, на которую относится расчет доходности. |  |
| ZCYCMOMENT | Метка времени для zero-coupon yield curve. |  |
| YIELDDATETYPE | Тип даты/режима расчета доходности. |  |
| EFFECTIVEYIELD | Эффективная доходность. |  |
| DURATION | Дюрация. |  |
| ZSPREADBP | Z-spread в базисных пунктах. |  |
| GSPREADBP | G-spread в базисных пунктах. |  |
| WAPRICE | Средневзвешенная цена. | ✅ |
| EFFECTIVEYIELDWAPRICE | Эффективная доходность на WAPRICE. |  |
| DURATIONWAPRICE | Дюрация на WAPRICE. |  |
| IR | Параметр IR из расчетной модели доходности MOEX. |  |
| ICPI | Инфляционный индекс (CPI) в расчетной модели. |  |
| BEI | Break-even inflation в расчетной модели. |  |
| CBR | Опорная ставка ЦБ в расчетной модели. |  |
| YIELDTOOFFER | Доходность к оферте. |  |
| YIELDLASTCOUPON | Доходность к последнему купону. |  |
| TRADEMOMENT | Момент времени, для которого рассчитаны метрики. |  |
| SEQNUM | Порядковый номер записи (sequence). |  |
| SYSTIME | Системное время формирования строки ISS. |  |

## 5) Колонки, удаленные из целевого endpoint workbook

Для `iss__engines__engine__markets__market__boardgroups__boardgroup__securities__security` удаляются как избыточные:

- `BOARDID`
- `BOARDNAME`
- `SECNAME`
- `ISIN`
- `LATNAME`
- `REGNUMBER`
- `LISTLEVEL`

Также удаляется лист `dataversion`.
