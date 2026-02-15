# IMPROVEMENTS

## Реализовано в этом релизе
1. ✅ **Вынесен SQL-слой details-кэша в `repositories/`** (`DetailsRepository` с bulk-загрузкой cache/latest).
   - Профит: меньше SQL-вызовов в горячем цикле, заметно быстрее этап подготовки задач на 3-м этапе.
2. ✅ **Structured logging (JSON) + correlation_id (`run_id`)**.
   - Профит: проще трассировать один запуск через весь pipeline и разбирать инциденты в лог-агрегаторах.
3. ✅ **Lightweight Prometheus-метрики через `/metrics` в API**.
   - Профит: доступны ключевые показатели (`cache_hit_ratio`, latency по этапам, error_rate по endpoint) для дашбордов и алертов.
4. ✅ **Добавлены unit-тесты критичных участков (diff, incremental, read-model)**.
   - Профит: меньше регрессий при изменениях ETL/SQLite логики.

## Осознанно не реализовано
- Уведомления Telegram/Slack (по вашему условию).
- Вынос scheduler-service в отдельный orchestrator/процесс.

## Предлагаемые следующие доработки (кратко + профит)
1. **Разделить `MOEX_API.py` на доменные сервисы (`ingest`, `details`, `export`, `dq`)**.
   - Профит: проще сопровождать, меньше риск side-effect при изменениях.
2. **Перевести тяжёлые merge/normalize шаги на батчи + pyarrow dataset scan**.
   - Профит: снижение пикового RAM и ускорение на больших объёмах.
3. **Инкрементально обновлять `bonds_read_model` (upsert по SECID), а не пересоздавать таблицу полностью**.
   - Профит: быстрее 4-й этап и меньше write amplification в SQLite.
4. **Добавить retention/архивацию для `details_rows`, `intraday_quotes_snapshot`, `bonds_enriched_incremental` с политиками по дням/объёму**.
   - Профит: контролируемый размер БД, прогнозируемое время VACUUM/backup.
5. **Ввести профили запуска (`full|incremental|offline`) как обязательный CLI-параметр для cron**.
   - Профит: детерминированное поведение и меньше случайных "тяжёлых" прогонов.
