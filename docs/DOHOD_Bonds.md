# DOHOD_Bonds — скачивание Excel с dohod.ru

## Что делает скрипт
`DOHOD_Bonds.py` автоматизирует кнопку **«СКАЧАТЬ EXCEL»** на странице:
`https://www.dohod.ru/analytic/bonds`

Скрипт:
1. Проверяет, есть ли рядом уже файл `Dohod_Bonds.xlsx`, обновленный **сегодня**.
2. Если файл уже сегодняшний — повторно не качает (быстрый skip).
3. Если файла нет или он не сегодняшний — открывает сайт через Playwright.
4. Нажимает кнопку «СКАЧАТЬ EXCEL» и ловит событие скачивания.
5. Сохраняет файл рядом со скриптом как `Dohod_Bonds.xlsx`.
6. Пишет перезаписываемый лог в `logs/DOHOD_Bonds.log`.
7. Показывает интерактивный прогресс-бар и спиннер в консоли.
8. При ошибке скрипт завершает процесс с кодом `1` без выброса `SystemExit`-traceback в консоль.
9. Если файл `Dohod_Bonds.xlsx` уже скачан сегодня, шаг завершается успешно без запуска Playwright.

---

## Настройки YAML
Все параметры находятся в `config/moex_bonds.yaml` в секции `dohod`.

```yaml
dohod:
  enabled: true
  source:
    url: "https://www.dohod.ru/analytic/bonds"
    download_button_text: "СКАЧАТЬ EXCEL"
  output:
    excel_path: "Dohod_Bonds.xlsx"
  network:
    timeout_sec: 90
    download_timeout_sec: 120
  browser:
    headless: true
  logging:
    path: "logs/DOHOD_Bonds.log"
```

### Что можно настраивать
- `dohod.enabled` — включение/выключение шага.
- `dohod.required_for_pipeline` — если `true`, ошибка шага роняет `main.py`; если `false`, пайплайн завершается с предупреждением.
- `dohod.source.url` — URL страницы.
- `dohod.source.download_button_text` — текст кнопки загрузки.
- `dohod.output.excel_path` — путь итогового файла.
- `dohod.network.timeout_sec` — таймаут открытия страницы и ожидания кнопки.
- `dohod.network.download_timeout_sec` — таймаут ожидания события скачивания.
- `dohod.browser.headless` — запуск браузера в headless/headed режиме.
- `dohod.logging.path` — путь к логу.

---

## Логи и отладка
- Лог перезаписывается каждый запуск (`mode="w"`).
- Основные этапы и ошибки пишутся в `logs/DOHOD_Bonds.log`.
- Дополнительно состояние последней загрузки сохраняется в `logs/cache/dohod_bonds_state.json`.

Если не скачивается файл:
1. Проверьте, что Playwright установлен и браузер Chromium доступен.
2. Убедитесь, что текст кнопки в `download_button_text` совпадает с текущим на сайте.
3. Увеличьте `timeout_sec` и `download_timeout_sec`.
4. Посмотрите stack trace в `logs/DOHOD_Bonds.log`.
5. Если видите сообщение про `PyYAML`/`Playwright` — установите зависимости и Chromium-браузер для Playwright.

---

## Запуск
```bash
python3 DOHOD_Bonds.py --config config/moex_bonds.yaml
```


## Зависимости
Рекомендуется устанавливать из `requirements.txt`:
```bash
pip install -r requirements.txt
python -m playwright install chromium
```
