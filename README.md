# bond_screener

Локальный проект для скрининга облигаций MOEX, ориентированный на запуск из VS Code и работу без аргументов.

## Быстрый старт

1. Установите Python 3.11+.
2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```
3. Запустите проект:
   ```bash
   python scripts/run.py
   ```

## Что происходит при запуске без аргументов

- автоматически создаются папки `config/`, `out/`, `logs/`, `raw/`;
- если нет конфигов, создаются:
  - `config/config.yml`
  - `config/scenarios.yml`
  - `config/allowlist.yml`
  - `config/issuer_links.yml`
  - `config/portfolio.yml`
- печатаются этапы выполнения (пока заглушки);
- выполняется очистка `raw/` по TTL;
- перезаписывается `logs/latest.log`;
- печатается итоговая сводка и время выполнения.

## Структура

- `bond_screener/` — пакет с основной логикой запуска.
- `scripts/` — исполняемые скрипты (`run.py`).
- `docs/` — документация по конфигам, источникам, скорингу и мониторингу.
- `tests/` — pytest-тесты.
- `config/`, `out/`, `logs/`, `raw/` — рабочие папки, создаются автоматически.

## Тесты

```bash
pytest
```
