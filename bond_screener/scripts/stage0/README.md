# Stage0 — инфраструктурный этап

Stage0 подготавливает среду, проверяет базовые компоненты и фиксирует каждый запуск в run registry.

## Скрипты

- `env_check.py`:
  - проверяет Python/SQLite/платформу;
  - инициализирует SQLite и базовые таблицы (`runs`, `job_items`);
  - пишет лог `logs/stage0_env_check.log`;
  - при `excel_debug=true` сохраняет `source/xlsx/stage0_env_check.xlsx`.

- `reset_tool.py`:
  - читает `config/reset.yaml`;
  - выполняет сбросы (`cache`, `db`, `checkpoints`, `ttl` заглушка);
  - пишет лог `logs/stage0_reset_tool.log`;
  - сохраняет `source/xlsx/stage0_reset_tool.xlsx` при включенном debug;
  - после выполнения всегда возвращает `reset.yaml` в безопасное состояние.

- `run_registry.py`:
  - выполняет self-test реестра запусков;
  - добавляет тестовую запись и закрывает её со статусом `ok`;
  - пишет лог `logs/stage0_run_registry.log`;
  - сохраняет `source/xlsx/stage0_registry.xlsx` при включенном debug.

- `run.py`:
  - оркестратор Stage0;
  - выполняет шаги последовательно: `env_check -> reset_tool -> run_registry`;
  - оборачивает каждый шаг в `open_run/close_run_*`;
  - печатает единообразный статус в консоль.

## Таблицы SQLite

- `runs` — журнал запусков скриптов со статусом, ошибкой и длительностью.
- `job_items` — таблица чекпоинтов для задач с множеством элементов.
