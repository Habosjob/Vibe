#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import json
import os
import time
from datetime import datetime
import requests

# Настройка логирования
LOG_FILE = 'logs/moex_bond_details.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Основные точки входа MOEX для облигаций
MOEX_API_ENDPOINTS = [
    "https://iss.moex.com/iss/securities.json",
    "https://iss.moex.com/iss/securities/{isin}.json",
    "https://iss.moex.com/iss/history/securities.json?secid={isin}",
    "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/tqob/securities/{isin}.json",
    "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bonds.json?iss.only=bonds&bonds.isin={isin}"
]

# Директория для сохранения ответов
RESPONSE_DIR = "responses/bond_details"
os.makedirs(RESPONSE_DIR, exist_ok=True)



def save_response(isin: str, endpoint: str, response: requests.Response):
    """Сохраняет ответ сервера в файл (текст + JSON, если возможно)."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    safe_endpoint = endpoint.replace("https://", "").replace("/", "_").replace("?", "_").replace("=", "_")
    filename = f"{RESPONSE_DIR}/{isin}_{safe_endpoint}_{timestamp}"

    # Сохраняем текст ответа
    with open(f"{filename}.txt", "w", encoding="utf-8") as f:
        f.write(f"Endpoint: {endpoint}\n")
        f.write(f"Status: {response.status_code}\n")
        f.write(f"Headers: {dict(response.headers)}\n")
        f.write(f"\nResponse body:\n{response.text}\n")

    # Если ответ — JSON, сохраняем отдельно
    try:
        json_data = response.json()
        with open(f"{filename}.json", "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранён JSON-ответ: {filename}.json")
    except json.JSONDecodeError:
        logger.warning(f"Ответ не является JSON, пропускаем сохранение .json для {filename}")



def fetch_bond_info(isin: str) -> bool:
    """Пытается получить информацию по ISIN через все точки входа MOEX."""
    success = False
    for endpoint in MOEX_API_ENDPOINTS:
        url = endpoint.format(isin=isin)
        logger.info(f"Запрос к {url}")

        try:
            response = requests.get(url, timeout=10)
            logger.info(f"Статус: {response.status_code}")

            if response.status_code == 200:
                logger.info(f"Успешный ответ от {url}")
                save_response(isin, url, response)
                success = True
            else:
                logger.warning(f"Неуспешный статус: {response.status_code} для {url}")

        except requests.RequestException as e:
            logger.error(f"Ошибка запроса к {url}: {e}")

        # Пауза между запросами
        time.sleep(1)

    return success



def main():
    print("Введите ISIN облигации (например, RU000A10A2V0):")
    isin = input().strip().upper()
    logger.info(f"Старт поиска информации по ISIN: {isin}")

    if not isin.startswith("RU") or len(isin) != 12:
        logger.error("ISIN должен начинаться с RU и иметь длину 12 символов")
        sys.exit(1)

    success = fetch_bond_info(isin)

    if success:
        logger.info(f"Информация по ISIN {isin} успешно получена и сохранена в {RESPONSE_DIR}")
    else:
        logger.error(f"Не удалось получить информацию по ISIN {isin} ни через одну точку входа")

    logger.info("Завершение работы")



if __name__ == "__main__":
    main()
