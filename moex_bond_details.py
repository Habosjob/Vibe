import pandas as pd
import logging
import time
import os
import sys
import requests
from datetime import datetime
import json

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ (ДЕТАЛЬНО В ФАЙЛ, КРАТКО В КОНСОЛЬ) ====================
def setup_logging():
    """Настройка логирования: детальные логи в файл, только важное в консоль."""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(log_dir, "moex_bond_details.log")
    
    # Создаем логгер
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Ловим все сообщения от DEBUG и выше
    
    # Формат для файла (детальный)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    
    # Формат для консоли (краткий)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Обработчик для файла (все сообщения от DEBUG)
    file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Обработчик для консоли (только INFO и выше, без DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # В консоль только INFO, WARNING, ERROR, CRITICAL
    console_handler.setFormatter(console_formatter)
    
    # Добавляем обработчики к логгеру
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Инициализировано логирование. Детальные логи в файле: {log_filename}")
    logger.debug(f"Текущая директория: {os.getcwd()}")
    logger.debug(f"Python версия: {sys.version}")
    
    return logger

# ==================== КОНФИГУРАЦИЯ ====================
BOND_ISINS = [
    "RU000A10D533",
    "RU000A0JV4P3", 
    "RU000A10DB16",
    "RU000A0ZZ885",
    "RU000A106LL5"
]

BASE_URL = "https://iss.moex.com/iss"

# ==================== ОСНОВНЫЕ ФУНКЦИИ ====================
def fetch_endpoint_data(endpoint, logger, max_retries=2):
    """
    Загружает данные с указанного endpoint API MOEX.
    """
    url = f"{BASE_URL}{endpoint}"
    logger.debug(f"=== НАЧАЛО ЗАПРОСА ===")
    logger.debug(f"URL: {url}")
    
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
            }
            
            logger.debug(f"Попытка {attempt + 1}/{max_retries}")
            
            start_time = time.time()
            response = requests.get(url, headers=headers, timeout=30)
            request_time = time.time() - start_time
            
            logger.debug(f"Время запроса: {request_time:.2f} сек")
            logger.debug(f"HTTP статус: {response.status_code}")
            logger.debug(f"Размер ответа: {len(response.content)} байт")
            
            if response.status_code != 200:
                logger.warning(f"HTTP ошибка {response.status_code} для {endpoint}")
                if response.status_code == 404:
                    logger.error(f"Endpoint не найден: {endpoint}")
                    return None
                    
                if attempt < max_retries - 1:
                    logger.debug(f"Повторная попытка через 2 секунды...")
                    time.sleep(2)
                    continue
                return None
            
            # Декодируем ответ
            raw_content = response.content
            text_content = None
            used_encoding = None
            
            for encoding in ['utf-8', 'windows-1251', 'cp1251', 'iso-8859-5']:
                try:
                    text_content = raw_content.decode(encoding)
                    if text_content and '404' not in text_content and 'error' not in text_content.lower():
                        used_encoding = encoding
                        logger.debug(f"Успешное декодирование с кодировкой: {encoding}")
                        break
                except UnicodeDecodeError as e:
                    logger.debug(f"Ошибка декодирования с {encoding}: {e}")
                    continue
            
            if not text_content:
                logger.error(f"Не удалось декодировать ответ для {endpoint}")
                return None
            
            # Логируем начало ответа только в файл
            logger.debug(f"Начало ответа (500 символов):")
            logger.debug(f"{'-'*50}")
            logger.debug(text_content[:500])
            logger.debug(f"{'-'*50}")
            
            # Парсим JSON
            try:
                data = json.loads(text_content)
                logger.debug(f"JSON успешно распарсен. Ключи: {list(data.keys())}")
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка парсинга JSON для {endpoint}: {e}")
                logger.error(f"Начало текста: {text_content[:200]}")
                return None
            
            result = {}
            
            # Обрабатываем все блоки данных
            for key in data:
                if key == 'metadata':
                    logger.debug(f"Метаданные для {endpoint}: {data[key]}")
                    continue
                    
                if isinstance(data[key], dict) and 'columns' in data[key] and 'data' in data[key]:
                    logger.debug(f"Обработка ключа: {key}")
                    
                    columns = data[key]['columns']
                    rows = data[key]['data']
                    
                    logger.debug(f"  Столбцов: {len(columns)}, Строк: {len(rows)}")
                    logger.debug(f"  Столбцы: {columns}")
                    
                    if rows:
                        df = pd.DataFrame(rows, columns=columns)
                        
                        # Преобразуем даты
                        for col in df.columns:
                            if isinstance(col, str) and ('date' in col.lower() or 'time' in col.lower()):
                                try:
                                    df[col] = pd.to_datetime(df[col], errors='coerce')
                                except Exception as e:
                                    logger.debug(f"  Не удалось преобразовать столбец {col}: {e}")
                        
                        result[key] = df
                        logger.debug(f"  Создан DataFrame: {df.shape}")
                    else:
                        logger.warning(f"  Нет данных для ключа {key}")
            
            logger.debug(f"=== КОНЕЦ ЗАПРОСА, получено {len(result)} блоков ===")
            return result if result else None
            
        except requests.exceptions.Timeout:
            logger.error(f"Таймаут запроса для {endpoint}")
            if attempt < max_retries - 1:
                logger.debug("Повторная попытка...")
                time.sleep(3)
                continue
            return None
            
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Ошибка соединения для {endpoint}: {e}")
            if attempt < max_retries - 1:
                logger.debug("Повторная попытка...")
                time.sleep(5)
                continue
            return None
            
        except Exception as e:
            logger.error(f"Неожиданная ошибка для {endpoint}: {e}", exc_info=True)
            return None
    
    return None

def fetch_bond_data(isin, logger):
    """
    Загружает все доступные данные по облигации.
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"НАЧАЛО СБОРА ДАННЫХ ДЛЯ ОБЛИГАЦИИ: {isin}")
    logger.info(f"{'='*80}")
    
    bond_data = {}
    endpoints_to_fetch = [
        ("Основная информация", f"/securities/{isin}.json"),
        ("Рыночные данные", f"/securities/{isin}/marketdata.json"),
        ("Купоны", f"/securities/{isin}/coupons.json"),
        ("Амортизация", f"/securities/{isin}/amortizations.json"),
        ("Оферты", f"/securities/{isin}/offers.json"),
        ("Размещение", f"/securities/{isin}/placement.json"),
    ]
    
    for endpoint_name, endpoint_url in endpoints_to_fetch:
        logger.info(f"Загрузка: {endpoint_name}")
        
        data = fetch_endpoint_data(endpoint_url, logger)
        
        if data:
            # Сохраняем данные под понятным именем
            key_name = endpoint_name.lower().replace(' ', '_')
            bond_data[key_name] = data
            logger.info(f"✓ Успешно загружено: {len(data)} блок(ов) данных")
            
            # Логируем статистику по каждому блоку
            for data_key, df in data.items():
                if isinstance(df, pd.DataFrame):
                    logger.info(f"  - {data_key}: {df.shape[0]} строк, {df.shape[1]} столбцов")
        else:
            logger.warning(f"✗ Не удалось загрузить: {endpoint_name}")
    
    # Пробуем получить исторические данные
    logger.info(f"Поиск исторических данных...")
    
    # Сначала получаем информацию о бумаге, чтобы узнать engine/market/board
    if 'основная_информация' in bond_data and 'securities' in bond_data['основная_информация']:
        sec_df = bond_data['основная_информация']['securities']
        if not sec_df.empty and 'engine' in sec_df.columns and 'market' in sec_df.columns:
            engine = sec_df.iloc[0]['engine']
            market = sec_df.iloc[0]['market']
            
            logger.info(f"Найдены engine={engine}, market={market}")
            
            # Получаем список торговых площадок
            boards_url = f"/securities/{isin}/boards.json"
            boards_data = fetch_endpoint_data(boards_url, logger)
            
            if boards_data and 'boards' in boards_data:
                boards_df = boards_data['boards']
                if not boards_df.empty and 'boardid' in boards_df.columns:
                    board_id = boards_df.iloc[0]['boardid']
                    
                    # Формируем URL для истории
                    history_url = f"/history/engines/{engine}/markets/{market}/boards/{board_id}/securities/{isin}.json?limit=50"
                    logger.info(f"Загрузка истории: {history_url}")
                    
                    history_data = fetch_endpoint_data(history_url, logger)
                    if history_data:
                        bond_data['история'] = history_data
                        logger.info(f"✓ Загружена история: {len(history_data.get('history', pd.DataFrame()))} записей")
    
    logger.info(f"\nЗАВЕРШЕНО: {isin}. Получено блоков данных: {len(bond_data)}")
    
    return bond_data if bond_data else None

def save_bond_data_to_excel(bond_data, isin, logger):
    """Сохраняет все данные по облигации в Excel файл."""
    filename = f"bond_{isin}_data.xlsx"
    logger.info(f"Сохранение в Excel: {filename}")
    
    try:
        # Удаляем старый файл
        if os.path.exists(filename):
            os.remove(filename)
            logger.debug(f"Удален старый файл: {filename}")
        
        if not bond_data:
            logger.error("Нет данных для сохранения!")
            return False
        
        # Создаем Excel writer
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            sheets_created = 0
            
            # Сохраняем каждый блок данных
            for data_group_name, data_dict in bond_data.items():
                if isinstance(data_dict, dict):
                    for table_name, df in data_dict.items():
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            # Создаем имя листа
                            sheet_name = f"{data_group_name[:10]}_{table_name[:20]}"[:31]
                            
                            # Сохраняем
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            sheets_created += 1
                            
                            logger.debug(f"Лист '{sheet_name}': {df.shape}")
            
            # Создаем summary лист
            summary_df = create_summary_df(bond_data, isin)
            if not summary_df.empty:
                summary_df.to_excel(writer, sheet_name='SUMMARY', index=False)
                sheets_created += 1
                logger.debug(f"Лист 'SUMMARY': {summary_df.shape}")
        
        file_size = os.path.getsize(filename)
        logger.info(f"✓ Файл сохранен: {filename}")
        logger.info(f"  Размер: {file_size / 1024:.1f} KB")
        logger.info(f"  Листов: {sheets_created}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Ошибка сохранения Excel: {e}", exc_info=True)
        return False

def create_summary_df(bond_data, isin):
    """Создает DataFrame с ключевой информацией."""
    summary_data = {
        'ISIN': [isin],
        'Время_загрузки': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        'Блоков_данных': [len(bond_data)]
    }
    
    try:
        # Основная информация
        if 'основная_информация' in bond_data and 'securities' in bond_data['основная_информация']:
            sec_df = bond_data['основная_информация']['securities']
            if not sec_df.empty:
                row = sec_df.iloc[0]
                summary_data['Тикер'] = [row.get('SECID', '')]
                summary_data['Название'] = [row.get('SHORTNAME', '')[:30]]
                summary_data['Номинал'] = [row.get('FACEVALUE', '')]
                summary_data['Валюта'] = [row.get('FACEUNIT', '')]
                summary_data['Погашение'] = [str(row.get('MATDATE', ''))[:10]]
        
        # Рыночные данные
        if 'рыночные_данные' in bond_data and 'marketdata' in bond_data['рыночные_данные']:
            market_df = bond_data['рыночные_данные']['marketdata']
            if not market_df.empty:
                row = market_df.iloc[0]
                summary_data['Цена'] = [row.get('LAST', '')]
                summary_data['Доходность'] = [row.get('YIELD', '')]
                summary_data['Объем'] = [row.get('VOLUME', '')]
        
        # Купоны
        if 'купоны' in bond_data and 'coupons' in bond_data['купоны']:
            coupons_df = bond_data['купоны']['coupons']
            summary_data['Купонов'] = [len(coupons_df)]
    
    except Exception as e:
        logger.warning(f"Ошибка создания summary: {e}")
    
    return pd.DataFrame(summary_data)

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Главная функция скрипта."""
    logger = setup_logging()
    
    logger.info("=" * 80)
    logger.info("СКРИПТ СБОРА ДАННЫХ ПО ОБЛИГАЦИЯМ MOEX")
    logger.info("=" * 80)
    logger.info(f"Облигаций для обработки: {len(BOND_ISINS)}")
    logger.info(f"ISIN: {BOND_ISINS}")
    logger.info("=" * 80)
    
    successful = []
    failed = []
    
    start_time_total = time.time()
    
    for i, isin in enumerate(BOND_ISINS, 1):
        logger.info(f"\nОБРАБОТКА {i}/{len(BOND_ISINS)}: {isin}")
        
        bond_start = time.time()
        
        try:
            # Загружаем данные
            bond_data = fetch_bond_data(isin, logger)
            
            if bond_data:
                # Сохраняем в Excel
                if save_bond_data_to_excel(bond_data, isin, logger):
                    successful.append(isin)
                    logger.info(f"✓ УСПЕХ: {isin} - данные сохранены")
                else:
                    failed.append(isin)
                    logger.error(f"✗ ОШИБКА: {isin} - не удалось сохранить данные")
            else:
                failed.append(isin)
                logger.error(f"✗ ОШИБКА: {isin} - не удалось загрузить данные")
                
        except Exception as e:
            failed.append(isin)
            logger.error(f"✗ КРИТИЧЕСКАЯ ОШИБКА для {isin}: {e}", exc_info=True)
        
        bond_time = time.time() - bond_start
        logger.info(f"Время обработки {isin}: {bond_time:.2f} сек")
    
    # Итоги
    total_time = time.time() - start_time_total
    
    logger.info("\n" + "=" * 80)
    logger.info("ИТОГИ ВЫПОЛНЕНИЯ")
    logger.info("=" * 80)
    logger.info(f"Обработано: {len(BOND_ISINS)} облигаций")
    logger.info(f"Успешно: {len(successful)}")
    logger.info(f"С ошибками: {len(failed)}")
    
    if successful:
        logger.info("\nУспешные ISIN:")
        for isin in successful:
            logger.info(f"  ✓ {isin}")
    
    if failed:
        logger.warning("\nПроблемные ISIN:")
        for isin in failed:
            logger.warning(f"  ✗ {isin}")
    
    logger.info(f"\nОбщее время: {total_time:.2f} сек")
    logger.info(f"Среднее время на облигацию: {total_time/len(BOND_ISINS):.2f} сек")
    logger.info(f"Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    # Консольный вывод (только итоги)
    print(f"\n{'='*60}")
    print("РЕЗУЛЬТАТЫ СБОРА ДАННЫХ ПО ОБЛИГАЦИЯМ")
    print(f"{'='*60}")
    print(f"Всего облигаций: {len(BOND_ISINS)}")
    print(f"Успешно обработано: {len(successful)}")
    print(f"С ошибками: {len(failed)}")
    
    if successful:
        print(f"\nСозданные файлы:")
        for isin in successful:
            print(f"  • bond_{isin}_data.xlsx")
    
    print(f"\nДетальный лог: logs/moex_bond_details.log")
    print(f"Общее время: {total_time:.2f} сек")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()