import pandas as pd
import logging
import time
import os
import sys
import requests
import json
import argparse
from datetime import datetime, timedelta
from urllib.parse import urljoin

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
def setup_logging(isin, output_dir):
    """Настройка логирования с максимальной детализацией."""
    log_dir = os.path.join(output_dir, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = os.path.join(log_dir, f"{isin}_explorer.log")
    
    # Создаем логгер
    logger = logging.getLogger(isin)
    logger.setLevel(logging.DEBUG)
    
    # Формат для файла (максимально детальный)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s'
    )
    
    # Формат для консоли (только важное)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Обработчик для файла (ВСЁ в файл)
    file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Обработчик для консоли (только INFO и выше)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Логгер для запросов/ответов (отдельный файл)
    traffic_logger = setup_traffic_logger(isin, output_dir)
    
    return logger, traffic_logger, log_filename

def setup_traffic_logger(isin, output_dir):
    """Создает отдельный логгер для трафика запросов/ответов."""
    traffic_dir = os.path.join(output_dir, "traffic_logs")
    if not os.path.exists(traffic_dir):
        os.makedirs(traffic_dir)
    
    traffic_filename = os.path.join(traffic_dir, f"{isin}_traffic.log")
    
    traffic_logger = logging.getLogger(f"{isin}_traffic")
    traffic_logger.setLevel(logging.DEBUG)
    
    # Формат для трафика
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    handler = logging.FileHandler(traffic_filename, mode='w', encoding='utf-8')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    
    traffic_logger.addHandler(handler)
    traffic_logger.propagate = False  # Не передаем сообщения родительским логгерам
    
    return traffic_logger

# ==================== КОНСТАНТЫ И КОНФИГУРАЦИЯ ====================
BASE_URL = "https://iss.moex.com/iss"

# Список возможных точек входа (endpoints) для MOEX API
ENDPOINTS = [
    # Основная информация
    ("securities", "/securities/{isin}.json"),
    ("securities_search", "/securities.json?q={isin}&iss.meta=off&iss.json=extended"),
    
    # Рыночные данные
    ("marketdata", "/securities/{isin}/marketdata.json"),
    ("marketdata_yields", "/securities/{isin}/marketdata/yields.json"),
    
    # Финансовые данные по облигациям
    ("coupons", "/securities/{isin}/coupons.json"),
    ("coupons_full", "/securities/{isin}/coupons.json?iss.meta=off&iss.json=extended"),
    ("amortizations", "/securities/{isin}/amortizations.json"),
    ("offers", "/securities/{isin}/offers.json"),
    ("yields", "/securities/{isin}/yields.json"),
    ("zcyc", "/securities/{isin}/zcyc.json"),  # Кривая бескупонной доходности
    
    # Размещение
    ("placement", "/securities/{isin}/placement.json"),
    
    # Индикаторы и аналитика
    ("indicator", "/securities/{isin}/indicator.json"),
    ("indicatorhistory", "/securities/{isin}/indicatorhistory.json"),
    
    # Информация о торгах и листинге
    ("boards", "/securities/{isin}/boards.json"),
    ("boardgroups", "/securities/{isin}/boardgroups.json"),
    ("listing", "/securities/{isin}/listing.json"),
    
    # История торгов
    ("history_security", "/history/securities/{isin}.json"),
    ("history_engines", "/history/engines/stock/markets/bonds/securities/{isin}.json"),
    
    # Общая статистика
    ("statistics_bonds", "/statistics/engines/stock/markets/bonds/securities/{isin}.json"),
    ("statistics_bondization", "/statistics/engines/stock/markets/bonds/bondization.json?q={isin}"),
    
    # Индексы
    ("indices", "/statistics/engines/stock/markets/index/analytics/{isin}.json"),
    
    # Депозитарные данные
    ("depository", "/depository/{isin}.json"),
]

# Дополнительные endpoint'ы, требующие предварительного получения параметров
DYNAMIC_ENDPOINTS = [
    ("history_board", "/history/engines/{engine}/markets/{market}/boards/{board}/securities/{isin}.json"),
    ("history_dates", "/history/engines/{engine}/markets/{market}/boards/{board}/securities/{isin}.json?from={date_from}&till={date_to}"),
]

# ==================== УТИЛИТЫ ДЛЯ ЗАПРОСОВ ====================
def make_request(url, logger, traffic_logger, timeout=30, max_retries=2):
    """Выполняет HTTP-запрос с полным логированием."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    for attempt in range(max_retries):
        try:
            # Логируем запрос
            traffic_logger.info(f"=== ЗАПРОС ===")
            traffic_logger.info(f"URL: {url}")
            traffic_logger.info(f"Попытка: {attempt + 1}/{max_retries}")
            traffic_logger.info(f"Заголовки: {headers}")
            
            start_time = time.time()
            response = requests.get(url, headers=headers, timeout=timeout)
            request_time = time.time() - start_time
            
            # Логируем ответ
            traffic_logger.info(f"=== ОТВЕТ ===")
            traffic_logger.info(f"Статус: {response.status_code}")
            traffic_logger.info(f"Время: {request_time:.2f} сек")
            traffic_logger.info(f"Размер: {len(response.content)} байт")
            traffic_logger.info(f"Заголовки ответа: {dict(response.headers)}")
            
            # Пробуем разные кодировки
            raw_content = response.content
            decoded_content = None
            used_encoding = None
            
            encodings_to_try = ['utf-8', 'windows-1251', 'cp1251', 'iso-8859-5', 'koi8-r']
            
            for encoding in encodings_to_try:
                try:
                    decoded_content = raw_content.decode(encoding)
                    if decoded_content and len(decoded_content) > 0:
                        used_encoding = encoding
                        break
                except UnicodeDecodeError:
                    continue
            
            if decoded_content:
                traffic_logger.info(f"Кодировка: {used_encoding}")
                # Логируем первые 1000 символов ответа
                preview = decoded_content[:1000]
                traffic_logger.info(f"Начало ответа:\n{preview}")
                
                if len(decoded_content) > 1000:
                    traffic_logger.info(f"... и еще {len(decoded_content) - 1000} символов")
            else:
                traffic_logger.warning("Не удалось декодировать ответ")
                preview = raw_content[:500]
                traffic_logger.info(f"Сырые байты: {preview}")
            
            traffic_logger.info(f"=== КОНЕЦ ОТВЕТА ===\n")
            
            return {
                'success': True,
                'status_code': response.status_code,
                'url': url,
                'encoding': used_encoding,
                'content': decoded_content if decoded_content else raw_content,
                'headers': dict(response.headers),
                'time': request_time,
                'size': len(response.content)
            }
            
        except requests.exceptions.Timeout:
            traffic_logger.error(f"Таймаут запроса (попытка {attempt + 1})")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return {
                'success': False,
                'error': 'Timeout',
                'url': url
            }
            
        except requests.exceptions.ConnectionError as e:
            traffic_logger.error(f"Ошибка соединения: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            return {
                'success': False,
                'error': f'ConnectionError: {e}',
                'url': url
            }
            
        except Exception as e:
            traffic_logger.error(f"Неожиданная ошибка: {e}", exc_info=True)
            return {
                'success': False,
                'error': f'Exception: {str(e)[:200]}',
                'url': url
            }
    
    return {
        'success': False,
        'error': 'Max retries exceeded',
        'url': url
    }

def save_response(result, endpoint_name, isin, output_dir):
    """Сохраняет ответ endpoint'а в файл."""
    endpoint_dir = os.path.join(output_dir, "responses", endpoint_name)
    if not os.path.exists(endpoint_dir):
        os.makedirs(endpoint_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Сохраняем метаданные
    metadata = {
        'endpoint': endpoint_name,
        'isin': isin,
        'timestamp': timestamp,
        'success': result.get('success', False),
        'status_code': result.get('status_code'),
        'request_time': result.get('time'),
        'size_bytes': result.get('size'),
        'encoding': result.get('encoding'),
        'url': result.get('url'),
        'error': result.get('error')
    }
    
    metadata_filename = os.path.join(endpoint_dir, f"metadata_{timestamp}.json")
    with open(metadata_filename, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    # Сохраняем содержимое ответа
    if result.get('success') and result.get('content'):
        content = result['content']
        
        # Пробуем определить тип содержимого
        if isinstance(content, str):
            # Возможно, это JSON
            if content.strip().startswith('{') or content.strip().startswith('['):
                # Сохраняем как JSON
                json_filename = os.path.join(endpoint_dir, f"response_{timestamp}.json")
                try:
                    # Пробуем распарсить и сохранить красиво
                    parsed = json.loads(content)
                    with open(json_filename, 'w', encoding='utf-8') as f:
                        json.dump(parsed, f, indent=2, ensure_ascii=False)
                    
                    # Также сохраняем как сырой текст
                    raw_filename = os.path.join(endpoint_dir, f"response_{timestamp}.txt")
                    with open(raw_filename, 'w', encoding='utf-8') as f:
                        f.write(content)
                    
                    return {
                        'metadata_file': metadata_filename,
                        'json_file': json_filename,
                        'raw_file': raw_filename
                    }
                    
                except json.JSONDecodeError:
                    # Сохраняем как текст
                    txt_filename = os.path.join(endpoint_dir, f"response_{timestamp}.txt")
                    with open(txt_filename, 'w', encoding='utf-8') as f:
                        f.write(content)
                    
                    return {
                        'metadata_file': metadata_filename,
                        'raw_file': txt_filename
                    }
            else:
                # Сохраняем как текст
                txt_filename = os.path.join(endpoint_dir, f"response_{timestamp}.txt")
                with open(txt_filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                return {
                    'metadata_file': metadata_filename,
                    'raw_file': txt_filename
                }
    
    return {
        'metadata_file': metadata_filename
    }

# ==================== ОСНОВНЫЕ ФУНКЦИИ ====================
def discover_dynamic_endpoints(isin, initial_responses, logger, traffic_logger):
    """Обнаруживает динамические endpoints на основе полученных данных."""
    dynamic_results = []
    
    # Ищем информацию о торговых площадках (engine, market, board)
    boards_data = None
    for endpoint_name, result in initial_responses.items():
        if endpoint_name == 'boards' and result.get('success') and result.get('content'):
            try:
                content = json.loads(result['content'])
                boards_data = content
                logger.info("Найдены данные о торговых площадках для динамических запросов")
                break
            except:
                pass
    
    if boards_data:
        # Парсим данные о boards
        try:
            # Ищем первый доступный board
            if 'boards' in boards_data and 'data' in boards_data['boards']:
                boards_list = boards_data['boards']['data']
                if boards_list and len(boards_list) > 0:
                    # Берем первую площадку
                    first_board = boards_list[0]
                    columns = boards_data['boards']['columns']
                    
                    # Ищем нужные колонки
                    engine_idx = columns.index('engine') if 'engine' in columns else -1
                    market_idx = columns.index('market') if 'market' in columns else -1
                    board_idx = columns.index('boardid') if 'boardid' in columns else -1
                    
                    if engine_idx >= 0 and market_idx >= 0 and board_idx >= 0:
                        engine = first_board[engine_idx]
                        market = first_board[market_idx]
                        board = first_board[board_idx]
                        
                        logger.info(f"Найдены параметры: engine={engine}, market={market}, board={board}")
                        
                        # Формируем динамические endpoints
                        date_to = datetime.now().strftime("%Y-%m-%d")
                        date_from = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                        
                        dynamic_templates = [
                            ("history_board", 
                             f"/history/engines/{engine}/markets/{market}/boards/{board}/securities/{isin}.json"),
                            ("history_dates", 
                             f"/history/engines/{engine}/markets/{market}/boards/{board}/securities/{isin}.json?from={date_from}&till={date_to}"),
                        ]
                        
                        # Выполняем запросы к динамическим endpoints
                        for name, template in dynamic_templates:
                            url = urljoin(BASE_URL, template)
                            logger.info(f"Динамический запрос: {name}")
                            
                            result = make_request(url, logger, traffic_logger)
                            dynamic_results.append((name, result))
                            
                            if result.get('success'):
                                logger.info(f"  ✓ Успешно (статус: {result.get('status_code')})")
                            else:
                                logger.warning(f"  ✗ Ошибка: {result.get('error', 'Неизвестная ошибка')}")
        except Exception as e:
            logger.error(f"Ошибка при обработке динамических endpoints: {e}")
    
    return dynamic_results

def test_all_endpoints(isin, logger, traffic_logger, output_dir):
    """Тестирует все возможные endpoints для заданного ISIN."""
    logger.info(f"\n{'='*80}")
    logger.info(f"НАЧАЛО ТЕСТИРОВАНИЯ ENDPOINTS ДЛЯ {isin}")
    logger.info(f"{'='*80}\n")
    
    all_results = {}
    
    # Тестируем статические endpoints
    for endpoint_name, endpoint_template in ENDPOINTS:
        # Заменяем плейсхолдеры
        url_template = endpoint_template.replace("{isin}", isin)
        url = urljoin(BASE_URL, url_template)
        
        logger.info(f"Тестируем endpoint: {endpoint_name}")
        logger.info(f"URL: {url}")
        
        # Выполняем запрос
        result = make_request(url, logger, traffic_logger)
        all_results[endpoint_name] = result
        
        # Сохраняем ответ
        saved_files = save_response(result, endpoint_name, isin, output_dir)
        
        # Логируем результат
        if result.get('success'):
            status = result.get('status_code')
            time_taken = result.get('time', 0)
            size = result.get('size', 0)
            
            logger.info(f"  ✓ Успешно (статус: {status}, время: {time_taken:.2f} сек, размер: {size} байт)")
            
            # Проверяем, есть ли данные в ответе
            if result.get('content'):
                content = result['content']
                if isinstance(content, str):
                    try:
                        data = json.loads(content)
                        # Считаем количество записей данных
                        total_records = 0
                        if isinstance(data, dict):
                            for key, value in data.items():
                                if isinstance(value, dict) and 'data' in value:
                                    records = len(value['data'])
                                    total_records += records
                                    if records > 0:
                                        logger.info(f"    - {key}: {records} записей")
                        
                        if total_records == 0:
                            logger.warning(f"    ⚠ В ответе нет данных (пустые массивы)")
                    except json.JSONDecodeError:
                        logger.info(f"    - Ответ не в формате JSON")
        else:
            error_msg = result.get('error', 'Неизвестная ошибка')
            logger.error(f"  ✗ Ошибка: {error_msg}")
    
    # После тестирования статических endpoints, ищем динамические
    logger.info(f"\n{'='*80}")
    logger.info("ПОИСК ДИНАМИЧЕСКИХ ENDPOINTS")
    logger.info(f"{'='*80}\n")
    
    dynamic_results = discover_dynamic_endpoints(isin, all_results, logger, traffic_logger)
    
    # Сохраняем динамические результаты
    for endpoint_name, result in dynamic_results:
        saved_files = save_response(result, endpoint_name, isin, output_dir)
        all_results[endpoint_name] = result
    
    return all_results

def analyze_results(results, logger):
    """Анализирует результаты тестирования endpoints."""
    logger.info(f"\n{'='*80}")
    logger.info("АНАЛИЗ РЕЗУЛЬТАТОВ ТЕСТИРОВАНИЯ")
    logger.info(f"{'='*80}")
    
    total_endpoints = len(results)
    successful = sum(1 for r in results.values() if r.get('success') and r.get('status_code') == 200)
    failed = sum(1 for r in results.values() if not r.get('success') or r.get('status_code') != 200)
    
    logger.info(f"Всего протестировано endpoints: {total_endpoints}")
    logger.info(f"Успешных (HTTP 200): {successful}")
    logger.info(f"Неуспешных: {failed}")
    
    # Анализ кодов ответа
    status_codes = {}
    for result in results.values():
        status = result.get('status_code')
        if status:
            status_codes[status] = status_codes.get(status, 0) + 1
    
    if status_codes:
        logger.info("\nРаспределение по кодам ответа:")
        for code, count in sorted(status_codes.items()):
            logger.info(f"  HTTP {code}: {count}")
    
    # Находим endpoints с данными
    endpoints_with_data = []
    for endpoint_name, result in results.items():
        if result.get('success') and result.get('status_code') == 200 and result.get('content'):
            try:
                content = result['content']
                if isinstance(content, str):
                    data = json.loads(content)
                    has_data = False
                    
                    if isinstance(data, dict):
                        for key, value in data.items():
                            if isinstance(value, dict) and 'data' in value and len(value['data']) > 0:
                                has_data = True
                                break
                    
                    if has_data:
                        endpoints_with_data.append(endpoint_name)
            except:
                pass
    
    if endpoints_with_data:
        logger.info("\nEndpoints с данными:")
        for endpoint in sorted(endpoints_with_data):
            logger.info(f"  ✓ {endpoint}")
    else:
        logger.warning("\n⚠ Не найдено endpoints с данными")
    
    return {
        'total': total_endpoints,
        'successful': successful,
        'failed': failed,
        'status_codes': status_codes,
        'with_data': endpoints_with_data
    }

def generate_summary_report(isin, results, analysis, output_dir):
    """Генерирует сводный отчет."""
    report_dir = os.path.join(output_dir, "reports")
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = os.path.join(report_dir, f"summary_{timestamp}.txt")
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write(f"СВОДНЫЙ ОТЧЕТ ДЛЯ ОБЛИГАЦИИ: {isin}\n")
        f.write(f"Время создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"ОБЩАЯ СТАТИСТИКА:\n")
        f.write(f"  Всего протестировано endpoints: {analysis['total']}\n")
        f.write(f"  Успешных (HTTP 200): {analysis['successful']}\n")
        f.write(f"  Неуспешных: {analysis['failed']}\n")
        
        if analysis['with_data']:
            f.write(f"  Endpoints с данными: {len(analysis['with_data'])}\n")
        else:
            f.write(f"  Endpoints с данными: 0\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("ПОДРОБНЫЕ РЕЗУЛЬТАТЫ:\n")
        f.write("=" * 80 + "\n\n")
        
        # Группируем по статусу
        successful_endpoints = []
        failed_endpoints = []
        
        for endpoint_name, result in sorted(results.items()):
            status = result.get('status_code', 'N/A')
            success = result.get('success', False)
            error = result.get('error', '')
            
            if success and status == 200:
                successful_endpoints.append((endpoint_name, status, result.get('time', 0), result.get('size', 0)))
            else:
                failed_endpoints.append((endpoint_name, status, error))
        
        if successful_endpoints:
            f.write("УСПЕШНЫЕ ENDPOINTS:\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Endpoint':<30} {'Status':<10} {'Time (s)':<10} {'Size (bytes)':<15}\n")
            f.write("-" * 80 + "\n")
            
            for endpoint, status, time_taken, size in sorted(successful_endpoints):
                f.write(f"{endpoint:<30} {status:<10} {time_taken:<10.2f} {size:<15}\n")
            
            f.write("\n")
        
        if failed_endpoints:
            f.write("НЕУСПЕШНЫЕ ENDPOINTS:\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Endpoint':<30} {'Status':<10} {'Error':<40}\n")
            f.write("-" * 80 + "\n")
            
            for endpoint, status, error in sorted(failed_endpoints):
                f.write(f"{endpoint:<30} {status:<10} {error[:38]:<40}\n")
            
            f.write("\n")
        
        # Endpoints с данными
        if analysis['with_data']:
            f.write("ENDPOINTS С ДАННЫМИ:\n")
            f.write("-" * 80 + "\n")
            for endpoint in sorted(analysis['with_data']):
                f.write(f"  • {endpoint}\n")
            f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("ДИРЕКТОРИИ С ДАННЫМИ:\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("Структура сохраненных данных:\n")
        base_path = os.path.join(output_dir, "responses")
        if os.path.exists(base_path):
            for root, dirs, files in os.walk(base_path):
                level = root.replace(base_path, '').count(os.sep)
                indent = '  ' * level
                rel_path = os.path.relpath(root, base_path)
                
                if rel_path == '.':
                    f.write(f"{indent}responses/\n")
                else:
                    f.write(f"{indent}{os.path.basename(root)}/\n")
                
                subindent = '  ' * (level + 1)
                # Показываем только несколько файлов
                for i, file in enumerate(sorted(files)[:5]):
                    if i == 4 and len(files) > 5:
                        f.write(f"{subindent}... и еще {len(files) - 5} файлов\n")
                        break
                    f.write(f"{subindent}{file}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("РЕКОМЕНДАЦИИ:\n")
        f.write("=" * 80 + "\n\n")
        
        if analysis['with_data']:
            f.write("1. Для дальнейшего анализа используйте endpoints с данными (список выше).\n")
            f.write("2. Проверьте директорию 'responses' для получения сырых данных.\n")
            f.write("3. Для парсинга JSON ответов используйте сохраненные файлы .json.\n")
        else:
            f.write("1. Не найдено endpoints с данными. Возможные причины:\n")
            f.write("   - ISIN указан неверно\n")
            f.write("   - Облигация не торгуется на MOEX\n")
            f.write("   - Ограниченный доступ к данным\n")
            f.write("2. Проверьте логи в директории 'logs' для деталей.\n")
            f.write("3. Попробуйте другой ISIN.\n")
    
    return report_filename

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Главная функция скрипта."""
    parser = argparse.ArgumentParser(description='Поиск точек входа для облигаций на MOEX')
    parser.add_argument('isin', help='ISIN облигации (например, RU000A10D533)')
    parser.add_argument('--output', '-o', default='./moex_explorer_output',
                       help='Директория для сохранения результатов (по умолчанию: ./moex_explorer_output)')
    
    args = parser.parse_args()
    isin = args.isin.upper()
    output_dir = os.path.join(args.output, isin)
    
    # Создаем директорию для результатов
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Настраиваем логирование
    logger, traffic_logger, log_filename = setup_logging(isin, output_dir)
    
    logger.info(f"{'='*80}")
    logger.info(f"ЗАПУСК СКРИПТА ДЛЯ ОБЛИГАЦИИ: {isin}")
    logger.info(f"Директория результатов: {output_dir}")
    logger.info(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*80}\n")
    
    print(f"\n{'='*60}")
    print(f"ЗАПУСК ПОИСКА ТОЧЕК ВХОДА ДЛЯ: {isin}")
    print(f"Директория результатов: {output_dir}")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    
    try:
        # Тестируем все endpoints
        results = test_all_endpoints(isin, logger, traffic_logger, output_dir)
        
        # Анализируем результаты
        analysis = analyze_results(results, logger)
        
        # Генерируем отчет
        report_file = generate_summary_report(isin, results, analysis, output_dir)
        
        total_time = time.time() - start_time
        
        logger.info(f"\n{'='*80}")
        logger.info(f"ЗАВЕРШЕНО УСПЕШНО!")
        logger.info(f"Общее время выполнения: {total_time:.2f} сек")
        logger.info(f"Отчет сохранен в: {report_file}")
        logger.info(f"{'='*80}")
        
        # Краткий вывод в консоль
        print(f"\n{'='*60}")
        print("РЕЗУЛЬТАТЫ ПОИСКА:")
        print(f"{'='*60}")
        print(f"Облигация: {isin}")
        print(f"Всего endpoints протестировано: {analysis['total']}")
        print(f"Успешных (HTTP 200): {analysis['successful']}")
        print(f"Endpoints с данными: {len(analysis['with_data'])}")
        print(f"\nСОХРАНЕННЫЕ ДАННЫЕ:")
        print(f"• Логи: {output_dir}/logs/")
        print(f"• Ответы endpoints: {output_dir}/responses/")
        print(f"• Трафик: {output_dir}/traffic_logs/")
        print(f"• Отчеты: {output_dir}/reports/")
        print(f"\nВремя выполнения: {total_time:.2f} сек")
        print(f"{'='*60}")
        
        if analysis['with_data']:
            print(f"\nНАЙДЕНЫ ДАННЫЕ В СЛЕДУЮЩИХ ENDPOINTS:")
            for endpoint in sorted(analysis['with_data']):
                print(f"  • {endpoint}")
        else:
            print(f"\n⚠ ВНИМАНИЕ: Не найдено endpoints с данными")
            print(f"   Проверьте логи для деталей")
        
    except KeyboardInterrupt:
        logger.info("Скрипт прерван пользователем")
        print("\n\nСкрипт прерван пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        print(f"\n\n❌ Критическая ошибка: {e}")
        print(f"   Проверьте логи для деталей")
    
    print(f"\nДля деталей смотрите: {log_filename}")

if __name__ == "__main__":
    main()