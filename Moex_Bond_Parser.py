import pandas as pd
import logging
import time
import os
import sys
import requests
from datetime import datetime
from io import StringIO
import chardet

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
def setup_logging():
    """Настройка перезаписываемого лога."""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(log_dir, "moex_download.log")
    log_format = '%(asctime)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# ==================== ОПРЕДЕЛЕНИЕ ФОРМАТА ДАННЫХ ====================
def analyze_data_format(raw_text, logger):
    """
    Анализирует сырые данные, чтобы определить, где начинается таблица.
    Возвращает параметры для pd.read_csv.
    """
    logger.info("Анализ формата данных...")
    
    lines = raw_text.splitlines()
    
    # Логируем первые 10 строк для диагностики
    logger.info("Первые 10 строк ответа сервера:")
    for i, line in enumerate(lines[:10]):
        logger.info(f"  Строка {i}: {line[:200]}")

    # Находим первую строку, которая выглядит как заголовок таблицы
    # (содержит несколько точек с запятой подряд)
    header_line_num = None
    for i, line in enumerate(lines):
        if line.count(';') > 5:  # Если в строке больше 5 точек с запятой
            header_line_num = i
            logger.info(f"Найден заголовок таблицы в строке {i}: {line[:100]}...")
            break
    
    if header_line_num is None:
        logger.error("Не удалось найти строку с заголовком таблицы!")
        return None
    
    # Определяем кодировку с помощью chardet
    encoding = detect_encoding(raw_text)
    logger.info(f"Определена кодировка: {encoding}")
    
    # Определяем количество столбцов по заголовку
    column_count = lines[header_line_num].count(';') + 1
    logger.info(f"Определено количество столбцов: {column_count}")
    
    return {
        'skiprows': header_line_num,  # Пропускаем строки до заголовка
        'sep': ';',
        'encoding': encoding,
        'header': 0,  # Используем найденную строку как заголовок
        'on_bad_lines': 'skip'
    }

def detect_encoding(raw_text):
    """Определяет кодировку текста."""
    result = chardet.detect(raw_text[:10000].encode() if isinstance(raw_text, str) else raw_text[:10000])
    encoding = result['encoding']
    confidence = result['confidence']
    
    # Если определение неуверенное, используем windows-1251 по умолчанию
    if confidence < 0.7:
        return 'windows-1251'
    return encoding

# ==================== ЗАГРУЗКА И ПАРСИНГ ====================
def download_and_parse_data(url, logger):
    """Загружает данные и корректно парсит их."""
    try:
        logger.info("Загрузка данных с MOEX...")
        
        # Загружаем сырые данные
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=45)
        
        # Сохраняем сырой текст для анализа
        raw_text = response.text
        
        # Анализируем формат данных
        csv_params = analyze_data_format(raw_text, logger)
        if csv_params is None:
            logger.error("Не удалось определить параметры парсинга CSV")
            return None
        
        # Парсим CSV с определенными параметрами
        logger.info("Парсинг CSV...")
        df = pd.read_csv(StringIO(raw_text), **csv_params)
        
        logger.info(f"Успешно загружено: {df.shape[0]} строк, {df.shape[1]} столбцов")
        
        # Проверяем результат
        if df.shape[1] < 2:
            logger.error(f"Парсинг дал только {df.shape[1]} столбцов. Пробуем альтернативный метод...")
            df = try_alternative_parsing(raw_text, logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке: {e}")
        return None

def try_alternative_parsing(raw_text, logger):
    """Альтернативные методы парсинга, если основной не сработал."""
    logger.info("Пробуем альтернативные методы парсинга...")
    
    lines = raw_text.splitlines()
    
    # Метод 1: Ищем строку с максимальным количеством точек с запятой
    max_semicolons = 0
    best_line = 0
    
    for i, line in enumerate(lines):
        semicolon_count = line.count(';')
        if semicolon_count > max_semicolons:
            max_semicolons = semicolon_count
            best_line = i
    
    logger.info(f"Строка с максимальным количеством столбцов ({max_semicolons + 1}): строка {best_line}")
    
    # Пробуем парсить с разным количеством пропускаемых строк
    for skip in range(max(0, best_line - 3), min(best_line + 3, len(lines))):
        try:
            df = pd.read_csv(
                StringIO('\n'.join(lines[skip:])),
                sep=';',
                encoding='windows-1251',
                on_bad_lines='skip'
            )
            if df.shape[1] > 1:
                logger.info(f"Успех при skiprows={skip}: {df.shape[1]} столбцов")
                return df
        except:
            continue
    
    return None

# ==================== БЫСТРОЕ СОХРАНЕНИЕ ====================
def save_to_excel_fast(df, filename='moex_bond_rates.xlsx', logger=None):
    """Быстрое сохранение в Excel без форматирования столбцов."""
    if logger:
        logger.info(f"Сохранение в {filename}...")
    
    try:
        # Удаляем старый файл
        if os.path.exists(filename):
            os.remove(filename)
        
        # Сохраняем без индекса
        df.to_excel(filename, index=False, engine='openpyxl')
        
        file_size = os.path.getsize(filename) / 1024
        if logger:
            logger.info(f"Файл сохранен: {filename} ({file_size:.1f} KB)")
        
        return True
    except Exception as e:
        if logger:
            logger.error(f"Ошибка сохранения: {e}")
        return False

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Главная функция."""
    start_time = time.time()
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("УМНЫЙ ЗАГРУЗЧИК MOEX С АНАЛИЗОМ ФОРМАТА")
    logger.info("=" * 60)
    
    # URL для загрузки
    target_url = "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"
    
    # Загрузка и парсинг
    df = download_and_parse_data(target_url, logger)
    
    if df is not None and df.shape[1] > 1:
        # Сохранение
        success = save_to_excel_fast(df, logger=logger)
        
        if success:
            logger.info("=" * 60)
            logger.info(f"УСПЕХ! Загружено {df.shape[0]} строк, {df.shape[1]} столбцов")
            logger.info("=" * 60)
            
            # Вывод в консоль
            print(f"\n✅ УСПЕШНО!")
            print(f"   Строк: {df.shape[0]}")
            print(f"   Столбцов: {df.shape[1]}")
            print(f"   Первые 5 столбцов: {list(df.columns)[:5]}")
            print(f"   Сохранено в: moex_bond_rates.xlsx")
        else:
            logger.error("Ошибка сохранения файла")
    else:
        logger.error("Не удалось загрузить корректные данные")
        print(f"\n❌ ОШИБКА: Данные не загружены или содержат только 1 столбец")
        print(f"   Проверьте лог-файл для деталей")
    
    # Время выполнения
    total_time = time.time() - start_time
    logger.info(f"Общее время выполнения: {total_time:.2f} сек")
    
    print(f"\n⏱️  Время выполнения: {total_time:.2f} сек")

if __name__ == "__main__":
    main()