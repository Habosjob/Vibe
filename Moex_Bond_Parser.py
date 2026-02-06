import pandas as pd
import logging
import time
import os
import sys
import json
import requests
from datetime import datetime
from io import StringIO

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
def setup_logging():
    """
    Настройка системы логирования.
    Используется один перезаписываемый файл лога.
    """
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(log_dir, "moex_download.log")
    log_format = '%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'

    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Инициализировано логирование. Файл: {log_filename}")
    return logger

# ==================== ЗАГРУЗКА И ОБРАБОТКА ДАННЫХ ====================
def download_and_decode_data(url, logger):
    """
    Загружает сырые данные с MOEX и пытается декодировать их разными способами.
    Возвращает pandas DataFrame или None в случае неудачи.
    """
    try:
        logger.info("Загрузка сырых данных с сервера...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'windows-1251'

        # КРИТИЧЕСКОЕ: Сохраняем и логируем сырой ответ для анализа
        raw_content = response.text
        log_raw_data(raw_content, logger)

        # Стратегия 1: Пытаемся обработать как чистый CSV
        df = try_parse_as_csv(raw_content, logger)
        if df is not None and df.shape[1] > 1:
            logger.info(f"Стратегия 1 (CSV) успешна: {df.shape[0]}x{df.shape[1]}")
            return df

        # Стратегия 2: Пытаемся найти JSON в тексте
        df = try_parse_as_json(raw_content, logger)
        if df is not None and not df.empty:
            logger.info(f"Стратегия 2 (JSON) успешна: {df.shape[0]}x{df.shape[1]}")
            return df

        # Стратегия 3: Пытаемся извлечь таблицу из текста по строкам
        df = try_parse_as_text_table(raw_content, logger)
        if df is not None and not df.empty:
            logger.info(f"Стратегия 3 (текстовая таблица) успешна: {df.shape[0]}x{df.shape[1]}")
            return df

        logger.error("Все стратегии парсинга не дали результата.")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}", exc_info=True)
        return None

def log_raw_data(content, logger, max_lines=50):
    """Логирует начало сырых данных для диагностики."""
    lines = content.splitlines()
    logger.debug(f"Получено строк всего: {len(lines)}")
    logger.debug("Первые {} строк сырого ответа:".format(max_lines))
    for i, line in enumerate(lines[:max_lines]):
        logger.debug(f"  Строка {i:3d}: {line[:200]}")  # Логируем первые 200 символов каждой строки

def try_parse_as_csv(raw_text, logger):
    """Пытается разобрать сырой текст как CSV с разными разделителями."""
    separators = [';', ',', '\t', '|']
    for sep in separators:
        try:
            # Пробуем с заголовком и без
            for header_option in [0, None]:
                df = pd.read_csv(StringIO(raw_text), sep=sep, header=header_option, 
                                 on_bad_lines='skip', encoding='utf-8', low_memory=False)
                if df.shape[1] > 1:  # Если больше одного столбца - считаем успехом
                    logger.debug(f"CSV с sep={sep}, header={header_option}: {df.shape[1]} столбцов")
                    return df
        except Exception as e:
            logger.debug(f"Ошибка при парсинге CSV с sep={sep}: {e}")
            continue
    return None

def try_parse_as_json(raw_text, logger):
    """Пытается найти и разобрать JSON структуру в тексте."""
    try:
        # Ищем JSON-подобные структуры
        json_start = raw_text.find('{')
        json_end = raw_text.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_str = raw_text[json_start:json_end]
            data = json.loads(json_str)
            # Пробуем преобразовать в DataFrame
            if isinstance(data, dict):
                # Если это словарь, возможно, данные в одном из полей
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                        df = pd.DataFrame(value)
                        logger.debug(f"Найден JSON с ключом '{key}': {df.shape[1]} столбцов")
                        return df
    except json.JSONDecodeError:
        pass
    except Exception as e:
        logger.debug(f"Ошибка при парсинге JSON: {e}")
    return None

def try_parse_as_text_table(raw_text, logger):
    """Пытается извлечь таблицу из текста, разбивая по строкам и столбцам."""
    lines = raw_text.splitlines()
    # Ищем строки, которые выглядят как данные (содержат числа и разделители)
    data_lines = []
    for line in lines:
        # Проверяем, содержит ли строка цифры и разделители
        if any(char.isdigit() for char in line) and any(sep in line for sep in [';', ',', '\t', '|']):
            data_lines.append(line)
    
    if len(data_lines) > 1:
        # Берем разделитель из первой строки с данными
        first_line = data_lines[0]
        sep = detect_separator(first_line)
        if sep:
            try:
                df = pd.read_csv(StringIO('\n'.join(data_lines)), sep=sep, 
                                 header=None, on_bad_lines='skip', encoding='utf-8')
                logger.debug(f"Текстовая таблица с sep={sep}: {df.shape[1]} столбцов")
                return df
            except Exception as e:
                logger.debug(f"Ошибка при парсинге текстовой таблицы: {e}")
    
    return None

def detect_separator(line):
    """Определяет наиболее вероятный разделитель в строке."""
    separators = {';': 0, ',': 0, '\t': 0, '|': 0}
    for sep in separators:
        separators[sep] = line.count(sep)
    # Возвращаем разделитель с максимальным количеством вхождений
    max_sep = max(separators, key=separators.get)
    return max_sep if separators[max_sep] > 0 else None

# ==================== АНАЛИЗ И СОХРАНЕНИЕ ====================
def analyze_and_save(df, logger):
    """Анализирует данные и сохраняет в Excel."""
    if df is None or df.empty:
        logger.error("Нет данных для анализа и сохранения.")
        return False

    logger.info(f"Данные для анализа: {df.shape[0]} строк, {df.shape[1]} столбцов")
    logger.debug("Структура данных:\n" + str(df.info()))

    # Анализ свежести данных
    analyze_data_freshness(df, logger)

    # Сохранение в Excel
    return save_to_excel(df, logger)

def analyze_data_freshness(df, logger):
    """Анализ свежести данных для проверки динамичности ссылки."""
    logger.info("Анализ свежести данных...")
    
    # Ищем столбцы, содержащие даты
    date_columns = []
    for col in df.columns:
        col_str = str(col).lower()
        if any(keyword in col_str for keyword in ['date', 'time', 'дата', 'время', 'updated']):
            date_columns.append(col)
    
    if date_columns:
        logger.info(f"Найдены столбцы с датами: {date_columns}")
        for col in date_columns:
            try:
                # Пробуем преобразовать в datetime
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                latest = df[col].max()
                if pd.notna(latest):
                    logger.info(f"Последняя дата в '{col}': {latest}")
            except Exception as e:
                logger.debug(f"Не удалось обработать столбец '{col}': {e}")
    else:
        logger.warning("Столбцы с датами не найдены. Проверьте структуру данных.")
        logger.info(f"Все столбцы: {list(df.columns)}")

def save_to_excel(df, logger, filename='moex_bond_rates.xlsx'):
    """Сохраняет DataFrame в Excel файл."""
    try:
        logger.info(f"Сохранение в {filename}...")
        
        # Удаляем старый файл, если существует
        if os.path.exists(filename):
            os.remove(filename)
            logger.debug(f"Удален старый файл {filename}")
        
        # Сохраняем в Excel
        df.to_excel(filename, index=False, engine='openpyxl')
        
        # Автонастройка ширины столбцов
        try:
            from openpyxl import load_workbook
            wb = load_workbook(filename)
            ws = wb.active
            
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
            
            wb.save(filename)
            logger.debug("Настроена ширина столбцов в Excel")
        except Exception as e:
            logger.debug(f"Не удалось настроить ширину столбцов: {e}")
        
        file_size = os.path.getsize(filename) / 1024
        logger.info(f"Файл сохранен: {filename} ({file_size:.2f} KB)")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка сохранения в Excel: {e}", exc_info=True)
        return False

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Основная функция скрипта."""
    start_time = time.time()
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("ЗАГРУЗЧИК ДАННЫХ MOEX (УЛУЧШЕННАЯ ВЕРСИЯ)")
    logger.info("=" * 60)
    
    # URL для загрузки (ваша ссылка)
    target_url = "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"
    
    # Загрузка и обработка данных
    df = download_and_decode_data(target_url, logger)
    
    # Анализ и сохранение
    if df is not None:
        success = analyze_and_save(df, logger)
        if success:
            logger.info("Скрипт выполнен успешно!")
            print(f"\n[УСПЕХ] Данные сохранены в moex_bond_rates.xlsx")
            print(f"Размер таблицы: {df.shape[0]} строк, {df.shape[1]} столбцов")
            print(f"Столбцы: {list(df.columns)[:5]}{'...' if len(df.columns) > 5 else ''}")
        else:
            logger.error("Не удалось сохранить данные.")
    else:
        logger.error("Не удалось загрузить и обработать данные.")
        print("\n[ОШИБКА] Проверьте лог-файл для диагностики.")
    
    # Итоговая статистика
    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Общее время выполнения: {total_time:.2f} секунд")
    logger.info(f"Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()