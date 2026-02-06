import pandas as pd
import logging
import time
import os
import sys
import requests
from datetime import datetime
from io import StringIO

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

# ==================== ОПРЕДЕЛЕНИЕ ФОРМАТА И КОДИРОВКИ ====================
def analyze_and_find_header(raw_text, logger):
    """
    Анализирует сырые данные, чтобы найти заголовок таблицы.
    Возвращает номер строки с заголовком и кодировку.
    """
    lines = raw_text.splitlines()
    
    logger.info("Поиск заголовка таблицы...")
    
    # Ищем строку с максимальным количеством точек с запятой (скорее всего, это заголовок)
    max_semicolons = 0
    header_line = 0
    
    for i, line in enumerate(lines[:50]):  # Проверяем только первые 50 строк
        semicolon_count = line.count(';')
        if semicolon_count > max_semicolons:
            max_semicolons = semicolon_count
            header_line = i
    
    if max_semicolons == 0:
        logger.error("Не найдено строк с точками с запятой!")
        return None
    
    logger.info(f"Найден заголовок в строке {header_line} с {max_semicolons + 1} столбцами")
    logger.info(f"Пример заголовка: {lines[header_line][:200]}...")
    
    return {
        'skiprows': header_line,  # Пропускаем строки до заголовка
        'sep': ';',
        'header': 0,  # Используем найденную строку как заголовок
        'on_bad_lines': 'skip'
    }

# ==================== ОСНОВНАЯ ФУНКЦИЯ ЗАГРУЗКИ ====================
def download_moex_data(url, logger):
    """Загружает и корректно обрабатывает данные с MOEX."""
    try:
        logger.info("Начало загрузки данных...")
        
        # Загружаем сырые данные
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=45)
        
        # Пробуем разные кодировки, начиная с наиболее вероятных для MOEX
        encodings_to_try = ['windows-1251', 'cp1251', 'utf-8', 'iso-8859-5', 'koi8-r']
        raw_text = None
        used_encoding = None
        
        for encoding in encodings_to_try:
            try:
                response.encoding = encoding
                raw_text = response.text
                # Проверяем, что декодирование прошло успешно
                if raw_text and not '����' in raw_text[:1000]:
                    used_encoding = encoding
                    logger.info(f"Успешное декодирование с кодировкой: {encoding}")
                    break
            except Exception as e:
                logger.debug(f"Ошибка с кодировкой {encoding}: {e}")
                continue
        
        if not raw_text:
            logger.error("Не удалось декодировать данные ни с одной кодировкой!")
            return None
        
        # Анализируем данные для поиска заголовка
        csv_params = analyze_and_find_header(raw_text, logger)
        if csv_params is None:
            logger.error("Не удалось найти заголовок таблицы!")
            return None
        
        # Парсим CSV с правильными параметрами
        logger.info("Парсинг CSV...")
        df = pd.read_csv(StringIO(raw_text), **csv_params)
        
        logger.info(f"Загружено: {df.shape[0]} строк, {df.shape[1]} столбцов")
        
        # Проверяем наличие кракозябров в названиях столбцов
        for col in df.columns:
            if '�' in str(col):
                logger.warning(f"Найден некорректный символ в столбце: {col}")
        
        # Проверяем первую строку данных
        if not df.empty:
            logger.info("Проверка первой строки данных:")
            for col in df.columns[:5]:  # Только первые 5 столбцов для лога
                value = str(df.iloc[0][col])[:50]
                if '�' in value:
                    logger.warning(f"Столбец '{col}': содержит некорректные символы")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}", exc_info=True)
        return None

# ==================== ФИКСИРОВАННОЕ СОХРАНЕНИЕ В EXCEL ====================
def save_to_excel_fixed(df, filename='moex_bond_rates.xlsx', logger=None):
    """Сохраняет DataFrame в Excel с правильной кодировкой."""
    if logger:
        logger.info(f"Сохранение в Excel: {filename}")
    
    start_time = time.time()
    
    try:
        # Удаляем старый файл
        if os.path.exists(filename):
            os.remove(filename)
        
        # Важное исправление: используем engine='xlsxwriter' для лучшей поддержки кодировок
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # Преобразуем все строковые данные в правильную кодировку
            for column in df.columns:
                if df[column].dtype == 'object':
                    # Преобразуем в строку и заменяем некорректные символы
                    df[column] = df[column].astype(str).str.encode('utf-8', errors='ignore').str.decode('utf-8')
            
            # Сохраняем данные
            df.to_excel(writer, sheet_name='BondRates', index=False)
            
            # Настройка ширины столбцов (опционально)
            worksheet = writer.sheets['BondRates']
            for i, col in enumerate(df.columns):
                # Определяем максимальную длину в столбце
                max_length = max(
                    df[col].astype(str).str.len().max(),
                    len(str(col))
                )
                # Ограничиваем ширину столбца
                worksheet.set_column(i, i, min(max_length + 2, 50))
        
        file_size_kb = os.path.getsize(filename) / 1024
        save_time = time.time() - start_time
        
        if logger:
            logger.info(f"Файл успешно сохранен за {save_time:.2f} сек.")
            logger.info(f"Размер файла: {file_size_kb:.2f} KB")
            logger.info("Проверка содержимого файла:")
            logger.info(f"  - Столбцы: {len(df.columns)}")
            logger.info(f"  - Строки: {len(df)}")
            logger.info(f"  - Пример столбцов: {list(df.columns)[:3]}...")
        
        return True
        
    except Exception as e:
        if logger:
            logger.error(f"Ошибка сохранения Excel: {e}", exc_info=True)
        return False

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Главная функция скрипта."""
    script_start = time.time()
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("ИСПРАВЛЕННЫЙ ЗАГРУЗЧИК MOEX (FIXED ENCODING)")
    logger.info("=" * 60)
    
    # URL для загрузки
    target_url = "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"
    
    # 1. Загрузка данных
    logger.info("Этап 1: Загрузка данных с MOEX...")
    df = download_moex_data(target_url, logger)
    
    if df is None or df.empty:
        logger.error("Не удалось загрузить данные!")
        return
    
    # 2. Проверка данных перед сохранением
    logger.info("Этап 2: Проверка данных...")
    logger.info(f"Форма данных: {df.shape}")
    logger.info(f"Типы данных:\n{df.dtypes}")
    
    # Выводим информацию о столбцах для проверки
    logger.info("Пример столбцов и данных:")
    for i, col in enumerate(df.columns[:5]):  # Первые 5 столбцов
        sample = str(df[col].iloc[0])[:50] if not df.empty else "пусто"
        logger.info(f"  {i+1}. {col}: {sample}")
    
    # 3. Сохранение в Excel
    logger.info("Этап 3: Сохранение в Excel...")
    success = save_to_excel_fixed(df, logger=logger)
    
    if success:
        logger.info("=" * 60)
        logger.info("УСПЕХ! Данные сохранены без искажений кодировки")
        logger.info("=" * 60)
        
        # Вывод в консоль
        print(f"\n✅ ДАННЫЕ УСПЕШНО СОХРАНЕНЫ")
        print(f"   Файл: moex_bond_rates.xlsx")
        print(f"   Размер: {df.shape[0]} строк, {df.shape[1]} столбцов")
        print(f"   Пример столбцов: {list(df.columns)[:3]}...")
    else:
        logger.error("Ошибка при сохранении файла!")
        print(f"\n❌ Ошибка при сохранении файла. Проверьте лог.")
    
    # 4. Итоговая статистика
    total_time = time.time() - script_start
    logger.info("=" * 60)
    logger.info(f"ОБЩЕЕ ВРЕМЯ: {total_time:.2f} сек")
    logger.info(f"ЗАВЕРШЕНО: {datetime.now().strftime('%H:%M:%S')}")
    logger.info("=" * 60)
    
    print(f"\n⏱️  Общее время выполнения: {total_time:.2f} сек")

if __name__ == "__main__":
    main()