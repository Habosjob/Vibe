import pandas as pd
import logging
import time
import os
import sys
import requests
from datetime import datetime
import json

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
def setup_logging():
    """Настройка логирования: детальные логи в файл, только важное в консоль."""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(log_dir, "moex_bond_collector.log")
    
    # Создаем логгер
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # Формат для файла (детальный)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    
    # Формат для консоли (краткий)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Обработчик для файла (все сообщения)
    file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Обработчик для консоли (только INFO и выше)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Добавляем обработчики
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Инициализировано логирование. Детальные логи в: {log_filename}")
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
def fetch_endpoint_data(endpoint, logger):
    """
    Загружает данные с указанного endpoint API MOEX.
    Ключевое исправление: используем правильный формат URL с параметрами.
    """
    # ИСПРАВЛЕНИЕ 1: Добавляем параметры для получения данных в правильном формате
    url = f"{BASE_URL}{endpoint}?iss.meta=off&iss.json=extended&limit=unlimited"
    logger.debug(f"Запрос: {url}")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.warning(f"HTTP ошибка {response.status_code} для {endpoint}")
            return None
        
        # Парсим JSON
        data = response.json()
        logger.debug(f"JSON успешно распарсен. Структура: {type(data)}")
        
        # Формат extended: [metadata1, data1, metadata2, data2, ...]
        if isinstance(data, list) and len(data) >= 2:
            result = {}
            
            # Обрабатываем пары metadata/data
            for i in range(0, len(data), 2):
                if i+1 < len(data):
                    metadata = data[i]
                    rows = data[i+1]
                    
                    # metadata - словарь с ключами (названия таблиц)
                    for table_name, table_info in metadata.items():
                        if isinstance(table_info, dict) and 'columns' in table_info:
                            columns = table_info['columns']
                            
                            if rows and isinstance(rows, list) and len(rows) > 0:
                                df = pd.DataFrame(rows, columns=columns)
                                result[table_name] = df
                                logger.debug(f"  Таблица '{table_name}': {len(df)} строк")
                            else:
                                logger.debug(f"  Таблица '{table_name}': нет данных")
            
            return result if result else None
            
    except Exception as e:
        logger.error(f"Ошибка для {endpoint}: {e}")
        return None

def fetch_bond_data(isin, logger):
    """
    Загружает все доступные данные по облигации.
    ИСПРАВЛЕНИЕ 2: Используем правильные endpoint'ы.
    """
    logger.info(f"\nСбор данных для: {isin}")
    
    bond_data = {}
    
    # Ключевые endpoint'ы для облигаций
    endpoints = [
        ("securities", f"/securities/{isin}.json"),
        ("marketdata", f"/securities/{isin}/marketdata.json"),
        ("coupons", f"/securities/{isin}/coupons.json"),
        ("offers", f"/securities/{isin}/offers.json"),
        ("amortizations", f"/securities/{isin}/amortizations.json"),
    ]
    
    for endpoint_name, endpoint_url in endpoints:
        logger.info(f"  Загрузка: {endpoint_name}")
        data = fetch_endpoint_data(endpoint_url, logger)
        
        if data:
            bond_data[endpoint_name] = data
            # Логируем что получили
            for table_name, df in data.items():
                logger.info(f"    ✓ {table_name}: {len(df)} строк")
        else:
            logger.warning(f"    ✗ Нет данных")
    
    return bond_data if bond_data else None

def consolidate_all_data(all_bonds_data, logger):
    """
    Объединяет все данные со всех облигаций в один DataFrame.
    ИСПРАВЛЕНИЕ 3: Все на одном листе + удаление дубликатов.
    """
    logger.info("\n" + "="*60)
    logger.info("КОНСОЛИДАЦИЯ ВСЕХ ДАННЫХ")
    logger.info("="*60)
    
    all_rows = []
    
    for isin, bond_data in all_bonds_data.items():
        logger.info(f"\nОбработка данных для {isin}:")
        
        for source_name, tables in bond_data.items():
            for table_name, df in tables.items():
                if not df.empty:
                    # Добавляем идентификационные колонки
                    df = df.copy()
                    df['ISIN'] = isin
                    df['DATA_SOURCE'] = source_name
                    df['TABLE_NAME'] = table_name
                    df['LOAD_TIMESTAMP'] = datetime.now()
                    
                    all_rows.append(df)
                    logger.info(f"  Добавлено {len(df)} строк из {source_name}.{table_name}")
    
    if not all_rows:
        logger.error("Нет данных для консолидации!")
        return pd.DataFrame()
    
    # Объединяем все данные
    consolidated_df = pd.concat(all_rows, ignore_index=True)
    
    # УДАЛЯЕМ ДУБЛИКАТЫ
    initial_rows = len(consolidated_df)
    
    # Удаляем полные дубликаты по всем колонкам
    consolidated_df = consolidated_df.drop_duplicates()
    
    # Также удаляем дубликаты по ключевым полям (если они есть)
    # Ищем колонки, которые могут быть ключевыми
    key_columns = []
    for col in consolidated_df.columns:
        if col.lower() in ['id', 'isin', 'secid', 'trade_date', 'coupondate', 'recorddate']:
            key_columns.append(col)
    
    if key_columns and len(key_columns) > 1:
        # Удаляем дубликаты по ключевым полям
        consolidated_df = consolidated_df.drop_duplicates(subset=key_columns, keep='first')
    
    final_rows = len(consolidated_df)
    removed = initial_rows - final_rows
    
    logger.info(f"\nРезультаты консолидации:")
    logger.info(f"  Изначально строк: {initial_rows}")
    logger.info(f"  После удаления дубликатов: {final_rows}")
    logger.info(f"  Удалено дубликатов: {removed}")
    logger.info(f"  Столбцов: {len(consolidated_df.columns)}")
    
    return consolidated_df

def save_to_excel(consolidated_df, logger):
    """
    Сохраняет объединенные данные в один Excel файл.
    ВСЕ ДАННЫЕ НА ОДНОМ ЛИСТЕ.
    """
    if consolidated_df.empty:
        logger.error("Нет данных для сохранения!")
        return False
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"all_bonds_data_{timestamp}.xlsx"
    
    logger.info(f"\nСохранение в Excel: {filename}")
    
    try:
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # ВСЁ НА ОДНОМ ЛИСТЕ
            sheet_name = 'All_Bonds_Data'
            consolidated_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Автонастройка ширины столбцов
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(consolidated_df.columns):
                max_length = max(
                    consolidated_df[col].astype(str).str.len().max(),
                    len(str(col))
                )
                worksheet.set_column(i, i, min(max_length + 2, 50))
        
        # Создаем summary файл
        create_summary_file(consolidated_df, filename, logger)
        
        logger.info(f"✓ Файл успешно сохранен")
        logger.info(f"  Размер: {os.path.getsize(filename) / 1024:.1f} KB")
        logger.info(f"  Лист: {sheet_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка сохранения Excel: {e}")
        return False

def create_summary_file(df, excel_filename, logger):
    """Создает текстовый файл с описанием данных."""
    summary_filename = excel_filename.replace('.xlsx', '_SUMMARY.txt')
    
    try:
        with open(summary_filename, 'w', encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write("СВОДКА ПО СОБРАННЫМ ДАННЫМ ОБЛИГАЦИЙ\n")
            f.write("="*60 + "\n\n")
            
            f.write(f"Файл данных: {excel_filename}\n")
            f.write(f"Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"Всего строк: {len(df)}\n")
            f.write(f"Всего столбцов: {len(df.columns)}\n\n")
            
            f.write("Количество записей по ISIN:\n")
            isin_counts = df['ISIN'].value_counts()
            for isin, count in isin_counts.items():
                f.write(f"  {isin}: {count} строк\n")
            
            f.write("\nКоличество записей по источникам данных:\n")
            source_counts = df['DATA_SOURCE'].value_counts()
            for source, count in source_counts.items():
                f.write(f"  {source}: {count} строк\n")
            
            f.write("\nСтолбцы в данных:\n")
            for i, col in enumerate(df.columns, 1):
                f.write(f"{i:3d}. {col}\n")
                if i >= 50:  # Ограничим вывод
                    f.write(f"... и еще {len(df.columns) - 50} столбцов\n")
                    break
        
        logger.info(f"✓ Сводка сохранена в: {summary_filename}")
        
    except Exception as e:
        logger.warning(f"Не удалось создать сводку: {e}")

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Главная функция скрипта."""
    logger = setup_logging()
    
    logger.info("="*60)
    logger.info("СКРИПТ СБОРА ДАННЫХ ПО ОБЛИГАЦИЯМ MOEX (ИСПРАВЛЕННЫЙ)")
    logger.info("="*60)
    logger.info(f"Облигаций для обработки: {len(BOND_ISINS)}")
    logger.info(f"ISIN: {BOND_ISINS}")
    logger.info(f"Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    start_time_total = time.time()
    
    # Собираем данные по всем облигациям
    all_bonds_data = {}
    successful = []
    failed = []
    
    for i, isin in enumerate(BOND_ISINS, 1):
        logger.info(f"\n[{i}/{len(BOND_ISINS)}] Обработка облигации: {isin}")
        bond_start = time.time()
        
        try:
            bond_data = fetch_bond_data(isin, logger)
            
            if bond_data:
                all_bonds_data[isin] = bond_data
                successful.append(isin)
                logger.info(f"✓ Данные получены для {isin}")
            else:
                failed.append(isin)
                logger.warning(f"✗ Не удалось получить данные для {isin}")
                
        except Exception as e:
            failed.append(isin)
            logger.error(f"✗ Ошибка для {isin}: {e}", exc_info=True)
        
        bond_time = time.time() - bond_start
        logger.info(f"Время обработки: {bond_time:.2f} сек")
    
    # Консолидируем и сохраняем данные
    if all_bonds_data:
        logger.info(f"\n{'='*60}")
        logger.info(f"УСПЕШНО ОБРАБОТАНО: {len(successful)} из {len(BOND_ISINS)}")
        logger.info(f"С ОШИБКАМИ: {len(failed)}")
        logger.info(f"{'='*60}")
        
        # Консолидация всех данных
        consolidated_df = consolidate_all_data(all_bonds_data, logger)
        
        # Сохранение в Excel
        if not consolidated_df.empty:
            save_success = save_to_excel(consolidated_df, logger)
            
            if save_success:
                logger.info(f"\n✓ ВСЕ ДАННЫЕ УСПЕШНО СОХРАНЕНЫ")
            else:
                logger.error(f"\n✗ ОШИБКА ПРИ СОХРАНЕНИИ")
        else:
            logger.error(f"\n✗ НЕТ ДАННЫХ ДЛЯ СОХРАНЕНИЯ")
    else:
        logger.error(f"\n✗ НЕ УДАЛОСЬ ПОЛУЧИТЬ ДАННЫЕ НИ ПО ОДНОЙ ОБЛИГАЦИИ")
    
    # Итоги
    total_time = time.time() - start_time_total
    
    logger.info(f"\n{'='*60}")
    logger.info("ИТОГИ ВЫПОЛНЕНИЯ")
    logger.info(f"{'='*60}")
    logger.info(f"Успешно: {len(successful)} облигаций")
    logger.info(f"С ошибками: {len(failed)} облигаций")
    
    if failed:
        logger.info(f"\nОблигации с ошибками:")
        for isin in failed:
            logger.info(f"  • {isin}")
    
    logger.info(f"\nОбщее время выполнения: {total_time:.2f} сек")
    logger.info(f"Завершено: {datetime.now().strftime('%H:%M:%S')}")
    logger.info(f"{'='*60}")
    
    # Краткий вывод в консоль
    print(f"\n{'='*50}")
    print("РЕЗУЛЬТАТЫ СБОРА ДАННЫХ ПО ОБЛИГАЦИЯМ")
    print(f"{'='*50}")
    
    if successful:
        print(f"✓ Успешно обработано: {len(successful)} облигаций")
        print(f"✗ С ошибками: {len(failed)} облигаций")
        print(f"\nСозданные файлы:")
        print("  • all_bonds_data_YYYYMMDD_HHMMSS.xlsx - все данные на одном листе")
        print("  • all_bonds_data_YYYYMMDD_HHMMSS_SUMMARY.txt - сводка")
        print(f"\nДетальный лог: logs/moex_bond_collector.log")
    else:
        print("✗ Не удалось получить данные ни по одной облигации")
        print("  Проверьте лог-файл для деталей")
    
    print(f"\nОбщее время: {total_time:.2f} сек")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()