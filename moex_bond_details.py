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

    log_filename = os.path.join(log_dir, "moex_bond_consolidated.log")
    
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

# ==================== ФУНКЦИИ ДЛЯ ЗАГРУЗКИ ДАННЫХ ====================
def get_security_info(isin, logger):
    """
    Получает основную информацию по бумаге.
    Использует корректный endpoint API MOEX.
    """
    url = f"https://iss.moex.com/iss/securities.json?q={isin}&iss.meta=off&iss.json=extended"
    logger.debug(f"Запрос основной информации: {url}")
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        if response.status_code == 200:
            data = response.json()
            # Структура ответа: [metadata1, data1, metadata2, data2, ...]
            if len(data) >= 2 and isinstance(data[1], list) and len(data[1]) > 0:
                # Первый блок данных - информация о бумагах
                columns = data[0]['securities']['columns']
                rows = data[1]  # Данные securities
                
                if rows and len(rows) > 0:
                    # Ищем строку с нашим ISIN
                    for row in rows:
                        if row[columns.index('ISIN')] == isin:
                            df = pd.DataFrame([row], columns=columns)
                            logger.info(f"Найдена основная информация для {isin}")
                            return df
    except Exception as e:
        logger.error(f"Ошибка при получении основной информации для {isin}: {e}")
    
    return pd.DataFrame()  # Пустой DataFrame

def get_market_data(isin, logger):
    """
    Получает рыночные данные по бумаге.
    """
    url = f"https://iss.moex.com/iss/securities/{isin}/marketdata.json?iss.meta=off&iss.json=extended"
    logger.debug(f"Запрос рыночных данных: {url}")
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if len(data) >= 2 and isinstance(data[1], list) and len(data[1]) > 0:
                columns = data[0]['marketdata']['columns']
                rows = data[1]  # Данные marketdata
                
                if rows and len(rows) > 0:
                    df = pd.DataFrame(rows, columns=columns)
                    logger.info(f"Загружены рыночные данные для {isin}: {len(df)} строк")
                    return df
    except Exception as e:
        logger.error(f"Ошибка при получении рыночных данных для {isin}: {e}")
    
    return pd.DataFrame()

def get_coupons(isin, logger):
    """
    Получает данные о купонах.
    """
    url = f"https://iss.moex.com/iss/securities/{isin}/coupons.json?iss.meta=off&iss.json=extended"
    logger.debug(f"Запрос данных о купонах: {url}")
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if len(data) >= 2 and isinstance(data[1], list):
                columns = data[0]['coupons']['columns']
                rows = data[1]  # Данные coupons
                
                if rows and len(rows) > 0:
                    df = pd.DataFrame(rows, columns=columns)
                    logger.info(f"Загружены данные о купонах для {isin}: {len(df)} записей")
                    return df
    except Exception as e:
        logger.error(f"Ошибка при получении данных о купонах для {isin}: {e}")
    
    return pd.DataFrame()

def get_bond_data(isin, logger):
    """
    Собирает ВСЕ доступные данные по одной облигации.
    """
    logger.info(f"\nСбор данных для облигации: {isin}")
    logger.info("-" * 50)
    
    all_data = []
    
    # 1. Основная информация
    logger.info("1. Загрузка основной информации...")
    sec_info = get_security_info(isin, logger)
    if not sec_info.empty:
        # Добавляем идентификатор источника данных
        sec_info['data_source'] = 'security_info'
        sec_info['isin'] = isin
        all_data.append(sec_info)
        logger.info(f"   Загружено: {len(sec_info)} строк, {len(sec_info.columns)} столбцов")
    else:
        logger.warning("   Основная информация не найдена")
    
    # 2. Рыночные данные
    logger.info("2. Загрузка рыночных данных...")
    market_data = get_market_data(isin, logger)
    if not market_data.empty:
        market_data['data_source'] = 'market_data'
        market_data['isin'] = isin
        all_data.append(market_data)
        logger.info(f"   Загружено: {len(market_data)} строк, {len(market_data.columns)} столбцов")
    else:
        logger.warning("   Рыночные данные не найдены")
    
    # 3. Данные о купонах
    logger.info("3. Загрузка данных о купонах...")
    coupons_data = get_coupons(isin, logger)
    if not coupons_data.empty:
        coupons_data['data_source'] = 'coupons'
        coupons_data['isin'] = isin
        all_data.append(coupons_data)
        logger.info(f"   Загружено: {len(coupons_data)} строк, {len(coupons_data.columns)} столбцов")
    else:
        logger.warning("   Данные о купонах не найдены")
    
    # 4. Попробуем получить данные через общий endpoint для облигаций
    logger.info("4. Поиск дополнительных данных...")
    try:
        # Общий запрос для облигаций
        bonds_url = "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization.json?iss.meta=off&iss.json=extended"
        response = requests.get(bonds_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if len(data) >= 2 and isinstance(data[1], list) and len(data[1]) > 0:
                columns = data[0]['bondization']['columns']
                rows = data[1]
                
                if rows and len(rows) > 0:
                    # Фильтруем по ISIN
                    isin_index = columns.index('ISIN') if 'ISIN' in columns else -1
                    if isin_index >= 0:
                        filtered_rows = [row for row in rows if row[isin_index] == isin]
                        if filtered_rows:
                            df = pd.DataFrame(filtered_rows, columns=columns)
                            df['data_source'] = 'bondization'
                            df['isin'] = isin
                            all_data.append(df)
                            logger.info(f"   Найдено записей в bondization: {len(df)}")
    except Exception as e:
        logger.debug(f"   Ошибка при запросе bondization: {e}")
    
    # Объединяем все данные
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True, sort=False)
        logger.info(f"Итого собрано данных для {isin}: {len(combined_df)} строк")
        return combined_df
    else:
        logger.warning(f"Не удалось собрать данные для {isin}")
        return pd.DataFrame()

# ==================== ОБРАБОТКА И СОХРАНЕНИЕ ====================
def process_and_save_all_bonds(bond_isins, logger):
    """
    Обрабатывает все облигации, объединяет данные и сохраняет в один файл.
    """
    logger.info(f"\nНачало обработки {len(bond_isins)} облигаций")
    logger.info("=" * 60)
    
    all_bonds_data = []
    processed_count = 0
    
    for i, isin in enumerate(bond_isins, 1):
        logger.info(f"\n[{i}/{len(bond_isins)}] Обработка: {isin}")
        start_time = time.time()
        
        try:
            # Получаем данные по облигации
            bond_df = get_bond_data(isin, logger)
            
            if not bond_df.empty:
                all_bonds_data.append(bond_df)
                processed_count += 1
                logger.info(f"✓ Данные успешно получены ({len(bond_df)} строк)")
            else:
                logger.warning(f"✗ Не удалось получить данные для {isin}")
            
            elapsed = time.time() - start_time
            logger.info(f"Время обработки: {elapsed:.2f} сек")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке {isin}: {e}", exc_info=True)
    
    # Объединяем все данные
    if all_bonds_data:
        logger.info(f"\nОбъединение данных всех облигаций...")
        final_df = pd.concat(all_bonds_data, ignore_index=True, sort=False)
        
        # УДАЛЯЕМ ДУБЛИКАТЫ
        initial_count = len(final_df)
        final_df = final_df.drop_duplicates()
        removed_count = initial_count - len(final_df)
        
        logger.info(f"Итоговый датасет: {len(final_df)} строк, {len(final_df.columns)} столбцов")
        if removed_count > 0:
            logger.info(f"Удалено дубликатов: {removed_count} строк")
        
        # Анализируем структуру данных
        logger.info("\nАнализ структуры данных:")
        logger.info(f"- Уникальных ISIN: {final_df['isin'].nunique()}")
        logger.info(f"- Источников данных: {final_df['data_source'].unique().tolist()}")
        
        # Сохраняем в Excel
        return save_consolidated_excel(final_df, logger, processed_count)
    else:
        logger.error("Не удалось собрать данные ни по одной облигации")
        return False

def save_consolidated_excel(df, logger, bond_count):
    """
    Сохраняет объединенные данные в один Excel файл на одном листе.
    """
    filename = f"all_bonds_consolidated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    logger.info(f"\nСохранение данных в файл: {filename}")
    
    try:
        # Создаем Excel writer
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # Сохраняем все данные на одном листе
            sheet_name = 'All_Bonds_Data'
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Настраиваем ширину столбцов
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).str.len().max(),
                    len(str(col))
                )
                worksheet.set_column(i, i, min(max_length + 2, 50))
        
        file_size = os.path.getsize(filename) / 1024
        logger.info(f"✓ Файл успешно сохранен")
        logger.info(f"  Размер: {file_size:.1f} KB")
        logger.info(f"  Лист: {sheet_name}")
        logger.info(f"  Облигаций в файле: {bond_count}")
        
        # Создаем дополнительный файл со статистикой
        save_statistics(df, logger)
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка сохранения Excel: {e}", exc_info=True)
        return False

def save_statistics(df, logger):
    """
    Сохраняет статистику по данным в отдельный файл.
    """
    try:
        stats_filename = "bonds_data_statistics.txt"
        
        with open(stats_filename, 'w', encoding='utf-8') as f:
            f.write("СТАТИСТИКА ПО ДАННЫМ ОБЛИГАЦИЙ\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Всего строк: {len(df)}\n")
            f.write(f"Всего столбцов: {len(df.columns)}\n\n")
            
            f.write("Количество записей по ISIN:\n")
            isin_counts = df['isin'].value_counts()
            for isin, count in isin_counts.items():
                f.write(f"  {isin}: {count} строк\n")
            
            f.write("\nКоличество записей по источникам данных:\n")
            source_counts = df['data_source'].value_counts()
            for source, count in source_counts.items():
                f.write(f"  {source}: {count} строк\n")
            
            f.write("\nСписок столбцов:\n")
            for i, col in enumerate(df.columns, 1):
                f.write(f"  {i:2d}. {col}\n")
        
        logger.info(f"Статистика сохранена в: {stats_filename}")
        
    except Exception as e:
        logger.warning(f"Не удалось сохранить статистику: {e}")

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Главная функция скрипта."""
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("СКРИПТ СБОРА И КОНСОЛИДАЦИИ ДАННЫХ ПО ОБЛИГАЦИЯМ MOEX")
    logger.info("=" * 60)
    logger.info(f"Облигаций для обработки: {len(BOND_ISINS)}")
    logger.info(f"Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    start_time_total = time.time()
    
    # Обрабатываем все облигации
    success = process_and_save_all_bonds(BOND_ISINS, logger)
    
    # Итоги выполнения
    total_time = time.time() - start_time_total
    
    logger.info("\n" + "=" * 60)
    logger.info("ИТОГИ ВЫПОЛНЕНИЯ")
    logger.info("=" * 60)
    
    if success:
        logger.info("✓ Скрипт выполнен успешно!")
        logger.info(f"✓ Данные сохранены в консолидированном Excel-файле")
    else:
        logger.error("✗ В процессе выполнения возникли ошибки")
    
    logger.info(f"\nОбщее время выполнения: {total_time:.2f} сек")
    logger.info(f"Завершено: {datetime.now().strftime('%H:%M:%S')}")
    logger.info("=" * 60)
    
    # Краткий вывод в консоль
    print(f"\n{'='*50}")
    print("РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ СКРИПТА")
    print(f"{'='*50}")
    
    if success:
        print("✓ Данные успешно собраны и сохранены")
        print("✓ Все данные объединены в одном файле Excel")
        print("✓ Дубликаты удалены")
        print(f"\nПроверьте файлы:")
        print("  • all_bonds_consolidated_*.xlsx - основные данные")
        print("  • bonds_data_statistics.txt - статистика")
        print("  • logs/moex_bond_consolidated.log - детальный лог")
    else:
        print("✗ При выполнении скрипта возникли ошибки")
        print("  Проверьте лог-файл для деталей")
    
    print(f"\nОбщее время: {total_time:.2f} сек")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()