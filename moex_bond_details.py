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
    """Настройка перезаписываемого лога."""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(log_dir, "moex_bond_details.log")
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

# ==================== КОНФИГУРАЦИЯ ====================
# Список ISIN облигаций для сбора данных
BOND_ISINS = [
    "RU000A10D533",
    "RU000A0JV4P3", 
    "RU000A10DB16",
    "RU000A0ZZ885",
    "RU000A106LL5"
]

# Базовый URL API Московской биржи
BASE_URL = "https://iss.moex.com/iss"

# ==================== ФУНКЦИИ ДЛЯ ЗАГРУЗКИ ДАННЫХ ====================
def fetch_bond_data(isin, logger):
    """
    Загружает все доступные данные по облигации по ее ISIN.
    Возвращает словарь с данными из разных endpoint-ов.
    """
    bond_data = {}
    
    # 1. Основная информация по бумаге
    logger.info(f"Загрузка основной информации для {isin}...")
    security_info = fetch_endpoint_data(f"/securities/{isin}.json", logger)
    if security_info:
        bond_data['security'] = security_info
    
    # 2. Информация о торговых площадках (boards)
    logger.info(f"Загрузка информации о торговых площадках для {isin}...")
    boards_info = fetch_endpoint_data(f"/securities/{isin}/boards.json", logger)
    if boards_info:
        bond_data['boards'] = boards_info
    
    # 3. Данные о котировках (рыночные данные)
    logger.info(f"Загрузка рыночных данных для {isin}...")
    marketdata_info = fetch_endpoint_data(f"/securities/{isin}/marketdata.json", logger)
    if marketdata_info:
        bond_data['marketdata'] = marketdata_info
    
    # 4. История котировок (если есть)
    logger.info(f"Загрузка исторических данных для {isin}...")
    
    # Сначала получаем список торговых площадок
    if 'boards' in bond_data and 'boards' in bond_data['boards']:
        boards_df = bond_data['boards']['boards']
        if not boards_df.empty:
            # Берем первую торговую площадку для запроса истории
            board_id = boards_df.iloc[0]['boardid']
            engine = boards_df.iloc[0]['engine']
            market = boards_df.iloc[0]['market']
            
            history_info = fetch_endpoint_data(
                f"/history/engines/{engine}/markets/{market}/boards/{board_id}/securities/{isin}.json", 
                logger
            )
            if history_info:
                bond_data['history'] = history_info
    
    # 5. Данные о купонах
    logger.info(f"Загрузка данных о купонах для {isin}...")
    coupons_info = fetch_endpoint_data(f"/securities/{isin}/coupons.json", logger)
    if coupons_info:
        bond_data['coupons'] = coupons_info
    
    # 6. Данные об амортизации
    logger.info(f"Загрузка данных об амортизации для {isin}...")
    amortizations_info = fetch_endpoint_data(f"/securities/{isin}/amortizations.json", logger)
    if amortizations_info:
        bond_data['amortizations'] = amortizations_info
    
    # 7. Данные об офертах
    logger.info(f"Загрузка данных об офертах для {isin}...")
    offers_info = fetch_endpoint_data(f"/securities/{isin}/offers.json", logger)
    if offers_info:
        bond_data['offers'] = offers_info
    
    # 8. Данные о размещении
    logger.info(f"Загрузка данных о размещении для {isin}...")
    placement_info = fetch_endpoint_data(f"/securities/{isin}/placement.json", logger)
    if placement_info:
        bond_data['placement'] = placement_info
    
    # 9. Индикативные оценки (yield, duration и т.д.)
    logger.info(f"Загрузка индикативных оценок для {isin}...")
    indi_history_info = fetch_endpoint_data(f"/securities/{isin}/indicatorhistory.json", logger)
    if indi_history_info:
        bond_data['indicatorhistory'] = indi_history_info
    
    # 10. Справочник эмитента
    logger.info(f"Загрузка данных эмитента для {isin}...")
    if 'security' in bond_data and 'description' in bond_data['security']:
        desc_df = bond_data['security']['description']
        if not desc_df.empty and 'emitent_id' in desc_df.columns:
            emitent_id = desc_df.iloc[0]['emitent_id']
            if emitent_id:
                emitent_info = fetch_endpoint_data(f"/emitents/{emitent_id}.json", logger)
                if emitent_info:
                    bond_data['emitent'] = emitent_info
    
    return bond_data

def fetch_endpoint_data(endpoint, logger):
    """
    Загружает данные с указанного endpoint API MOEX.
    Возвращает словарь с DataFrame для каждого блока данных.
    """
    try:
        url = f"{BASE_URL}{endpoint}"
        logger.debug(f"Запрос к: {url}")
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.warning(f"Ошибка {response.status_code} для {endpoint}")
            return None
        
        data = response.json()
        result = {}
        
        # Обрабатываем все блоки данных (кроме metadata)
        for key in data:
            if key != 'metadata' and key != 'columns' and isinstance(data[key], list):
                # Проверяем, есть ли columns и data
                if len(data[key]) == 2 and 'columns' in data[key] and 'data' in data[key]:
                    columns = data[key]['columns']
                    rows = data[key]['data']
                    
                    if rows:
                        # Создаем DataFrame
                        df = pd.DataFrame(rows, columns=columns)
                        
                        # Преобразуем строковые даты в datetime
                        for col in df.columns:
                            if 'date' in col.lower() or 'time' in col.lower():
                                try:
                                    df[col] = pd.to_datetime(df[col], errors='coerce')
                                except:
                                    pass
                        
                        result[key] = df
                        logger.debug(f"  Загружено: {key} - {len(df)} строк")
        
        return result if result else None
        
    except Exception as e:
        logger.error(f"Ошибка при запросе {endpoint}: {e}")
        return None

# ==================== ФУНКЦИИ ДЛЯ СОХРАНЕНИЯ ====================
def save_bond_data_to_excel(bond_data, isin, logger):
    """
    Сохраняет все данные по облигации в Excel файл.
    """
    filename = f"{isin}_all_data.xlsx"
    logger.info(f"Сохранение данных для {isin} в {filename}...")
    
    start_time = time.time()
    
    try:
        # Удаляем старый файл, если существует
        if os.path.exists(filename):
            os.remove(filename)
        
        # Создаем Excel writer
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # Сохраняем каждый блок данных на отдельный лист
            sheets_created = 0
            
            for data_key, data_dict in bond_data.items():
                if isinstance(data_dict, dict):
                    for table_key, df in data_dict.items():
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            # Создаем имя листа (максимум 31 символ для Excel)
                            sheet_name = f"{data_key}_{table_key}"[:31]
                            
                            # Сохраняем DataFrame
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            sheets_created += 1
                            
                            # Настраиваем ширину столбцов
                            worksheet = writer.sheets[sheet_name]
                            for i, col in enumerate(df.columns):
                                max_length = max(
                                    df[col].astype(str).str.len().max(),
                                    len(str(col))
                                )
                                worksheet.set_column(i, i, min(max_length + 2, 50))
            
            # Создаем summary лист
            summary_data = create_summary_data(bond_data, isin)
            if summary_data:
                summary_df = pd.DataFrame([summary_data])
                summary_df.to_excel(writer, sheet_name='SUMMARY', index=False)
                sheets_created += 1
        
        file_size_kb = os.path.getsize(filename) / 1024
        save_time = time.time() - start_time
        
        logger.info(f"Файл сохранен: {filename} ({file_size_kb:.1f} KB, {sheets_created} листов, {save_time:.2f} сек)")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка сохранения Excel для {isin}: {e}")
        return False

def create_summary_data(bond_data, isin):
    """Создает сводные данные по облигации."""
    summary = {'ISIN': isin, 'Timestamp': datetime.now()}
    
    try:
        # Базовая информация
        if 'security' in bond_data and 'description' in bond_data['security']:
            desc_df = bond_data['security']['description']
            if not desc_df.empty:
                row = desc_df.iloc[0]
                summary['SHORTNAME'] = row.get('SHORTNAME', '')
                summary['NAME'] = row.get('NAME', '')
                summary['ISSUEDATE'] = row.get('ISSUEDATE', '')
                summary['MATDATE'] = row.get('MATDATE', '')
                summary['FACEVALUE'] = row.get('FACEVALUE', '')
                summary['FACEUNIT'] = row.get('FACEUNIT', '')
        
        # Рыночные данные
        if 'marketdata' in bond_data and 'marketdata' in bond_data['marketdata']:
            market_df = bond_data['marketdata']['marketdata']
            if not market_df.empty:
                row = market_df.iloc[0]
                summary['LAST'] = row.get('LAST', '')
                summary['YIELD'] = row.get('YIELD', '')
                summary['UPDATETIME'] = row.get('UPDATETIME', '')
        
        # Информация о купонах
        if 'coupons' in bond_data and 'coupons' in bond_data['coupons']:
            coupons_df = bond_data['coupons']['coupons']
            summary['COUPONS_COUNT'] = len(coupons_df) if not coupons_df.empty else 0
        
        # Информация об офертах
        if 'offers' in bond_data and 'offers' in bond_data['offers']:
            offers_df = bond_data['offers']['offers']
            summary['OFFERS_COUNT'] = len(offers_df) if not offers_df.empty else 0
        
        # Исторические данные
        if 'history' in bond_data and 'history' in bond_data['history']:
            history_df = bond_data['history']['history']
            summary['HISTORY_RECORDS'] = len(history_df) if not history_df.empty else 0
        
    except Exception as e:
        logger.warning(f"Ошибка создания summary для {isin}: {e}")
    
    return summary

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    """Главная функция скрипта."""
    script_start = time.time()
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("СКРИПТ ДЛЯ СБОРА ВСЕХ ДАННЫХ ПО ОБЛИГАЦИЯМ MOEX")
    logger.info(f"Обработка {len(BOND_ISINS)} облигаций")
    logger.info("=" * 60)
    
    successful_bonds = []
    failed_bonds = []
    
    # Обрабатываем каждую облигацию
    for i, isin in enumerate(BOND_ISINS, 1):
        logger.info(f"\n[{i}/{len(BOND_ISINS)}] Обработка облигации: {isin}")
        
        try:
            # Загружаем данные
            bond_start = time.time()
            bond_data = fetch_bond_data(isin, logger)
            load_time = time.time() - bond_start
            
            if bond_data:
                logger.info(f"Загружено {len(bond_data)} блоков данных за {load_time:.2f} сек")
                
                # Сохраняем в Excel
                save_success = save_bond_data_to_excel(bond_data, isin, logger)
                
                if save_success:
                    successful_bonds.append(isin)
                    logger.info(f"✓ Данные по {isin} успешно сохранены")
                else:
                    failed_bonds.append(isin)
                    logger.error(f"✗ Ошибка сохранения данных для {isin}")
            else:
                failed_bonds.append(isin)
                logger.error(f"✗ Не удалось загрузить данные для {isin}")
                
        except Exception as e:
            failed_bonds.append(isin)
            logger.error(f"✗ Критическая ошибка для {isin}: {e}", exc_info=True)
    
    # Итоговая статистика
    total_time = time.time() - script_start
    
    logger.info("\n" + "=" * 60)
    logger.info("ИТОГИ ВЫПОЛНЕНИЯ")
    logger.info("=" * 60)
    logger.info(f"Успешно обработано: {len(successful_bonds)} из {len(BOND_ISINS)}")
    
    if successful_bonds:
        logger.info("Успешные ISIN:")
        for isin in successful_bonds:
            logger.info(f"  - {isin}")
    
    if failed_bonds:
        logger.warning("Неудачные ISIN:")
        for isin in failed_bonds:
            logger.warning(f"  - {isin}")
    
    logger.info(f"\nОбщее время выполнения: {total_time:.2f} сек")
    logger.info(f"Среднее время на облигацию: {total_time/len(BOND_ISINS):.2f} сек")
    logger.info(f"Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # Вывод в консоль
    print(f"\n{'='*50}")
    print("РЕЗУЛЬТАТЫ СБОРА ДАННЫХ ПО ОБЛИГАЦИЯМ")
    print(f"{'='*50}")
    print(f"Обработано облигаций: {len(BOND_ISINS)}")
    print(f"Успешно: {len(successful_bonds)}")
    print(f"С ошибками: {len(failed_bonds)}")
    
    if successful_bonds:
        print(f"\nФайлы созданы:")
        for isin in successful_bonds:
            print(f"  • {isin}_all_data.xlsx")
    
    if failed_bonds:
        print(f"\nПроблемные ISIN (см. лог):")
        for isin in failed_bonds:
            print(f"  • {isin}")
    
    print(f"\nОбщее время: {total_time:.2f} сек")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()