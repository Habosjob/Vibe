import pandas as pd
import logging
import time
import os
from datetime import datetime
import sys

def setup_logging():
    """Настройка системы логирования"""
    # Создаем директорию для логов, если её нет
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Настраиваем формат логов
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    log_filename = f"logs/moex_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Настройка логирования в файл и консоль
    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def download_moex_data(url):
    """Загрузка данных с MOEX по URL"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Начинаем загрузку данных по URL: {url}")
        logger.info("Типы бумаг в запросе: ОФЗ, облигации ЦБ, субфедеральные, муниципальные, корпоративные, биржевые облигации")
        
        # Загружаем CSV с MOEX (разделитель - точка с запятой)
        df = pd.read_csv(
            url, 
            sep=';', 
            encoding='utf-8',
            on_bad_lines='warn'  # Предупреждаем о проблемных строках
        )
        
        logger.info(f"Данные успешно загружены. Размер данных: {df.shape[0]} строк, {df.shape[1]} столбцов")
        logger.debug(f"Столбцы в данных: {list(df.columns)}")
        
        # Выводим информацию о первых строках
        if not df.empty:
            logger.info(f"Первые 3 строки данных:\n{df.head(3).to_string()}")
            logger.info(f"Типы данных:\n{df.dtypes.to_string()}")
            
            # Проверяем наличие временных меток
            date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            if date_columns:
                logger.info(f"Столбцы с датой/временем: {date_columns}")
        
        return df
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {str(e)}", exc_info=True)
        return None

def save_to_excel(df, filename='moex_bond_rates.xlsx'):
    """Сохранение DataFrame в Excel"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Начинаем сохранение данных в Excel файл: {filename}")
        
        # Используем openpyxl как движок для Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='BondRates', index=False)
            
            # Получаем объект workbook для дополнительных настроек
            workbook = writer.book
            worksheet = writer.sheets['BondRates']
            
            # Автоматическая ширина столбцов
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"Данные успешно сохранены в файл: {filename}")
        logger.info(f"Размер сохраненного файла: {os.path.getsize(filename) / 1024:.2f} KB")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при сохранении в Excel: {str(e)}", exc_info=True)
        return False

def main():
    """Основная функция скрипта"""
    start_time = time.time()
    
    # Настройка логирования
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("СКРИПТ ДЛЯ ЗАГРУЗКИ ДАННЫХ С MOEX")
    logger.info(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # URL для загрузки
    url = "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"
    
    # Шаг 1: Загрузка данных
    logger.info("\n1. ЗАГРУЗКА ДАННЫХ С MOEX")
    data = download_moex_data(url)
    
    if data is not None:
        # Шаг 2: Сохранение в Excel
        logger.info("\n2. СОХРАНЕНИЕ В EXCEL")
        excel_filename = 'moex_bond_rates.xlsx'
        
        # Удаляем старый файл, если существует
        if os.path.exists(excel_filename):
            logger.warning(f"Найден существующий файл {excel_filename}. Он будет перезаписан.")
            os.remove(excel_filename)
        
        save_success = save_to_excel(data, excel_filename)
        
        # Шаг 3: Дополнительная информация
        if save_success:
            logger.info("\n3. ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ")
            logger.info(f"Пример данных (первые 5 строк):")
            print(data.head().to_string())
            
            logger.info(f"\nСтатистика по числовым столбцам:")
            numeric_cols = data.select_dtypes(include=['int64', 'float64']).columns
            if len(numeric_cols) > 0:
                print(data[numeric_cols].describe().to_string())
            
            # Проверяем динамичность данных
            logger.info("\n4. ПРОВЕРКА ДИНАМИЧНОСТИ ДАННЫХ")
            # Ищем столбцы с датой/временем
            date_cols = [col for col in data.columns if 'date' in col.lower() or 'время' in col.lower() or 'time' in col.lower()]
            if date_cols:
                latest_date = None
                for col in date_cols:
                    if col in data.columns and not data[col].empty:
                        try:
                            dates = pd.to_datetime(data[col], errors='coerce')
                            latest = dates.max()
                            if pd.notnull(latest):
                                latest_date = latest
                                logger.info(f"Последняя дата в столбце '{col}': {latest}")
                                break
                        except:
                            continue
                
                if latest_date:
                    current_time = datetime.now()
                    time_diff = current_time - latest_date.replace(tzinfo=None) if latest_date.tzinfo else current_time - latest_date
                    logger.info(f"Разница с текущим временем: {time_diff}")
                    
                    if time_diff.days == 0 and time_diff.seconds < 3600:  # Менее часа
                        logger.warning("Данные ОЧЕНЬ СВЕЖИЕ (менее часа). Ссылка, вероятно, ДИНАМИЧЕСКАЯ.")
                    elif time_diff.days == 0:
                        logger.info("Данные за сегодня. Ссылка, вероятно, динамическая.")
                    else:
                        logger.info("Данные не сегодняшние. Может быть как статичная, так и динамическая ссылка.")
    
    # Итоговая информация
    execution_time = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("ИНФОРМАЦИЯ О ВЫПОЛНЕНИИ")
    logger.info(f"Общее время выполнения: {execution_time:.2f} секунд")
    logger.info(f"Завершено в: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Потребление памяти: {sys.getsizeof(data) / 1024 / 1024:.2f} MB" if data is not None else "Данные не загружены")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()