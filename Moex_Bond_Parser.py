import pandas as pd
import logging
import time
import os
from datetime import datetime
import sys

def setup_logging():
    """
    Настройка системы логирования.
    Теперь используется один перезаписываемый файл лога.
    """
    # Создаем директорию для логов, если её нет
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        print(f"Создана директория для логов: {log_dir}")

    # ФИКС: Используем фиксированное имя файла для перезаписываемого лога
    log_filename = os.path.join(log_dir, "moex_download.log")
    log_format = '%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'

    # Настройка логирования. mode='w' перезаписывает файл при каждом запуске.
    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Инициализировано логирование. Файл: {log_filename} (режим перезаписи)")
    return logger

def download_moex_data(url, logger):
    """
    Загрузка данных с MOEX по URL.
    ФИКС: Изменена кодировка с 'utf-8' на 'windows-1251' для корректного чтения.
    """
    try:
        logger.info(f"Начинаем загрузку данных")
        logger.debug(f"URL: {url}")

        # ФИКС: Изменена кодировка на 'windows-1251'
        df = pd.read_csv(
            url,
            sep=';',
            encoding='windows-1251',  # Основная правка для обработки данных MOEX
            on_bad_lines='warn'
        )

        logger.info(f"УСПЕХ: Загружено {df.shape[0]} строк, {df.shape[1]} столбцов.")
        logger.debug("Столбцы в данных:\n" + "\n".join([f"  - {col}" for col in df.columns]))

        if not df.empty:
            # Выводим в лог срез данных для проверки
            sample_data = df.head(3).to_string(index=False)
            logger.debug(f"Пример данных (первые 3 строки):\n{sample_data}")
            # Логируем типы данных для отладки
            type_info = df.dtypes.to_string()
            logger.debug(f"Типы данных столбцов:\n{type_info}")

        return df

    except UnicodeDecodeError as ude:
        # Специальная обработка ошибки кодировки
        logger.error(f"ОШИБКА КОДИРОВКИ: {ude}", exc_info=True)
        logger.error("Попробуйте изменить кодировку в вызове pd.read_csv на 'cp1251' или 'utf-8-sig'.")
        return None
    except Exception as e:
        logger.error(f"ОШИБКА ПРИ ЗАГРУЗКЕ: {str(e)}", exc_info=True)
        return None

def save_to_excel(df, filename='moex_bond_rates.xlsx', logger=None):
    """Сохранение DataFrame в перезаписываемый Excel файл."""
    if logger:
        logger.info(f"Сохранение данных в Excel: {filename}")

    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='BondRates', index=False)
            workbook = writer.book
            worksheet = writer.sheets['BondRates']

            # Автонастройка ширины столбцов
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        cell_value_length = len(str(cell.value))
                        if cell_value_length > max_length:
                            max_length = cell_value_length
                    except Exception:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        if logger:
            file_size_kb = os.path.getsize(filename) / 1024
            logger.info(f"УСПЕХ: Файл '{filename}' сохранён. Размер: {file_size_kb:.2f} KB.")
        return True

    except Exception as e:
        if logger:
            logger.error(f"ОШИБКА СОХРАНЕНИЯ EXCEL: {str(e)}", exc_info=True)
        return False

def analyze_data_freshness(df, logger):
    """Анализ свежести данных для проверки динамичности ссылки."""
    logger.info("Анализ свежести загруженных данных...")

    # Поиск столбцов, которые могут содержать дату/время
    possible_date_cols = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['date', 'time', 'дата', 'время', 'updated', 'timestamp']):
            possible_date_cols.append(col)

    if not possible_date_cols:
        logger.warning("Столбцы, похожие на дату/время, не найдены. Автоанализ невозможен.")
        return

    logger.info(f"Найдены потенциальные столбцы с датой: {possible_date_cols}")

    for col in possible_date_cols:
        try:
            # Пробуем преобразовать столбец в datetime
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            latest_date = df[col].max()

            if pd.isna(latest_date):
                continue

            logger.info(f"Столбец '{col}': последняя запись - {latest_date}")
            time_diff = datetime.now() - latest_date.replace(tzinfo=None)

            if time_diff.days == 0:
                if time_diff.seconds < 300:  # 5 минут
                    logger.warning(f"  -> Данные очень свежие ({time_diff.seconds} сек). Ссылка ДИНАМИЧЕСКАЯ.")
                else:
                    logger.info(f"  -> Данные за сегодня ({time_diff.seconds // 3600} ч. назад).")
            else:
                logger.info(f"  -> Данные не сегодняшние ({time_diff.days} дн. назад).")

        except Exception as e:
            logger.debug(f"Не удалось проанализировать столбец '{col}': {e}")

def main():
    """Основная функция скрипта."""
    script_start = time.time()
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("НАЧАЛО РАБОТЫ СКРИПТА ЗАГРУЗКИ ДАННЫХ MOEX")
    logger.info("=" * 60)

    # Целевой URL
    target_url = "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"

    # 1. Загрузка данных
    data_frame = download_moex_data(target_url, logger)

    if data_frame is not None:
        # 2. Анализ свежести данных
        analyze_data_freshness(data_frame, logger)

        # 3. Сохранение в Excel
        excel_saved = save_to_excel(data_frame, logger=logger)

        if excel_saved:
            logger.info("Основные этапы скрипта выполнены успешно.")
        else:
            logger.error("Не удалось сохранить данные в Excel.")
    else:
        logger.critical("Загрузка данных не удалась. Дальнейшие этапы пропущены.")

    # Итоговая статистика
    total_time = time.time() - script_start
    logger.info("=" * 60)
    logger.info("ИТОГИ ВЫПОЛНЕНИЯ")
    logger.info(f"Общее время работы: {total_time:.2f} секунд")
    logger.info(f"Дата/время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()