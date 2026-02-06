import pandas as pd
import logging
import time
import os
from datetime import datetime
import sys
import requests
from io import StringIO

def setup_logging():
    """
    Настройка системы логирования.
    Используется один перезаписываемый файл лога.
    """
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        print(f"Создана директория для логов: {log_dir}")

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
    logger.info(f"Инициализировано логирование. Файл: {log_filename} (режим перезаписи)")
    return logger

def download_moex_data(url, logger):
    """
    Загрузка данных с MOEX по URL.
    Исправлено: Используется requests с заголовками для получения корректного CSV.
    """
    try:
        logger.info(f"Начинаем загрузку данных")
        logger.debug(f"URL: {url}")

        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Добавляем заголовки HTTP-запроса
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'windows-1251'  # Устанавливаем кодировку ответа
        logger.debug(f"HTTP статус ответа: {response.status_code}")

        # Проверяем первые 500 символов для диагностики
        sample_content = response.text[:500]
        logger.debug(f"Начало полученного ответа (500 символов):\n---\n{sample_content}\n---")

        # Используем StringIO для передачи текста в pandas
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, sep=';', on_bad_lines='warn')

        logger.info(f"УСПЕХ: Загружено {df.shape[0]} строк, {df.shape[1]} столбцов.")
        logger.debug("Столбцы в данных:\n" + "\n".join([f"  - {col}" for col in df.columns]))

        if not df.empty and df.shape[1] > 1:  # Проверяем, что столбцов больше одного
            sample_data = df.head(3).to_string(index=False)
            logger.debug(f"Пример данных (первые 3 строки):\n{sample_data}")
        elif df.shape[1] == 1:
            # Если все еще одна колонка, логируем ошибку структуры
            logger.error(f"ОШИБКА СТРУКТУРЫ: Данные содержат только 1 столбец '{df.columns[0]}'.")
            logger.error("Это означает, что получен невалидный CSV. Проверьте URL и параметры запроса.")
            return None

        return df

    except requests.exceptions.RequestException as req_err:
        logger.error(f"ОШИБКА СЕТЕВОГО ЗАПРОСА: {req_err}", exc_info=False)
        return None
    except Exception as e:
        logger.error(f"ОБЩАЯ ОШИБКА ПРИ ЗАГРУЗКЕ: {str(e)}", exc_info=True)
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
    if df is None or df.empty:
        logger.warning("Анализ свежести пропущен: DataFrame пуст или не загружен.")
        return

    logger.info("Анализ свежести загруженных данных...")

    possible_date_cols = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['date', 'time', 'дата', 'время', 'updated', 'timestamp']):
            possible_date_cols.append(col)

    if not possible_date_cols:
        logger.warning("Столбцы, похожие на дату/время, не найдены. Автоанализ невозможен.")
        # Выведем все названия столбцов для ручной проверки
        logger.info(f"Все доступные столбцы: {list(df.columns)}")
        return

    logger.info(f"Найдены потенциальные столбцы с датой: {possible_date_cols}")

    for col in possible_date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            latest_date = df[col].max()

            if pd.isna(latest_date):
                continue

            logger.info(f"Столбец '{col}': последняя запись - {latest_date}")
            time_diff = datetime.now() - latest_date.replace(tzinfo=None)

            if time_diff.days == 0:
                if time_diff.seconds < 300:
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
    logger.info("НАЧАЛО РАБОТЫ СКРИПТА ЗАГРУЗКИ ДАННЫХ MOEX (ИСПРАВЛЕННАЯ ВЕРСИЯ)")
    logger.info("=" * 60)

    target_url = "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?sec_type=stock_ofz_bond,stock_cb_bond,stock_subfederal_bond,stock_municipal_bond,stock_corporate_bond,stock_exchange_bond&iss.dp=comma&iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&iss.only=rates&limit=unlimited&lang=ru"

    data_frame = download_moex_data(target_url, logger)

    if data_frame is not None and not data_frame.empty:
        analyze_data_freshness(data_frame, logger)
        excel_saved = save_to_excel(data_frame, logger=logger)

        if excel_saved:
            logger.info("Основные этапы скрипта выполнены успешно.")
            # Краткий вывод в консоль для быстрой проверки
            print(f"\n[КРАТКИЙ ОТЧЕТ]")
            print(f"Загружено: {data_frame.shape[0]} строк, {data_frame.shape[1]} столбцов.")
            print(f"Столбцы: {', '.join(list(data_frame.columns)[:5])}{'...' if len(data_frame.columns) > 5 else ''}")
            print(f"Сохранено в: moex_bond_rates.xlsx")
        else:
            logger.error("Не удалось сохранить данные в Excel.")
    else:
        logger.critical("Загрузка данных не удалась или получены пустые данные. Дальнейшие этапы пропущены.")

    total_time = time.time() - script_start
    logger.info("=" * 60)
    logger.info("ИТОГИ ВЫПОЛНЕНИЯ")
    logger.info(f"Общее время работы: {total_time:.2f} секунд")
    logger.info(f"Дата/время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()