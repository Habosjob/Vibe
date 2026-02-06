"""
MOEX Bonds Parser - Парсер облигаций Московской биржи
Версия 4.0 - полностью стабильная
"""

import logging
import time
import json
import traceback
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class MOEXBondsParser:
    """Класс для парсинга облигаций с MOEX"""
    
    def __init__(self, excel_path: str = 'moex_bonds.xlsx', log_path: str = 'bonds_parser.log'):
        """
        Инициализация парсера
        
        Args:
            excel_path: Путь для сохранения Excel файла
            log_path: Путь для сохранения лог файла
        """
        self.excel_path = excel_path
        self.log_path = log_path
        self.session = self._create_session()
        self.base_url = 'https://iss.moex.com/iss'
        
        # Настройка логирования
        self.setup_logging()
        
        # Логирование начала работы
        logging.info("=" * 80)
        logging.info("MOEX BONDS PARSER - ЗАПУСК (Версия 4.0)")
        logging.info(f"Дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Файл Excel: {self.excel_path}")
        logging.info(f"Лог файл: {self.log_path}")
        logging.info(f"Базовый URL MOEX API: {self.base_url}")
        logging.debug("Инициализация парсера завершена")
        
    def _create_session(self) -> requests.Session:
        """Создание HTTP сессии с ретраями"""
        logging.debug("Создание HTTP сессии с настройками ретраев")
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[500, 502, 503, 504, 429],
            allowed_methods=['GET']
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        logging.debug("HTTP сессия создана с параметрами: total_retries=3, backoff_factor=1.0")
        return session
    
    def setup_logging(self) -> None:
        """Настройка расширенного логирования в файл"""
        try:
            # Очищаем файл при каждом запуске
            with open(self.log_path, 'w', encoding='utf-8') as f:
                f.write(f"=== ЛОГ ФАЙЛ MOEX BONDS PARSER ===\n")
                f.write(f"Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Версия парсера: 4.0 (стабильная)\n")
                f.write("=" * 80 + "\n\n")
            
            # Создаем логгер
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
            
            # Удаляем старые обработчики
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            
            # Обработчик для файла (DEBUG уровень, перезапись)
            file_handler = logging.FileHandler(self.log_path, mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_format = logging.Formatter(
                '%(asctime)s.%(msecs)03d [%(levelname)-8s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_format)
            
            # Обработчик для консоль
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_format = logging.Formatter(
                '[%(levelname)s] %(asctime)s - %(message)s',
                datefmt='%H:%M:%S'
            )
            console_handler.setFormatter(console_format)
            
            # Добавляем обработчики
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            
            logging.debug("Логирование настроено: файл (DEBUG), консоль (INFO)")
            
        except Exception as e:
            print(f"КРИТИЧЕСКАЯ ОШИБКА при настройке логирования: {str(e)}")
            raise
    
    def _make_request(self, url: str, params: Dict = None, operation: str = "") -> Optional[Dict]:
        """
        Универсальный метод для выполнения HTTP запросов с детальным логированием
        
        Args:
            url: URL для запроса
            params: Параметры запроса
            operation: Описание операции для логирования
        
        Returns:
            Ответ в виде словаря или None при ошибке
        """
        request_id = f"REQ_{int(time.time() * 1000) % 10000:04d}"
        logging.debug(f"[{request_id}] Начало запроса: {operation}")
        logging.debug(f"[{request_id}] URL: {url}")
        if params:
            logging.debug(f"[{request_id}] Параметры: {params}")
        
        start_time = time.time()
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            request_time = time.time() - start_time
            
            logging.debug(f"[{request_id}] Ответ получен за {request_time:.3f} сек")
            logging.debug(f"[{request_id}] HTTP статус: {response.status_code}")
            logging.debug(f"[{request_id}] Размер ответа: {len(response.content)} байт")
            
            if response.status_code != 200:
                logging.error(f"[{request_id}] Ошибка HTTP: {response.status_code}")
                if len(response.text) > 0:
                    logging.error(f"[{request_id}] Текст ошибки: {response.text[:500]}")
                return None
            
            # Пытаемся распарсить JSON
            try:
                data = response.json()
                logging.debug(f"[{request_id}] JSON успешно распарсен")
                
                return data
                
            except json.JSONDecodeError as e:
                logging.error(f"[{request_id}] Ошибка парсинга JSON: {str(e)}")
                logging.error(f"[{request_id}] Ответ (первые 500 символов): {response.text[:500]}")
                return None
                
        except requests.exceptions.Timeout:
            logging.error(f"[{request_id}] Таймаут запроса (30 секунд)")
            return None
        except requests.exceptions.ConnectionError as e:
            logging.error(f"[{request_id}] Ошибка подключения: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"[{request_id}] Неожиданная ошибка: {str(e)}")
            return None
        finally:
            total_time = time.time() - start_time
            logging.debug(f"[{request_id}] Общее время запроса: {total_time:.3f} сек")
    
    def get_all_boards(self) -> List[str]:
        """
        Получение всех доступных торговых площадок для облигаций
        
        Returns:
            Список кодов торговых площадок
        """
        logging.info("Получение списка торговых площадок...")
        
        url = f"{self.base_url}/engines/stock/markets/bonds/boards.json"
        params = {
            'iss.meta': 'off',
            'iss.json': 'extended',
            'limit': 100
        }
        
        data = self._make_request(url, params, "Получение торговых площадок")
        
        if not data:
            logging.warning("Не удалось получить данные о площадках, используются значения по умолчанию")
            return ['TQOB', 'TQCB', 'TQDB', 'TQIR']
        
        try:
            # Правильная обработка структуры ответа MOEX API
            if isinstance(data, list) and len(data) > 1:
                # Второй элемент - это словарь с данными
                boards_dict = data[1]
                
                if 'boards' in boards_dict and 'data' in boards_dict['boards']:
                    boards_data = boards_dict['boards']['data']
                    
                    # Определяем индексы колонок
                    columns = boards_dict['boards']['columns']
                    boardid_idx = columns.index('boardid') if 'boardid' in columns else 0
                    is_primary_idx = columns.index('is_primary') if 'is_primary' in columns else 2
                    
                    # Фильтруем только активные площадки для облигаций
                    active_boards = []
                    for board_item in boards_data:
                        try:
                            board_id = board_item[boardid_idx]
                            is_primary = board_item[is_primary_idx]
                            
                            # Фильтруем по признаку is_primary и по префиксу TQ (торговая система)
                            if (is_primary == 1 or is_primary == '1') and isinstance(board_id, str) and board_id.startswith('TQ'):
                                active_boards.append(board_id)
                        except (IndexError, TypeError) as e:
                            continue
                    
                    # Если не нашли площадок, используем стандартные
                    if not active_boards:
                        logging.warning("Не найдено активных площадок, используются стандартные")
                        active_boards = ['TQOB', 'TQCB', 'TQDB', 'TQIR']
                    
                    logging.info(f"Найдено {len(active_boards)} активных торговых площадок")
                    logging.debug(f"Площадки: {active_boards}")
                    
                    return active_boards
                
            logging.warning("Неожиданная структура ответа, используются стандартные площадки")
            return ['TQOB', 'TQCB', 'TQDB', 'TQIR']
                
        except Exception as e:
            logging.error(f"Ошибка при обработке данных площадок: {str(e)}")
            return ['TQOB', 'TQCB', 'TQDB', 'TQIR']
    
    def get_bonds_from_board(self, board: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получение облигаций с конкретной торговой площадки
        
        Args:
            board: Код торговой площадки
            limit: Количество записей за один запрос
        
        Returns:
            Список облигаций
        """
        bonds = []
        start = 0
        total_fetched = 0
        
        logging.info(f"Загрузка облигаций с площадки {board}...")
        
        # Список полей для запроса
        securities_columns = [
            'SECID', 'SHORTNAME', 'SECNAME', 'MATDATE', 
            'PREVLEGALCLOSEPRICE', 'ACCRUEDINT', 'COUPONPERIOD',
            'COUPONPERCENT', 'ISIN', 'REGNUMBER', 'LOTVALUE',
            'MINSTEP', 'PREVWAPRICE', 'CURRENCYID', 'FACEVALUE',
            'ISSUESIZE', 'COUPONVALUE', 'NEXTCOUPON', 'LATNAME'
        ]
        
        while True:
            try:
                url = f"{self.base_url}/engines/stock/markets/bonds/boards/{board}/securities.json"
                params = {
                    'iss.meta': 'off',
                    'iss.json': 'extended',
                    'limit': limit,
                    'start': start,
                    'securities.columns': ','.join(securities_columns)
                }
                
                data = self._make_request(url, params, f"Получение облигаций с {board}")
                
                if not data:
                    logging.warning(f"Не удалось получить данные с площадки {board}")
                    break
                
                # Правильная обработка структуры ответа MOEX API
                if isinstance(data, list) and len(data) > 1:
                    # Ищем блок с данными облигаций
                    securities_block = None
                    for item in data:
                        if isinstance(item, dict) and 'securities' in item:
                            securities_block = item['securities']
                            break
                    
                    if not securities_block or 'data' not in securities_block:
                        logging.debug(f"Нет данных облигаций на площадке {board}")
                        break
                    
                    securities_data = securities_block['data']
                    securities_columns_list = securities_block.get('columns', [])
                    
                    if not securities_data:
                        logging.debug(f"Пустые данные облигаций на площадке {board}")
                        break
                    
                    # Обрабатываем каждую облигацию
                    for bond_data in securities_data:
                        try:
                            # Создаем словарь из данных облигации
                            bond = {}
                            for idx, col in enumerate(securities_columns_list):
                                if idx < len(bond_data):
                                    bond[col] = bond_data[idx]
                            
                            # Добавляем информацию о площадке
                            bond['BOARDID'] = board
                            
                            # Преобразуем даты
                            for date_field in ['MATDATE', 'NEXTCOUPON']:
                                if date_field in bond and bond[date_field]:
                                    try:
                                        bond[date_field] = pd.to_datetime(bond[date_field]).strftime('%Y-%m-%d')
                                    except:
                                        # Оставляем оригинальное значение
                                        pass
                            
                            bonds.append(bond)
                            total_fetched += 1
                            
                        except Exception as e:
                            logging.debug(f"Ошибка обработки облигации: {str(e)}")
                            continue
                else:
                    break
                
                # Проверяем, нужно ли продолжать
                if len(securities_data) < limit:
                    break
                
                start += limit
                time.sleep(0.2)
                
            except Exception as e:
                logging.error(f"Ошибка при обработке площадки {board}: {str(e)}")
                break
        
        logging.info(f"Загружено {total_fetched} облигаций с площадки {board}")
        return bonds
    
    def get_marketdata_for_bonds(self, board: str, secids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Получение рыночных данных для облигаций
        
        Args:
            board: Код торговой площадки
            secids: Список идентификаторов ценных бумаг
        
        Returns:
            Словарь с рыночными данными
        """
        marketdata = {}
        
        if not secids:
            logging.debug(f"Пустой список secids для площадки {board}")
            return marketdata
        
        # Разбиваем на чанки по 20 облигаций
        chunk_size = 20
        total_chunks = (len(secids) + chunk_size - 1) // chunk_size
        
        logging.info(f"Загрузка рыночных данных для {len(secids)} облигаций с площадки {board}...")
        
        for i in range(0, len(secids), chunk_size):
            chunk = secids[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            
            try:
                url = f"{self.base_url}/engines/stock/markets/bonds/boards/{board}/securities.json"
                params = {
                    'iss.meta': 'off',
                    'securities': ','.join(chunk),
                    'marketdata.columns': 'SECID,LAST,OPEN,LOW,HIGH,LASTCHANGE,LASTTOPREVPRICE,CHANGE,UPDATETIME,DURATION,YIELD,DECIMALS'
                }
                
                data = self._make_request(url, params, f"Рыночные данные чанк {chunk_num}")
                
                if not data:
                    continue
                
                # Ищем блок marketdata в ответе
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and 'marketdata' in item:
                            md_block = item['marketdata']
                            if 'data' in md_block and 'columns' in md_block:
                                md_columns = md_block['columns']
                                md_data = md_block['data']
                                
                                for row in md_data:
                                    try:
                                        row_dict = {}
                                        for idx, col in enumerate(md_columns):
                                            if idx < len(row):
                                                row_dict[col] = row[idx]
                                        
                                        secid = row_dict.get('SECID')
                                        if secid:
                                            marketdata[secid] = row_dict
                                    except Exception as e:
                                        continue
                
                time.sleep(0.3)
                
            except Exception as e:
                logging.debug(f"Ошибка при обработке чанка {chunk_num}: {str(e)}")
                continue
        
        logging.info(f"Получены рыночные данные для {len(marketdata)} облигаций с площадки {board}")
        return marketdata
    
    def parse_all_bonds(self) -> pd.DataFrame:
        """
        Парсинг всех доступных облигаций
        
        Returns:
            DataFrame с данными об облигациях
        """
        start_time = time.time()
        logging.info("Начало парсинга всех облигаций...")
        
        try:
            # Получаем все торговые площадки
            boards = self.get_all_boards()
            
            if not boards:
                logging.error("Не удалось получить торговые площадки")
                return pd.DataFrame()
            
            all_bonds = []
            
            # Собираем облигации со всех площадок
            for idx, board in enumerate(boards, 1):
                board_start = time.time()
                
                bonds = self.get_bonds_from_board(board)
                
                if bonds:
                    # Получаем рыночные данные
                    secids = [bond.get('SECID') for bond in bonds if bond.get('SECID')]
                    marketdata = self.get_marketdata_for_bonds(board, secids)
                    
                    # Добавляем рыночные данные к облигациям
                    for bond in bonds:
                        secid = bond.get('SECID')
                        if secid and secid in marketdata:
                            bond.update(marketdata[secid])
                    
                    all_bonds.extend(bonds)
                    board_time = time.time() - board_start
                    logging.info(f"Площадка {board}: {len(bonds)} облигаций за {board_time:.2f} сек")
                else:
                    logging.warning(f"Площадка {board}: не содержит облигаций")
                
                time.sleep(0.5)
            
            # Создаем DataFrame
            if not all_bonds:
                logging.error("Не удалось получить данные об облигациях")
                return pd.DataFrame()
            
            df = pd.DataFrame(all_bonds)
            
            # Удаляем дубликаты по SECID
            initial_count = len(df)
            df = df.drop_duplicates(subset=['SECID'], keep='first')
            dup_count = initial_count - len(df)
            
            if dup_count > 0:
                logging.info(f"Удалено {dup_count} дубликатов облигаций")
            
            # Преобразуем числовые колонки
            numeric_columns = [
                'PREVLEGALCLOSEPRICE', 'ACCRUEDINT', 'COUPONPERCENT', 
                'LOTVALUE', 'MINSTEP', 'PREVWAPRICE', 'FACEVALUE',
                'ISSUESIZE', 'COUPONVALUE', 'LAST', 'OPEN', 'LOW', 'HIGH',
                'LASTCHANGE', 'LASTTOPREVPRICE', 'CHANGE', 'DURATION', 'YIELD'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except Exception as e:
                        logging.debug(f"Ошибка преобразования колонки {col}: {str(e)}")
            
            # Сортируем если есть название
            sort_columns = ['SHORTNAME', 'SECNAME', 'LATNAME', 'SECID']
            for col in sort_columns:
                if col in df.columns:
                    df = df.sort_values(col)
                    logging.debug(f"DataFrame отсортирован по {col}")
                    break
            
            total_time = time.time() - start_time
            logging.info(f"Парсинг завершен: {len(df)} уникальных облигаций за {total_time:.2f} сек")
            
            return df
            
        except Exception as e:
            logging.error(f"Ошибка при парсинге облигаций: {str(e)}")
            return pd.DataFrame()
    
    def save_to_excel(self, df: pd.DataFrame) -> bool:
        """
        Сохранение данных в Excel файл
        
        Args:
            df: DataFrame с данными об облигациями
        
        Returns:
            True если сохранение успешно, False в противном случае
        """
        if df.empty:
            logging.error("Нет данных для сохранения")
            return False
        
        try:
            start_time = time.time()
            
            # Создаем Excel writer
            with pd.ExcelWriter(self.excel_path, engine='openpyxl') as writer:
                # Основной лист с данными
                df.to_excel(writer, sheet_name='Облигации', index=False)
                
                # Автонастройка ширины колонок
                worksheet = writer.sheets['Облигации']
                for column_cells in worksheet.columns:
                    if column_cells:
                        # Получаем букву колонки
                        column_letter = column_cells[0].column_letter
                        
                        # Находим максимальную длину значения в колонке
                        max_length = 0
                        for cell in column_cells:
                            try:
                                cell_length = len(str(cell.value))
                                if cell_length > max_length:
                                    max_length = cell_length
                            except:
                                pass
                        
                        # Устанавливаем ширину (максимум 50 символов)
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                
                # Создаем лист со сводной информацией
                summary_data = {
                    'Параметр': [
                        'Всего облигаций',
                        'Дата выгрузки',
                        'Время выгрузки',
                        'Версия парсера'
                    ],
                    'Значение': [
                        len(df),
                        datetime.now().strftime('%Y-%m-%d'),
                        datetime.now().strftime('%H:%M:%S'),
                        '4.0 (стабильная)'
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Сводка', index=False)
                
                # Создаем лист с первыми 10 записями для примера
                if len(df) > 10:
                    sample_df = df.head(10)
                    sample_df.to_excel(writer, sheet_name='Примеры', index=False)
            
            save_time = time.time() - start_time
            
            # Проверяем размер файла
            if os.path.exists(self.excel_path):
                file_size = os.path.getsize(self.excel_path) / 1024 / 1024
            else:
                file_size = 0
            
            logging.info(f"Данные сохранены в {self.excel_path}")
            logging.info(f"Размер файла: {file_size:.2f} МБ")
            logging.info(f"Время сохранения: {save_time:.2f} сек")
            
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении в Excel: {str(e)}")
            return False
    
    def run(self) -> None:
        """Основной метод запуска парсера"""
        total_start_time = time.time()
        
        try:
            logging.info("Запуск парсера облигаций MOEX...")
            
            # Парсим облигации
            df = self.parse_all_bonds()
            
            if not df.empty:
                # Сохраняем в Excel
                if self.save_to_excel(df):
                    total_time = time.time() - total_start_time
                    logging.info(f"Скрипт успешно выполнен за {total_time:.2f} сек")
                    logging.info(f"Сохранено {len(df)} облигаций")
                    
                    # Вывод в консоль
                    print("\n" + "=" * 60)
                    print("ПАРСИНГ ЗАВЕРШЕН УСПЕШНО!")
                    print(f"Облигаций сохранено: {len(df)}")
                    print(f"Время выполнения: {total_time:.2f} сек")
                    print(f"Файл данных: {self.excel_path}")
                    print("=" * 60)
                else:
                    logging.error("Не удалось сохранить данные в Excel")
                    print("\nОШИБКА: Не удалось сохранить данные в Excel")
            else:
                logging.error("Не удалось получить данные об облигациях")
                print("\nОШИБКА: Не удалось получить данные об облигациях")
                
        except KeyboardInterrupt:
            logging.warning("Парсер прерван пользователем")
            print("\nПарсер прерван пользователем")
        except Exception as e:
            logging.error(f"Непредвиденная ошибка: {str(e)}")
            print(f"\nКРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        finally:
            self.session.close()
            total_time = time.time() - total_start_time
            logging.info(f"Общее время работы: {total_time:.2f} сек")


def main():
    """Основная функция"""
    print("=" * 60)
    print("MOEX BONDS PARSER v4.0")
    print("Парсер облигаций Московской биржи")
    print("=" * 60)
    
    try:
        parser = MOEXBondsParser(
            excel_path='moex_bonds.xlsx',
            log_path='bonds_parser.log'
        )
        parser.run()
    except Exception as e:
        print(f"\nОШИБКА ПРИ ЗАПУСКЕ: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()