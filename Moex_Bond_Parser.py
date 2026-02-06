"""
MOEX Bonds Parser - Парсер облигаций Московской биржи
Улучшенная версия с детальным логированием
"""

import logging
import time
import json
import traceback
from datetime import datetime
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
        logging.info("MOEX BONDS PARSER - ЗАПУСК")
        logging.info(f"Дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Версия скрипта: 2.0 (с расширенным логированием)")
        logging.info(f"Файл Excel: {self.excel_path}")
        logging.info(f"Лог файл: {self.log_path}")
        logging.info(f"Базовый URL MOEX API: {self.base_url}")
        logging.debug("Инициализация парсера завершена")
        
    def _create_session(self) -> requests.Session:
        """Создание HTTP сессии с ретраями"""
        logging.debug("Создание HTTP сессии с настройками ретраев")
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=[500, 502, 503, 504, 429],
            allowed_methods=['GET', 'POST']
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({
            'User-Agent': 'MOEX-Bonds-Parser/2.0 (Python)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        logging.debug("HTTP сессия создана с параметрами: total_retries=5, backoff_factor=1.0")
        return session
    
    def setup_logging(self) -> None:
        """Настройка расширенного логирования в файл"""
        try:
            # Очищаем файл при каждом запуске
            with open(self.log_path, 'w', encoding='utf-8') as f:
                f.write(f"=== ЛОГ ФАЙЛ MOEX BONDS PARSER ===\n")
                f.write(f"Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
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
            
            # Обработчик для консоли (INFO уровень)
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
                logging.error(f"[{request_id}] Текст ответа: {response.text[:500]}")
                return None
            
            # Пытаемся распарсить JSON
            try:
                data = response.json()
                logging.debug(f"[{request_id}] JSON успешно распарсен")
                
                # Логируем структуру ответа
                if isinstance(data, dict):
                    logging.debug(f"[{request_id}] Ключи в ответе: {list(data.keys())}")
                elif isinstance(data, list):
                    logging.debug(f"[{request_id}] Ответ является списком, длина: {len(data)}")
                
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
            logging.error(f"[{request_id}] Трассировка: {traceback.format_exc()}")
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
            'iss.json': 'extended'
        }
        
        data = self._make_request(url, params, "Получение торговых площадок")
        
        if not data:
            logging.warning("Не удалось получить данные о площадках, используются значения по умолчанию")
            return ['TQOB', 'TQCB', 'TQDB', 'TQBR']
        
        try:
            # Исправленная логика парсинга JSON структуры MOEX
            # Структура ответа MOEX: [metadata, data, ...]
            if isinstance(data, list) and len(data) > 1:
                boards_data = data[1]  # Второй элемент содержит данные
                logging.debug(f"Структура данных площадок: {list(boards_data.keys())}")
                
                if 'boards' in boards_data:
                    boards = boards_data['boards']['data']
                    columns = boards_data['boards']['columns']
                    
                    logging.debug(f"Найдено {len(boards)} записей о площадках")
                    logging.debug(f"Колонки площадок: {columns}")
                    
                    # Фильтруем только активные площадки
                    active_boards = []
                    for board in boards:
                        try:
                            board_dict = dict(zip(columns, board))
                            board_id = board_dict.get('boardid', '')
                            is_primary = board_dict.get('is_primary', 0)
                            
                            if is_primary == 1 and board_id.startswith('TQ'):
                                active_boards.append(board_id)
                                logging.debug(f"Добавлена активная площадка: {board_id}")
                        except Exception as e:
                            logging.warning(f"Ошибка обработки площадки {board}: {str(e)}")
                            continue
                    
                    logging.info(f"Найдено {len(active_boards)} активных торговых площадок")
                    if active_boards:
                        logging.debug(f"Активные площадки: {active_boards}")
                    
                    return active_boards
                else:
                    logging.error("Ключ 'boards' не найден в ответе")
                    return ['TQOB', 'TQCB', 'TQDB', 'TQBR']
            else:
                logging.error(f"Неожиданная структура ответа. Тип данных: {type(data)}, Длина: {len(data) if isinstance(data, list) else 'N/A'}")
                return ['TQOB', 'TQCB', 'TQDB', 'TQBR']
                
        except Exception as e:
            logging.error(f"Ошибка при обработке данных площадок: {str(e)}")
            logging.error(f"Трассировка: {traceback.format_exc()}")
            return ['TQOB', 'TQCB', 'TQDB', 'TQBR']
    
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
        
        while True:
            try:
                logging.debug(f"Запрос облигаций с {board}: start={start}, limit={limit}")
                
                url = f"{self.base_url}/engines/stock/markets/bonds/boards/{board}/securities.json"
                params = {
                    'iss.meta': 'off',
                    'iss.json': 'extended',
                    'limit': limit,
                    'start': start,
                    'securities.columns': ','.join([
                        'SECID', 'SHORTNAME', 'SECNAME', 'MATDATE', 
                        'PREVLEGALCLOSEPRICE', 'ACCRUEDINT', 'COUPONPERIOD',
                        'COUPONPERCENT', 'ISIN', 'REGNUMBER', 'LOTVALUE',
                        'MINSTEP', 'PREVWAPRICE', 'CURRENCYID', 'FACEVALUE',
                        'ISSUESIZE', 'COUPONVALUE', 'NEXTCOUPON'
                    ])
                }
                
                data = self._make_request(url, params, f"Получение облигаций с {board}")
                
                if not data:
                    logging.warning(f"Не удалось получить данные с площадки {board}")
                    break
                
                # Обработка структуры ответа MOEX
                if isinstance(data, list) and len(data) > 1:
                    # Ищем блок с securities
                    securities_found = False
                    for item in data:
                        if isinstance(item, dict) and 'securities' in item:
                            securities_data = item['securities']
                            if 'data' in securities_data and 'columns' in securities_data:
                                bonds_batch = securities_data['data']
                                columns = securities_data['columns']
                                
                                logging.debug(f"Получено {len(bonds_batch)} облигаций из блока securities")
                                
                                # Обрабатываем каждую облигацию
                                for bond_data in bonds_batch:
                                    try:
                                        bond = dict(zip(columns, bond_data))
                                        bond['BOARDID'] = board
                                        
                                        # Преобразуем даты
                                        date_fields = ['MATDATE', 'NEXTCOUPON']
                                        for field in date_fields:
                                            if field in bond and bond[field]:
                                                try:
                                                    bond[field] = pd.to_datetime(bond[field]).strftime('%Y-%m-%d')
                                                except:
                                                    pass
                                        
                                        bonds.append(bond)
                                        total_fetched += 1
                                        
                                    except Exception as e:
                                        logging.warning(f"Ошибка обработки облигации {bond_data}: {str(e)}")
                                        continue
                                
                                securities_found = True
                                break
                    
                    if not securities_found:
                        logging.warning(f"Блок 'securities' не найден в ответе от площадки {board}")
                        # Попробуем найти данные в другом формате
                        for i, item in enumerate(data):
                            logging.debug(f"Элемент {i}: тип={type(item)}, содержимое={str(item)[:200]}")
                        break
                
                else:
                    logging.warning(f"Неожиданный формат ответа от площадки {board}")
                    break
                
                # Проверяем, нужно ли продолжать
                if len(bonds_batch) < limit:
                    logging.debug(f"Получены все облигации с площадки {board} (меньше лимита)")
                    break
                
                start += limit
                time.sleep(0.2)  # Задержка между запросами
                
            except Exception as e:
                logging.error(f"Ошибка при обработке площадки {board}: {str(e)}")
                logging.error(f"Трассировка: {traceback.format_exc()}")
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
            logging.warning(f"Пустой список secids для площадки {board}")
            return marketdata
        
        chunk_size = 30  # Уменьшенный размер чанка для надежности
        total_chunks = (len(secids) + chunk_size - 1) // chunk_size
        
        logging.info(f"Загрузка рыночных данных для {len(secids)} облигаций с площадки {board} (чанков: {total_chunks})...")
        
        for i in range(0, len(secids), chunk_size):
            chunk = secids[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            
            logging.debug(f"Обработка чанка {chunk_num}/{total_chunks}, размер: {len(chunk)} облигаций")
            logging.debug(f"Пример secids в чанке: {chunk[:3]}")
            
            secids_param = ','.join(chunk)
            
            try:
                url = f"{self.base_url}/engines/stock/markets/bonds/boards/{board}/securities.json"
                params = {
                    'iss.meta': 'off',
                    'securities': secids_param,
                    'marketdata.columns': ','.join([
                        'SECID', 'LAST', 'OPEN', 'LOW', 'HIGH', 
                        'LASTCHANGE', 'LASTTOPREVPRICE', 'CHANGE',
                        'UPDATETIME', 'DURATION', 'YIELD', 'DECIMALS'
                    ])
                }
                
                data = self._make_request(url, params, f"Рыночные данные чанк {chunk_num}")
                
                if not data:
                    logging.warning(f"Не удалось получить рыночные данные для чанка {chunk_num}")
                    continue
                
                # Ищем блок marketdata в ответе
                marketdata_found = False
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and 'marketdata' in item:
                            md_data = item['marketdata']
                            if 'data' in md_data and 'columns' in md_data:
                                columns = md_data['columns']
                                
                                logging.debug(f"Получено {len(md_data['data'])} записей рыночных данных")
                                
                                for row in md_data['data']:
                                    try:
                                        row_dict = dict(zip(columns, row))
                                        secid = row_dict.get('SECID')
                                        
                                        if secid:
                                            marketdata[secid] = row_dict
                                            logging.debug(f"Добавлены рыночные данные для {secid}")
                                    except Exception as e:
                                        logging.warning(f"Ошибка обработки рыночных данных {row}: {str(e)}")
                                        continue
                                
                                marketdata_found = True
                                break
                
                if not marketdata_found:
                    logging.warning(f"Блок 'marketdata' не найден в ответе для чанка {chunk_num}")
                    # Логируем структуру ответа для отладки
                    logging.debug(f"Структура ответа чанка {chunk_num}: {str(data)[:500]}")
                
                time.sleep(0.3)  # Задержка между запросами
                
            except Exception as e:
                logging.error(f"Ошибка при обработке чанка {chunk_num}: {str(e)}")
                continue
        
        logging.info(f"Получены рыночные данные для {len(marketdata)} облигаций с площадки {board}")
        return marketdata
    
    def parse_all_bonds(self) -> pd.DataFrame:
        """
        Парсинг всех доступных облигаций с детальным логированием
        
        Returns:
            DataFrame с данными об облигациях
        """
        start_time = time.time()
        logging.info("=" * 80)
        logging.info("НАЧАЛО ПАРСИНГА ВСЕХ ОБЛИГАЦИЙ")
        logging.info(f"Время начала: {datetime.now().strftime('%H:%M:%S')}")
        
        try:
            # Получаем все торговые площадки
            boards_start = time.time()
            boards = self.get_all_boards()
            boards_time = time.time() - boards_start
            
            logging.info(f"Получено {len(boards)} площадок за {boards_time:.2f} сек")
            logging.debug(f"Список площадок: {boards}")
            
            if not boards:
                logging.error("Не удалось получить торговые площадки")
                return pd.DataFrame()
            
            all_bonds = []
            stats = {
                'total_bonds': 0,
                'boards_processed': 0,
                'boards_failed': 0
            }
            
            # Собираем облигации со всех площадок
            for idx, board in enumerate(boards, 1):
                board_start = time.time()
                logging.info(f"[{idx}/{len(boards)}] Обработка площадки: {board}")
                
                try:
                    bonds = self.get_bonds_from_board(board)
                    
                    if bonds:
                        # Получаем рыночные данные
                        secids = [bond['SECID'] for bond in bonds if 'SECID' in bond]
                        logging.debug(f"Запрос рыночных данных для {len(secids)} облигаций")
                        
                        marketdata = self.get_marketdata_for_bonds(board, secids)
                        
                        # Добавляем рыночные данные к облигациям
                        updated_count = 0
                        for bond in bonds:
                            secid = bond.get('SECID')
                            if secid and secid in marketdata:
                                bond.update(marketdata[secid])
                                updated_count += 1
                        
                        all_bonds.extend(bonds)
                        stats['boards_processed'] += 1
                        stats['total_bonds'] += len(bonds)
                        
                        board_time = time.time() - board_start
                        logging.info(f"✓ Площадка {board}: {len(bonds)} облигаций, "
                                   f"рыночные данные: {updated_count}/{len(bonds)}, "
                                   f"время: {board_time:.2f} сек")
                    else:
                        logging.warning(f"✗ Площадка {board}: не содержит облигаций")
                        stats['boards_failed'] += 1
                    
                except Exception as e:
                    logging.error(f"✗ Ошибка обработки площадки {board}: {str(e)}")
                    logging.error(f"Трассировка: {traceback.format_exc()}")
                    stats['boards_failed'] += 1
                
                # Задержка между площадками
                time.sleep(0.5)
            
            # Создаем DataFrame
            if not all_bonds:
                logging.error("Не удалось получить данные об облигациях")
                return pd.DataFrame()
            
            df_start = time.time()
            df = pd.DataFrame(all_bonds)
            df_time = time.time() - df_start
            
            logging.debug(f"DataFrame создан: {len(df)} строк, {len(df.columns)} колонок")
            logging.debug(f"Колонки DataFrame: {list(df.columns)}")
            
            # Удаляем дубликаты по SECID
            dup_start = time.time()
            initial_count = len(df)
            df = df.drop_duplicates(subset=['SECID'], keep='first')
            dup_count = initial_count - len(df)
            dup_time = time.time() - dup_start
            
            if dup_count > 0:
                logging.info(f"Удалено {dup_count} дубликатов облигаций за {dup_time:.3f} сек")
            
            # Преобразуем числовые колонки
            numeric_columns = [
                'PREVLEGALCLOSEPRICE', 'ACCRUEDINT', 'COUPONPERCENT', 
                'LOTVALUE', 'MINSTEP', 'PREVWAPRICE', 'FACEVALUE',
                'ISSUESIZE', 'COUPONVALUE', 'LAST', 'OPEN', 'LOW', 'HIGH',
                'LASTCHANGE', 'LASTTOPREVPRICE', 'CHANGE', 'DURATION', 'YIELD'
            ]
            
            conv_start = time.time()
            for col in numeric_columns:
                if col in df.columns:
                    try:
                        initial_nulls = df[col].isna().sum()
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        new_nulls = df[col].isna().sum()
                        if new_nulls > initial_nulls:
                            logging.debug(f"Колонка {col}: преобразовано, новых NaN: {new_nulls - initial_nulls}")
                    except Exception as e:
                        logging.warning(f"Ошибка преобразования колонки {col}: {str(e)}")
            conv_time = time.time() - conv_start
            
            # Сортируем по названию
            if 'SHORTNAME' in df.columns:
                df = df.sort_values('SHORTNAME')
                logging.debug("DataFrame отсортирован по SHORTNAME")
            
            total_time = time.time() - start_time
            logging.info("=" * 80)
            logging.info("ПАРСИНГ ЗАВЕРШЕН")
            logging.info(f"Статистика:")
            logging.info(f"  Всего площадок: {len(boards)}")
            logging.info(f"  Успешно обработано: {stats['boards_processed']}")
            logging.info(f"  Ошибок обработки: {stats['boards_failed']}")
            logging.info(f"  Уникальных облигаций: {len(df)}")
            logging.info(f"  Общее время выполнения: {total_time:.2f} сек")
            logging.info(f"  Среднее время на облигацию: {total_time/len(df):.3f} сек" if len(df) > 0 else "")
            
            return df
            
        except Exception as e:
            logging.error(f"КРИТИЧЕСКАЯ ОШИБКА при парсинге облигаций: {str(e)}")
            logging.error(f"Трассировка: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def save_to_excel(self, df: pd.DataFrame) -> bool:
        """
        Сохранение данных в Excel файл с детальным логированием
        
        Args:
            df: DataFrame с данными об облигациях
        
        Returns:
            True если сохранение успешно, False в противном случае
        """
        if df.empty:
            logging.error("DataFrame пуст, нет данных для сохранения")
            return False
        
        save_start = time.time()
        logging.info("=" * 80)
        logging.info("СОХРАНЕНИЕ ДАННЫХ В EXCEL")
        logging.info(f"Размер данных: {len(df)} строк × {len(df.columns)} колонок")
        logging.debug(f"Колонки для сохранения: {list(df.columns)}")
        
        try:
            # Создаем Excel writer с настройками
            excel_settings = {
                'engine': 'openpyxl',
                'options': {'strings_to_formulas': False, 'strings_to_urls': False}
            }
            
            with pd.ExcelWriter(self.excel_path, **excel_settings) as writer:
                # Основной лист с данными
                sheet_start = time.time()
                df.to_excel(writer, sheet_name='Облигации', index=False)
                sheet_time = time.time() - sheet_start
                
                logging.debug(f"Основной лист создан за {sheet_time:.3f} сек")
                
                # Получаем объект листа для форматирования
                worksheet = writer.sheets['Облигации']
                
                # Автонастройка ширины колонок
                format_start = time.time()
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            cell_value = str(cell.value) if cell.value is not None else ""
                            if len(cell_value) > max_length:
                                max_length = len(cell_value)
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
                format_time = time.time() - format_start
                
                logging.debug(f"Форматирование колонок выполнено за {format_time:.3f} сек")
                
                # Создаем лист со статистикой
                stats_start = time.time()
                summary_data = {
                    'Параметр': [
                        'Всего облигаций',
                        'Дата выгрузки',
                        'Время начала',
                        'Время окончания',
                        'Общее время выполнения',
                        'Файл данных',
                        'Размер данных',
                        'Версия парсера'
                    ],
                    'Значение': [
                        len(df),
                        datetime.now().strftime('%Y-%m-%d'),
                        datetime.fromtimestamp(save_start).strftime('%H:%M:%S'),
                        datetime.now().strftime('%H:%M:%S'),
                        f"{time.time() - save_start:.2f} сек",
                        self.excel_path,
                        f"{len(df)}×{len(df.columns)}",
                        '2.0 (с расширенным логированием)'
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Статистика', index=False)
                stats_time = time.time() - stats_start
                
                logging.debug(f"Лист статистики создан за {stats_time:.3f} сек")
                
                # Создаем лист с примерами данных
                if len(df) > 10:
                    sample_start = time.time()
                    sample_df = df.head(10)
                    sample_df.to_excel(writer, sheet_name='Примеры_данных', index=False)
                    sample_time = time.time() - sample_start
                    logging.debug(f"Лист с примерами создан за {sample_time:.3f} сек")
            
            save_time = time.time() - save_start
            file_size = os.path.getsize(self.excel_path) / 1024 / 1024  # Размер в МБ
            
            logging.info("✓ Данные успешно сохранены")
            logging.info(f"  Файл: {self.excel_path}")
            logging.info(f"  Размер файла: {file_size:.2f} МБ")
            logging.info(f"  Время сохранения: {save_time:.2f} сек")
            logging.info(f"  Кол-во листов: {len(writer.sheets)}")
            
            return True
            
        except ImportError as e:
            logging.error(f"Ошибка импорта библиотеки: {str(e)}")
            logging.error("Убедитесь, что установлены библиотеки: pandas, openpyxl")
            return False
        except Exception as e:
            logging.error(f"Ошибка при сохранении в Excel: {str(e)}")
            logging.error(f"Трассировка: {traceback.format_exc()}")
            return False
    
    def run(self) -> None:
        """Основной метод запуска парсера"""
        total_start_time = time.time()
        
        try:
            logging.info("=" * 80)
            logging.info("ЗАПУСК ПАРСЕРА ОБЛИГАЦИЙ MOEX")
            logging.info(f"Идентификатор сессии: SESS_{int(time.time() * 1000) % 10000:04d}")
            
            # Парсим облигации
            df = self.parse_all_bonds()
            
            if not df.empty:
                # Сохраняем в Excel
                if self.save_to_excel(df):
                    total_time = time.time() - total_start_time
                    
                    logging.info("=" * 80)
                    logging.info("СКРИПТ УСПЕШНО ВЫПОЛНЕН")
                    logging.info(f"Итоговая статистика:")
                    logging.info(f"  Облигаций получено: {len(df)}")
                    logging.info(f"  Общее время работы: {total_time:.2f} сек")
                    logging.info(f"  Файл с данными: {self.excel_path}")
                    logging.info(f"  Лог файл: {self.log_path}")
                    
                    # Выводим краткую сводку в консоль
                    print("\n" + "=" * 60)
                    print("ПАРСИНГ ЗАВЕРШЕН УСПЕШНО!")
                    print(f"Облигаций сохранено: {len(df)}")
                    print(f"Время выполнения: {total_time:.2f} сек")
                    print(f"Файл данных: {self.excel_path}")
                    print("=" * 60)
                else:
                    logging.error("✗ Не удалось сохранить данные в Excel")
                    print("\n✗ ОШИБКА: Не удалось сохранить данные в Excel")
            else:
                logging.error("✗ Не удалось получить данные об облигациях")
                print("\n✗ ОШИБКА: Не удалось получить данные об облигациях")
                
        except KeyboardInterrupt:
            logging.warning("⚠ Парсер прерван пользователем (Ctrl+C)")
            print("\n⚠ Парсер прерван пользователем")
        except Exception as e:
            logging.error(f"✗ Непредвиденная ошибка в основном цикле: {str(e)}")
            logging.error(f"Трассировка: {traceback.format_exc()}")
            print(f"\n✗ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        finally:
            self.session.close()
            logging.debug("HTTP сессия закрыта")
            
            total_time = time.time() - total_start_time
            logging.info(f"Общее время работы скрипта: {total_time:.2f} сек")
            logging.info("=" * 80)
            logging.info("РАБОТА ПАРСЕРА ЗАВЕРШЕНА")
            logging.info("=" * 80)


def main():
    """Основная функция"""
    import os
    import sys
    
    print("=" * 60)
    print("MOEX BONDS PARSER v2.0")
    print("Парсер облигаций Московской биржи")
    print("=" * 60)
    
    try:
        parser = MOEXBondsParser(
            excel_path='moex_bonds.xlsx',
            log_path='bonds_parser.log'
        )
        parser.run()
    except Exception as e:
        print(f"\n✗ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()