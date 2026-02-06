"""
MOEX Bonds Parser - Парсер облигаций Московской биржи
Версия 10.0 - с сохранением JSON для отладки
"""

import logging
import time
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class MOEXBondsParser:
    """Класс для парсинга облигаций с MOEX"""

    def __init__(self, 
                 excel_path: str = 'moex_bonds.xlsx', 
                 log_path: str = 'bonds_parser.log',
                 save_json_dumps: bool = True):
        """
        Инициализация парсера

        Args:
            excel_path: Путь для сохранения Excel файла
            log_path: Путь для сохранения лог файла
            save_json_dumps: Сохранять ли сырые JSON-ответы от API
        """
        self.excel_path = excel_path
        self.log_path = log_path
        self.save_json_dumps = save_json_dumps
        self.json_dumps_dir = "moex_json_dumps"
        
        # Создаем директорию для дампов JSON при необходимости
        if self.save_json_dumps:
            os.makedirs(self.json_dumps_dir, exist_ok=True)
        
        self.session = self._create_session()
        self.base_url = 'https://iss.moex.com/iss'

        # Настройка логирования
        self.setup_logging()

        # Логирование начала работы
        logging.info("=" * 80)
        logging.info("MOEX BONDS PARSER - ЗАПУСК (Версия 10.0)")
        logging.info(f"Дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Файл Excel: {self.excel_path}")
        logging.info(f"Лог файл: {self.log_path}")
        logging.info(f"Сохранение JSON: {'ВКЛ' if self.save_json_dumps else 'ВЫКЛ'}")
        logging.info(f"Базовый URL MOEX API: {self.base_url}")

    def _create_session(self) -> requests.Session:
        """Создание HTTP сессии с ретраями"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
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
        return session

    def setup_logging(self) -> None:
        """Настройка логирования"""
        try:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

            file_handler = logging.FileHandler(self.log_path, mode='w', encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_format = logging.Formatter(
                '%(asctime)s [%(levelname)-8s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_format)

            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(file_format)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

            logging.info("=" * 80)
            logging.info("ЛОГ ФАЙЛ MOEX BONDS PARSER")
            logging.info(f"Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info(f"Версия парсера: 10.0 (с сохранением JSON)")
            logging.info("=" * 80)

        except Exception as e:
            print(f"Ошибка при настройке логирования: {str(e)}")
            raise

    def _save_json_response(self, data: Dict, filename: str) -> None:
        """Сохраняет JSON-ответ в файл для последующего анализа"""
        if not self.save_json_dumps:
            return
            
        try:
            filepath = os.path.join(self.json_dumps_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logging.debug(f"JSON-ответ сохранен в {filepath}")
        except Exception as e:
            logging.warning(f"Не удалось сохранить JSON: {str(e)}")

    def _make_request(self, url: str, params: Dict = None, operation: str = "", 
                      save_filename: str = None) -> Optional[Dict]:
        """
        Универсальный метод для выполнения HTTP запросов
        """
        try:
            response = self.session.get(url, params=params, timeout=30)

            if response.status_code != 200:
                logging.error(f"Ошибка HTTP {response.status_code}: {operation}")
                logging.debug(f"URL: {response.url}")
                return None

            # Парсим JSON
            try:
                data = response.json()
                
                # Сохраняем сырой JSON если нужно
                if save_filename and self.save_json_dumps:
                    self._save_json_response(data, save_filename)
                
                return data
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка парсинга JSON: {str(e)}")
                # Сохраняем сырой текст ответа для анализа
                if save_filename and self.save_json_dumps:
                    raw_filepath = os.path.join(self.json_dumps_dir, f"{save_filename}.txt")
                    with open(raw_filepath, 'w', encoding='utf-8') as f:
                        f.write(response.text[:5000])
                return None

        except requests.exceptions.Timeout:
            logging.error(f"Таймаут запроса: {operation}")
            return None
        except Exception as e:
            logging.error(f"Ошибка запроса {operation}: {str(e)}")
            return None

    def get_all_boards(self) -> List[str]:
        """
        Получение всех доступных торговых площадок для облигаций
        """
        logging.info("Получение списка торговых площадок...")

        url = f"{self.base_url}/engines/stock/markets/bonds/boards.json"
        params = {
            'iss.meta': 'off',
            'iss.json': 'extended',
            'limit': 100
        }

        data = self._make_request(url, params, "Получение торговых площадок", 
                                 "boards_response.json")

        if not data:
            logging.warning("Не удалось получить данные о площадках, используются стандартные")
            return ['TQOB', 'TQCB', 'TQDB']

        try:
            boards = []
            if isinstance(data, list) and len(data) > 1:
                boards_data = data[1]
                if isinstance(boards_data, dict) and 'boards' in boards_data:
                    if 'data' in boards_data['boards'] and 'columns' in boards_data['boards']:
                        boards_list = boards_data['boards']['data']
                        columns = boards_data['boards']['columns']

                        try:
                            boardid_idx = columns.index('boardid')
                            is_primary_idx = columns.index('is_primary')
                        except ValueError:
                            boardid_idx = 0
                            is_primary_idx = 2

                        for board_item in boards_list:
                            try:
                                board_id = board_item[boardid_idx]
                                is_primary = board_item[is_primary_idx]

                                if board_id and str(is_primary) == '1' and board_id.startswith('TQ'):
                                    boards.append(board_id)
                            except (IndexError, TypeError):
                                continue

            if not boards:
                boards = ['TQOB', 'TQCB', 'TQDB']

            logging.info(f"Найдено {len(boards)} торговых площадок: {boards}")
            return boards

        except Exception as e:
            logging.error(f"Ошибка обработки данных площадок: {str(e)}")
            return ['TQOB', 'TQCB', 'TQDB']

    def _find_securities_data(self, data: Any, path: str = "") -> Optional[tuple]:
        """
        Рекурсивно ищет данные облигаций в структуре JSON
        
        Returns:
            tuple(bonds_data, columns) или None
        """
        try:
            if isinstance(data, dict):
                # Если это похоже на блок с данными облигаций
                if 'data' in data and 'columns' in data:
                    bonds_data = data['data']
                    columns = data['columns']
                    if isinstance(bonds_data, list) and isinstance(columns, list):
                        logging.debug(f"Найдены данные по пути: {path}")
                        return bonds_data, columns
                
                # Рекурсивно ищем во всех значениях словаря
                for key, value in data.items():
                    result = self._find_securities_data(value, f"{path}.{key}")
                    if result:
                        return result
                        
            elif isinstance(data, list):
                # Рекурсивно ищем во всех элементах списка
                for i, item in enumerate(data):
                    result = self._find_securities_data(item, f"{path}[{i}]")
                    if result:
                        return result
                        
        except Exception as e:
            logging.debug(f"Ошибка при поиске данных по пути {path}: {str(e)}")
        
        return None

    def get_bonds_from_board(self, board: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получение облигаций с конкретной торговой площадки
        """
        bonds = []
        start = 0
        page = 1

        logging.info(f"Загрузка облигаций с площадки {board}...")

        while True:
            try:
                url = f"{self.base_url}/engines/stock/markets/bonds/boards/{board}/securities.json"
                params = {
                    'iss.meta': 'off',
                    'iss.json': 'extended',
                    'limit': limit,
                    'start': start,
                    'securities.columns': 'SECID,SHORTNAME,SECNAME,MATDATE,PREVLEGALCLOSEPRICE,ACCRUEDINT,COUPONPERIOD,COUPONPERCENT,ISIN,REGNUMBER,LOTVALUE,MINSTEP,PREVWAPRICE,CURRENCYID,FACEVALUE,ISSUESIZE,COUPONVALUE,NEXTCOUPON'
                }

                filename = f"{board}_page_{page}.json"
                data = self._make_request(url, params, f"Получение облигаций с {board}", filename)

                if not data:
                    logging.warning(f"Нет данных от площадки {board}")
                    break

                # Ищем данные рекурсивно во всей структуре JSON
                result = self._find_securities_data(data)
                
                if result:
                    bonds_data, columns = result
                    
                    if bonds_data:
                        logging.info(f"Найдено {len(bonds_data)} облигаций на странице {page}")
                        
                        for bond_row in bonds_data:
                            try:
                                bond_dict = {}
                                for idx, col_name in enumerate(columns):
                                    if idx < len(bond_row):
                                        bond_dict[col_name] = bond_row[idx]
                                bond_dict['BOARDID'] = board
                                bonds.append(bond_dict)
                            except Exception:
                                continue
                        
                        # Проверяем пагинацию
                        if len(bonds_data) < limit:
                            break
                            
                        start += limit
                        page += 1
                        time.sleep(0.1)
                        continue

                # Если данные не найдены, логируем структуру для отладки
                logging.warning(f"Данные облигаций не найдены в ответе от {board}")
                logging.debug(f"Тип полученных данных: {type(data)}")
                
                if isinstance(data, dict):
                    logging.debug(f"Ключи в корневом словаре: {list(data.keys())}")
                elif isinstance(data, list):
                    logging.debug(f"Длина списка: {len(data)}")
                    if len(data) > 0:
                        logging.debug(f"Тип первого элемента: {type(data[0])}")
                        if isinstance(data[0], dict):
                            logging.debug(f"Ключи первого элемента: {list(data[0].keys())}")
                
                break

            except Exception as e:
                logging.error(f"Ошибка при загрузке данных с площадки {board}: {str(e)}")
                break

        if bonds:
            logging.info(f"Загружено {len(bonds)} облигаций с площадки {board} ({page-1} страниц)")
        else:
            logging.warning(f"Не удалось загрузить облигации с площадки {board}")
            logging.info(f"Проверьте файл {self.json_dumps_dir}/{board}_page_1.json для анализа структуры данных")
        
        return bonds

    def get_marketdata_for_bonds(self, board: str, secids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Получение рыночных данных для облигаций
        """
        marketdata = {}

        if not secids:
            return marketdata

        chunk_size = 30
        total_chunks = (len(secids) + chunk_size - 1) // chunk_size
        
        logging.info(f"Загрузка рыночных данных для {len(secids)} облигаций ({total_chunks} чанков)...")

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

                data = self._make_request(url, params, f"Рыночные данные чанк {chunk_num}",
                                         f"{board}_marketdata_{chunk_num}.json")

                if data and isinstance(data, list) and len(data) > 1:
                    data_block = data[1]
                    if isinstance(data_block, dict) and 'marketdata' in data_block:
                        md_block = data_block['marketdata']
                        if 'data' in md_block and 'columns' in md_block:
                            columns = md_block['columns']
                            md_data = md_block['data']
                            
                            for row in md_data:
                                try:
                                    row_dict = dict(zip(columns, row))
                                    secid = row_dict.get('SECID')
                                    if secid:
                                        marketdata[secid] = row_dict
                                except Exception:
                                    continue

                time.sleep(0.2)

            except Exception as e:
                logging.warning(f"Ошибка при обработке чанка {chunk_num}: {str(e)}")
                continue

        logging.info(f"Получены рыночные данные для {len(marketdata)} облигаций")
        return marketdata

    def parse_all_bonds(self) -> pd.DataFrame:
        """
        Парсинг всех доступных облигаций
        """
        start_time = time.time()
        logging.info("Начало парсинга всех облигаций...")

        try:
            boards = self.get_all_boards()

            if not boards:
                logging.error("Не удалось получить торговые площадки")
                return pd.DataFrame()

            all_bonds = []
            stats = {'processed': 0, 'failed': 0}

            for board in boards:
                board_start = time.time()
                logging.info(f"Обработка площадки: {board}")

                try:
                    bonds = self.get_bonds_from_board(board)

                    if bonds:
                        secids = [bond.get('SECID') for bond in bonds if bond.get('SECID')]
                        if secids:
                            marketdata = self.get_marketdata_for_bonds(board, secids)
                            
                            updated_count = 0
                            for bond in bonds:
                                secid = bond.get('SECID')
                                if secid in marketdata:
                                    bond.update(marketdata[secid])
                                    updated_count += 1
                            
                            logging.info(f"Рыночные данные получены для {updated_count}/{len(bonds)} облигаций")
                        
                        all_bonds.extend(bonds)
                        stats['processed'] += 1
                        board_time = time.time() - board_start
                        logging.info(f"✓ {board}: {len(bonds)} облигаций, время: {board_time:.2f} сек")
                    else:
                        logging.warning(f"✗ {board}: не содержит облигаций")
                        stats['failed'] += 1

                except Exception as e:
                    logging.error(f"✗ Ошибка обработки площадки {board}: {str(e)}")
                    stats['failed'] += 1

                time.sleep(0.5)

            if not all_bonds:
                logging.error("Не удалось получить данные об облигациях")
                if self.save_json_dumps:
                    logging.info(f"JSON-ответы сохранены в папке '{self.json_dumps_dir}' для анализа")
                return pd.DataFrame()

            df = pd.DataFrame(all_bonds)

            if 'SECID' in df.columns:
                initial_count = len(df)
                df = df.drop_duplicates(subset=['SECID'], keep='first')
                dup_count = initial_count - len(df)
                if dup_count > 0:
                    logging.info(f"Удалено {dup_count} дубликатов облигаций")
            else:
                logging.warning("Столбец SECID не найден, дубликаты не удалены")

            numeric_columns = [
                'PREVLEGALCLOSEPRICE', 'ACCRUEDINT', 'COUPONPERCENT', 
                'LOTVALUE', 'MINSTEP', 'PREVWAPRICE', 'FACEVALUE',
                'ISSUESIZE', 'COUPONVALUE', 'LAST', 'OPEN', 'LOW', 'HIGH',
                'LASTCHANGE', 'LASTTOPREVPRICE', 'CHANGE', 'DURATION', 'YIELD'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            if 'SHORTNAME' in df.columns:
                df = df.sort_values('SHORTNAME')
                logging.debug("Данные отсортированы по названию")

            total_time = time.time() - start_time
            logging.info("=" * 60)
            logging.info("ПАРСИНГ ЗАВЕРШЕН")
            logging.info(f"Успешно обработано площадок: {stats['processed']}")
            logging.info(f"Площадок с ошибками: {stats['failed']}")
            logging.info(f"Уникальных облигаций: {len(df)}")
            logging.info(f"Общее время выполнения: {total_time:.2f} сек")
            logging.info("=" * 60)

            return df

        except Exception as e:
            logging.error(f"КРИТИЧЕСКАЯ ОШИБКА при парсинге облигаций: {str(e)}")
            return pd.DataFrame()

    def save_to_excel(self, df: pd.DataFrame) -> bool:
        """
        Сохранение данных в Excel файл
        """
        if df.empty:
            logging.error("Нет данных для сохранения")
            return False

        try:
            start_time = time.time()
            logging.info(f"Сохранение {len(df)} облигаций в Excel...")

            with pd.ExcelWriter(self.excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Облигации', index=False)

                worksheet = writer.sheets['Облигации']
                for column in worksheet.columns:
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
                    worksheet.column_dimensions[column_letter].width = adjusted_width

                summary_data = {
                    'Параметр': ['Всего облигаций', 'Дата выгрузки', 'Версия парсера'],
                    'Значение': [len(df), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '10.0']
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Сводка', index=False)

            save_time = time.time() - start_time
            file_size = os.path.getsize(self.excel_path) / 1024 / 1024
            logging.info(f"✓ Данные сохранены в {self.excel_path}")
            logging.info(f"  Размер файла: {file_size:.2f} МБ")
            logging.info(f"  Время сохранения: {save_time:.2f} сек")
            return True

        except Exception as e:
            logging.error(f"✗ Ошибка при сохранении в Excel: {str(e)}")
            return False

    def run(self) -> None:
        """Основной метод запуска парсера"""
        total_start_time = time.time()

        try:
            logging.info("Запуск парсера облигаций MOEX...")

            df = self.parse_all_bonds()

            if not df.empty:
                if self.save_to_excel(df):
                    total_time = time.time() - total_start_time
                    logging.info(f"Скрипт успешно выполнен за {total_time:.2f} секунд")
                    logging.info(f"Итог: сохранено {len(df)} облигаций")
                    print(f"\n[УСПЕХ] Парсинг завершен. Сохранено {len(df)} облигаций в {self.excel_path}")
                else:
                    logging.error("Не удалось сохранить данные в Excel")
                    print("\n[ОШИБКА] Не удалось сохранить данные в Excel. См. лог.")
            else:
                logging.error("Не удалось получить данные об облигациях")
                if self.save_json_dumps:
                    print(f"\n[ДЕБАГ] JSON-ответы сохранены в папке '{self.json_dumps_dir}'")
                    print("Проанализируйте файлы для определения структуры данных MOEX API")
                print("\n[ОШИБКА] Не удалось получить данные об облигациях. См. лог.")

        except KeyboardInterrupt:
            logging.warning("Выполнение прервано пользователем.")
            print("\n[ИНФО] Выполнение прервано пользователем.")
        except Exception as e:
            logging.error(f"Непредвиденная ошибка в основном цикле: {str(e)}")
            print(f"\n[КРИТИЧЕСКАЯ ОШИБКА] {str(e)}")
        finally:
            self.session.close()
            total_time = time.time() - total_start_time
            logging.info(f"Общее время работы скрипта: {total_time:.2f} секунд")


def main():
    """Основная функция"""
    print("=" * 60)
    print("MOEX BONDS PARSER v10.0")
    print("Парсер облигаций Московской биржи")
    print("=" * 60)

    try:
        # Можно отключить сохранение JSON, передав save_json_dumps=False
        parser = MOEXBondsParser(
            excel_path='moex_bonds.xlsx',
            log_path='bonds_parser.log',
            save_json_dumps=True  # Сохранять JSON-ответы для отладки
        )
        parser.run()
    except Exception as e:
        print(f"\n[ОШИБКА] Не удалось запустить парсер: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()