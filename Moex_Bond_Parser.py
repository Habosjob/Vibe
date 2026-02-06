"""
MOEX Bonds Parser - Парсер облигаций Московской биржи
Версия 6.0 - Стабильная и доработанная
"""

import logging
import time
import json
import os
import sys
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
        logging.info("MOEX BONDS PARSER - ЗАПУСК (Версия 6.0)")
        logging.info(f"Дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Файл Excel: {self.excel_path}")
        logging.info(f"Лог файл: {self.log_path}")
        logging.info(f"Базовый URL MOEX API: {self.base_url}")

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
        return session

    def setup_logging(self) -> None:
        """Настройка логирования в файл с кодировкой UTF-8"""
        try:
            # Создаем логгер и удаляем старые обработчики
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

            # Обработчик для файла (DEBUG уровень, перезапись)
            file_handler = logging.FileHandler(self.log_path, mode='w', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_format = logging.Formatter(
                '%(asctime)s [%(levelname)-8s] %(message)s',
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

            # Записываем заголовок в лог через логгер
            logging.info("=" * 80)
            logging.info("ЛОГ ФАЙЛ MOEX BONDS PARSER")
            logging.info(f"Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info(f"Версия парсера: 6.0 (стабильная)")
            logging.info("=" * 80)

        except Exception as e:
            print(f"КРИТИЧЕСКАЯ ОШИБКА при настройке логирования: {str(e)}")
            raise

    def _make_request(self, url: str, params: Dict = None, operation: str = "") -> Optional[Dict]:
        """
        Универсальный метод для выполнения HTTP запросов

        Args:
            url: URL для запроса
            params: Параметры запроса
            operation: Описание операции для логирования

        Returns:
            Ответ в виде словаря или None при ошибке
        """
        logging.debug(f"Запрос: {operation} | URL: {url}")

        try:
            response = self.session.get(url, params=params, timeout=30)

            if response.status_code == 404:
                logging.warning(f"Ресурс не найден (404): {url}")
                return None
            elif response.status_code != 200:
                logging.error(f"Ошибка HTTP {response.status_code}: {url}")
                return None

            # Пытаемся распарсить JSON
            try:
                return response.json()
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка парсинга JSON: {str(e)}")
                logging.debug(f"Ответ сервера (первые 500 символов): {response.text[:500]}")
                return None

        except requests.exceptions.Timeout:
            logging.error(f"Таймаут запроса: {url}")
            return None
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Ошибка подключения: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Неожиданная ошибка запроса: {str(e)}")
            return None

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
            logging.warning("Не удалось получить данные о площадках, используются стандартные")
            return ['TQOB', 'TQCB', 'TQDB']

        try:
            boards = []
            # Структура ответа MOEX: список, где второй элемент содержит данные
            if isinstance(data, list) and len(data) > 1:
                boards_data = data[1]
                if 'boards' in boards_data and 'data' in boards_data['boards']:
                    boards_list = boards_data['boards']['data']
                    columns = boards_data['boards']['columns']

                    # Определяем индексы нужных колонок
                    try:
                        boardid_idx = columns.index('boardid')
                        is_primary_idx = columns.index('is_primary')
                    except ValueError:
                        boardid_idx = 0
                        is_primary_idx = 2

                    # Фильтруем площадки
                    for board_item in boards_list:
                        try:
                            board_id = board_item[boardid_idx]
                            is_primary = board_item[is_primary_idx]

                            if board_id and str(is_primary) == '1' and board_id.startswith('TQ'):
                                boards.append(board_id)
                        except (IndexError, TypeError):
                            continue

            # Если не нашли площадок, используем стандартные
            if not boards:
                boards = ['TQOB', 'TQCB', 'TQDB']

            logging.info(f"Найдено {len(boards)} торговых площадок: {boards}")
            return boards

        except Exception as e:
            logging.error(f"Ошибка обработки данных площадок: {str(e)}")
            return ['TQOB', 'TQCB', 'TQDB']

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
        fetch_more = True  # Флаг для продолжения пагинации

        logging.info(f"Загрузка облигаций с площадки {board}...")

        while fetch_more:
            try:
                url = f"{self.base_url}/engines/stock/markets/bonds/boards/{board}/securities.json"
                params = {
                    'iss.meta': 'off',
                    'iss.json': 'extended',
                    'limit': limit,
                    'start': start,
                    'securities.columns': 'SECID,SHORTNAME,SECNAME,MATDATE,PREVLEGALCLOSEPRICE,ACCRUEDINT,COUPONPERIOD,COUPONPERCENT,ISIN,REGNUMBER,LOTVALUE,MINSTEP,PREVWAPRICE,CURRENCYID,FACEVALUE,ISSUESIZE,COUPONVALUE,NEXTCOUPON'
                }

                data = self._make_request(url, params, f"Получение облигаций с {board}")

                if not data:
                    logging.warning(f"Нет данных от площадки {board}, прерывание.")
                    break

                batch_found = False
                # Обработка структуры ответа
                if isinstance(data, list) and len(data) > 1:
                    # Ищем блок с данными облигаций
                    for item in data:
                        if isinstance(item, dict) and 'securities' in item:
                            securities_data = item['securities']
                            if 'data' in securities_data and 'columns' in securities_data:
                                bonds_batch = securities_data['data']
                                columns = securities_data['columns']

                                # Обрабатываем облигации
                                for bond_row in bonds_batch:
                                    try:
                                        bond_dict = dict(zip(columns, bond_row))
                                        bond_dict['BOARDID'] = board
                                        bonds.append(bond_dict)
                                    except Exception as e:
                                        logging.debug(f"Ошибка обработки строки облигации: {e}")
                                        continue

                                batch_found = True
                                # Проверяем, нужно ли продолжать пагинацию
                                if len(bonds_batch) < limit:
                                    fetch_more = False
                                break  # Выходим из цикла for после нахождения данных

                if not batch_found:
                    logging.info(f"На площадке {board} больше нет данных.")
                    fetch_more = False
                else:
                    start += limit  # Увеличиваем offset для следующего запроса
                    time.sleep(0.1)  # Задержка между запросами

            except Exception as e:
                logging.error(f"Ошибка при загрузке данных с площадки {board}: {str(e)}")
                fetch_more = False
                break

        logging.info(f"Загружено {len(bonds)} облигаций с площадки {board}")
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

        # Разбиваем на чанки по 30 облигаций
        chunk_size = 30
        total_chunks = (len(secids) + chunk_size - 1) // chunk_size
        logging.info(f"Загрузка рыночных данных для {len(secids)} облигаций с площадки {board} (чанков: {total_chunks})...")

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

                if data and isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and 'marketdata' in item:
                            md_data = item['marketdata']
                            if 'data' in md_data and 'columns' in md_data:
                                columns = md_data['columns']

                                for row in md_data['data']:
                                    try:
                                        row_dict = dict(zip(columns, row))
                                        secid = row_dict.get('SECID')
                                        if secid:
                                            marketdata[secid] = row_dict
                                    except Exception as e:
                                        logging.debug(f"Ошибка обработки рыночных данных для строки: {e}")
                                        continue
                time.sleep(0.2)

            except Exception as e:
                logging.warning(f"Ошибка при обработке чанка {chunk_num}: {str(e)}")
                continue

        logging.info(f"Получены рыночные данные для {len(marketdata)} из {len(secids)} облигаций с площадки {board}")
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
            stats = {'processed': 0, 'failed': 0}

            # Собираем облигации со всех площадок
            for board in boards:
                board_start = time.time()
                logging.info(f"Обработка площадки: {board}")

                try:
                    bonds = self.get_bonds_from_board(board)

                    if bonds:
                        # Получаем рыночные данные
                        secids = [bond.get('SECID') for bond in bonds if bond.get('SECID')]
                        marketdata = self.get_marketdata_for_bonds(board, secids)

                        # Добавляем рыночные данные к облигациям
                        updated_count = 0
                        for bond in bonds:
                            secid = bond.get('SECID')
                            if secid in marketdata:
                                bond.update(marketdata[secid])
                                updated_count += 1

                        all_bonds.extend(bonds)
                        stats['processed'] += 1
                        board_time = time.time() - board_start
                        logging.info(f"✓ {board}: {len(bonds)} облигаций, данные: {updated_count}/{len(bonds)}, время: {board_time:.2f} сек")
                    else:
                        logging.warning(f"✗ {board}: не содержит облигаций")
                        stats['failed'] += 1

                except Exception as e:
                    logging.error(f"✗ Ошибка обработки площадки {board}: {str(e)}")
                    stats['failed'] += 1

                time.sleep(0.5)  # Задержка между площадками

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
                logging.info(f"Удалено {dup_count} дубликатов облигаций.")

            # Преобразуем числовые колонки
            numeric_columns = [
                'PREVLEGALCLOSEPRICE', 'ACCRUEDINT', 'COUPONPERCENT',
                'LOTVALUE', 'MINSTEP', 'PREVWAPRICE', 'FACEVALUE',
                'ISSUESIZE', 'COUPONVALUE', 'LAST', 'OPEN', 'LOW', 'HIGH',
                'LASTCHANGE', 'LASTTOPREVPRICE', 'CHANGE', 'DURATION', 'YIELD'
            ]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Сортируем по названию, если есть
            if 'SHORTNAME' in df.columns:
                df = df.sort_values('SHORTNAME')

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

        Args:
            df: DataFrame с данными об облигациях

        Returns:
            True если сохранение успешно, False в противном случае
        """
        if df.empty:
            logging.error("Нет данных для сохранения")
            return False

        try:
            start_time = time.time()
            logging.info(f"Сохранение {len(df)} облигаций в Excel...")

            # Создаем Excel writer
            with pd.ExcelWriter(self.excel_path, engine='openpyxl') as writer:
                # Основной лист с данными
                df.to_excel(writer, sheet_name='Облигации', index=False)

                # Автонастройка ширины колонок
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

                # Лист со статистикой
                summary_data = {
                    'Параметр': ['Всего облигаций', 'Дата выгрузки', 'Время выполнения скрипта'],
                    'Значение': [len(df), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'См. лог файл']
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Сводка', index=False)

            save_time = time.time() - start_time
            file_size = os.path.getsize(self.excel_path) / 1024 / 1024  # Размер в МБ
            logging.info(f"✓ Файл сохранен: {self.excel_path}")
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

            # Парсим облигации
            df = self.parse_all_bonds()

            if not df.empty:
                # Сохраняем в Excel
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
    print("MOEX BONDS PARSER v6.0")
    print("Парсер облигаций Московской биржи")
    print("=" * 60)

    try:
        parser = MOEXBondsParser(
            excel_path='moex_bonds.xlsx',
            log_path='bonds_parser.log'
        )
        parser.run()
    except Exception as e:
        print(f"\n[ОШИБКА] Не удалось запустить парсер: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()