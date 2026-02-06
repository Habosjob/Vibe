"""
MOEX Bonds Parser - Парсер облигаций Московской биржи
"""

import logging
import time
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
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
        logging.info("=" * 60)
        logging.info("MOEX Bonds Parser запущен")
        logging.info(f"Дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Файл Excel будет сохранен: {self.excel_path}")
        logging.info(f"Лог файл: {self.log_path}")
        
    def _create_session(self) -> requests.Session:
        """Создание HTTP сессии с ретраями"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({
            'User-Agent': 'MOEX-Bonds-Parser/1.0'
        })
        return session
    
    def setup_logging(self) -> None:
        """Настройка логирования в файл"""
        # Очищаем файл при каждом запуске
        open(self.log_path, 'w').close()
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_path, mode='w'),  # Перезапись файла
                logging.StreamHandler()  # Также вывод в консоль
            ]
        )
    
    def get_all_boards(self) -> List[str]:
        """
        Получение всех доступных торговых площадок для облигаций
        
        Returns:
            Список кодов торговых площадок
        """
        logging.info("Получение списка торговых площадок...")
        
        url = f"{self.base_url}/engines/stock/markets/bonds/boards.json"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            boards = data['boards']['data']
            
            # Фильтруем только активные площадки
            active_boards = [
                board[0] for board in boards 
                if board[2] == 1 and board[0].startswith('TQ')  # is_primary=1 и TQ площадки
            ]
            
            logging.info(f"Найдено {len(active_boards)} активных торговых площадок")
            return active_boards
            
        except Exception as e:
            logging.error(f"Ошибка при получении списка площадок: {str(e)}")
            # Возвращаем стандартные площадки при ошибке
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
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if not data[1]['securities']:
                    break
                
                # Извлекаем данные облигаций
                securities_data = data[1]['securities']['data']
                columns = data[1]['securities']['columns']
                
                for bond_data in securities_data:
                    bond = dict(zip(columns, bond_data))
                    
                    # Добавляем информацию о площадке
                    bond['BOARDID'] = board
                    
                    # Преобразуем даты
                    if bond.get('MATDATE'):
                        try:
                            bond['MATDATE'] = pd.to_datetime(bond['MATDATE']).strftime('%Y-%m-%d')
                        except:
                            bond['MATDATE'] = bond['MATDATE']
                    
                    if bond.get('NEXTCOUPON'):
                        try:
                            bond['NEXTCOUPON'] = pd.to_datetime(bond['NEXTCOUPON']).strftime('%Y-%m-%d')
                        except:
                            bond['NEXTCOUPON'] = bond['NEXTCOUPON']
                    
                    bonds.append(bond)
                
                # Проверяем, есть ли еще данные
                if len(securities_data) < limit:
                    break
                    
                start += limit
                time.sleep(0.1)  # Небольшая задержка между запросами
                
            except requests.exceptions.RequestException as e:
                logging.error(f"Ошибка сети при загрузке данных с площадки {board}: {str(e)}")
                break
            except Exception as e:
                logging.error(f"Ошибка при обработке данных с площадки {board}: {str(e)}")
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
        chunk_size = 50  # MOEX имеет ограничения на длину URL
        
        logging.info(f"Загрузка рыночных данных для {len(secids)} облигаций с площадки {board}...")
        
        for i in range(0, len(secids), chunk_size):
            chunk = secids[i:i + chunk_size]
            secids_param = ','.join(chunk)
            
            try:
                url = f"{self.base_url}/engines/stock/markets/bonds/boards/{board}/securities.json"
                params = {
                    'iss.meta': 'off',
                    'securities': secids_param,
                    'marketdata.columns': 'SECID,LAST,OPEN,LOW,HIGH,LASTCHANGE,LASTTOPREVPRICE,CHANGE,UPDATETIME,DURATION,YIELD,DECIMALS'
                }
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if 'marketdata' in data and 'data' in data['marketdata']:
                    for md_data in data['marketdata']['data']:
                        if len(md_data) >= 12:
                            secid = md_data[0]
                            marketdata[secid] = {
                                'LAST': md_data[1],
                                'OPEN': md_data[2],
                                'LOW': md_data[3],
                                'HIGH': md_data[4],
                                'LASTCHANGE': md_data[5],
                                'LASTTOPREVPRICE': md_data[6],
                                'CHANGE': md_data[7],
                                'UPDATETIME': md_data[8],
                                'DURATION': md_data[9],
                                'YIELD': md_data[10],
                                'DECIMALS': md_data[11]
                            }
                
                time.sleep(0.1)  # Задержка между запросами
                
            except Exception as e:
                logging.warning(f"Ошибка при загрузке рыночных данных для чанка: {str(e)}")
                continue
        
        return marketdata
    
    def parse_all_bonds(self) -> pd.DataFrame:
        """
        Парсинг всех доступных облигаций
        
        Returns:
            DataFrame с данными об облигациях
        """
        start_time = time.time()
        
        try:
            # Получаем все торговые площадки
            boards = self.get_all_boards()
            
            if not boards:
                logging.error("Не удалось получить торговые площадки")
                return pd.DataFrame()
            
            all_bonds = []
            
            # Собираем облигации со всех площадок
            for board in boards:
                board_start = time.time()
                
                bonds = self.get_bonds_from_board(board)
                
                if bonds:
                    # Получаем рыночные данные для облигаций этой площадки
                    secids = [bond['SECID'] for bond in bonds]
                    marketdata = self.get_marketdata_for_bonds(board, secids)
                    
                    # Добавляем рыночные данные к облигациям
                    for bond in bonds:
                        secid = bond['SECID']
                        if secid in marketdata:
                            bond.update(marketdata[secid])
                    
                    all_bonds.extend(bonds)
                    board_time = time.time() - board_start
                    logging.info(f"Обработка площадки {board} завершена за {board_time:.2f} сек")
                
                time.sleep(0.5)  # Задержка между обработкой разных площадок
            
            # Создаем DataFrame
            if not all_bonds:
                logging.warning("Не удалось получить данные об облигациях")
                return pd.DataFrame()
            
            df = pd.DataFrame(all_bonds)
            
            # Удаляем дубликаты по SECID (одинаковые облигации могут торговаться на разных площадках)
            df = df.drop_duplicates(subset=['SECID'], keep='first')
            
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
            
            # Сортируем по названию
            if 'SHORTNAME' in df.columns:
                df = df.sort_values('SHORTNAME')
            
            total_time = time.time() - start_time
            logging.info(f"Всего обработано {len(df)} уникальных облигаций")
            logging.info(f"Общее время парсинга: {total_time:.2f} секунд")
            
            return df
            
        except Exception as e:
            logging.error(f"Критическая ошибка при парсинге облигаций: {str(e)}", exc_info=True)
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
            
            # Создаем Excel writer
            with pd.ExcelWriter(self.excel_path, engine='openpyxl') as writer:
                # Основной лист с данными
                df.to_excel(writer, sheet_name='Облигации', index=False)
                
                # Создаем лист со сводной информацией
                summary_data = {
                    'Параметр': ['Всего облигаций', 'Дата выгрузки', 'Время выполнения'],
                    'Значение': [len(df), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f"{time.time() - start_time:.2f} сек"]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Сводка', index=False)
                
                # Автонастройка ширины колонок
                worksheet = writer.sheets['Облигации']
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
            
            save_time = time.time() - start_time
            logging.info(f"Данные успешно сохранены в {self.excel_path}")
            logging.info(f"Время сохранения: {save_time:.2f} секунд")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении в Excel: {str(e)}")
            return False
    
    def run(self) -> None:
        """Основной метод запуска парсера"""
        total_start_time = time.time()
        
        try:
            logging.info("Начало парсинга облигаций MOEX...")
            
            # Парсим облигации
            df = self.parse_all_bonds()
            
            if not df.empty:
                # Сохраняем в Excel
                if self.save_to_excel(df):
                    total_time = time.time() - total_start_time
                    logging.info(f"Скрипт успешно выполнен за {total_time:.2f} секунд")
                    logging.info(f"Сохранено {len(df)} облигаций")
                else:
                    logging.error("Не удалось сохранить данные в Excel")
            else:
                logging.error("Не удалось получить данные об облигациях")
                
        except KeyboardInterrupt:
            logging.warning("Скрипт прерван пользователем")
        except Exception as e:
            logging.error(f"Непредвиденная ошибка: {str(e)}", exc_info=True)
        finally:
            logging.info("=" * 60)
            logging.info("MOEX Bonds Parser завершил работу")
            self.session.close()


def main():
    """Основная функция"""
    parser = MOEXBondsParser(
        excel_path='moex_bonds.xlsx',
        log_path='bonds_parser.log'
    )
    parser.run()


if __name__ == '__main__':
    main()