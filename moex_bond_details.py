#!/usr/bin/env python3
"""
MOEX Bond Details Collector - Final Production Version
Optimized for data integrity and minimal redundant output.
"""

import os
import json
import time
import logging
import shutil
from datetime import datetime
from urllib.parse import urlparse, urlencode
import requests
import pandas as pd
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from pathlib import Path

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('logs/moex_bond_details.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA CLASSES
# ============================================================================
@dataclass
class EndpointMetrics:
    """Metrics tracking for API endpoint calls."""
    url: str
    description: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    success: bool = False
    status_code: Optional[int] = None
    data_size: int = 0
    rows_extracted: int = 0
    error_message: Optional[str] = None

class MoexApiResponse:
    """Unified wrapper for MOEX API responses with data extraction capabilities."""
    
    def __init__(self, raw_data: Dict, endpoint_name: str):
        self.raw_data = raw_data
        self.endpoint_name = endpoint_name
        self.tables: Dict[str, pd.DataFrame] = {}
        self._extract_all_tables()
    
    def _extract_all_tables(self):
        """Extract all data tables from MOEX API response."""
        if not self.raw_data or not isinstance(self.raw_data, dict):
            logger.warning(f"No valid data found for {self.endpoint_name}")
            return
        
        # MOEX responses have nested structure with 'securities' root key
        root_data = self.raw_data.get('securities', self.raw_data)
        
        if not isinstance(root_data, dict):
            return
        
        # Process all top-level keys that contain data tables
        for block_name, block_content in root_data.items():
            if isinstance(block_content, dict):
                df = self._extract_table_from_block(block_content, block_name)
                if df is not None:
                    table_name = f"{self.endpoint_name}_{block_name}"
                    self.tables[table_name] = df
                    logger.debug(f"Extracted table '{table_name}' with shape {df.shape}")
    
    def _extract_table_from_block(self, block: Dict, block_name: str) -> Optional[pd.DataFrame]:
        """Extract a single table from a data block."""
        # MOEX API has two common structures: metadata/data or columns/data
        data_rows = None
        column_names = []
        
        # Structure type 1: metadata + data
        if 'metadata' in block and 'data' in block:
            metadata = block['metadata']
            data_rows = block['data']
            
            # Extract column names from metadata
            if isinstance(metadata, dict):
                column_names = list(metadata.keys())
            elif isinstance(metadata, list) and metadata:
                # Metadata as list of dicts
                column_names = []
                for item in metadata:
                    if isinstance(item, dict) and 'name' in item:
                        column_names.append(item['name'])
                    else:
                        # Fallback to indexed columns
                        column_names.append(f"col_{len(column_names)}")
        
        # Structure type 2: columns + data (more common)
        elif 'columns' in block and 'data' in block:
            column_names = block['columns']
            data_rows = block['data']
        
        # Create DataFrame if we have data
        if data_rows is not None:
            # Ensure column_names length matches data columns
            if not column_names and data_rows:
                column_names = [f"col_{i}" for i in range(len(data_rows[0]))]
            
            try:
                df = pd.DataFrame(data_rows, columns=column_names[:len(data_rows[0])] if data_rows else [])
                return df
            except Exception as e:
                logger.error(f"Failed to create DataFrame for {block_name}: {e}")
                return None
        
        return None

# ============================================================================
# MAIN COLLECTOR CLASS
# ============================================================================
class MoexBondDetailsCollector:
    """Main class for collecting bond data from MOEX API."""
    
    def __init__(self, isin: str = "RU000A0ZZ885"):
        """
        Initialize collector with target ISIN.
        
        Args:
            isin: Bond ISIN code (fixed for debugging per requirements)
        """
        self.isin = isin
        self.base_url = "https://iss.moex.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        
        # Setup directories and cleanup
        self._setup_directories()
        
        # Performance tracking
        self.total_start_time: Optional[float] = None
        self.total_end_time: Optional[float] = None
        self.endpoint_metrics: Dict[str, EndpointMetrics] = {}
        self.api_responses: Dict[str, MoexApiResponse] = {}
        
        # Configuration
        self.exclude_tables = [
            'security_search_securities',  # Redundant - same as security_info
            'security_info_boards'         # Board list not needed for analysis
        ]
    
    def _setup_directories(self):
        """Create necessary directories and cleanup old responses."""
        # Create directories
        os.makedirs('logs', exist_ok=True)
        os.makedirs('responses/bond_details', exist_ok=True)
        
        # Cleanup old responses for this ISIN
        self._cleanup_old_responses()
    
    def _cleanup_old_responses(self):
        """Remove old response files for current ISIN before run."""
        response_dir = Path('responses/bond_details')
        if response_dir.exists():
            pattern = f"{self.isin}_*"
            old_files = list(response_dir.glob(pattern))
            
            for file_path in old_files:
                try:
                    file_path.unlink()
                    logger.info(f"Removed old response: {file_path.name}")
                except Exception as e:
                    logger.warning(f"Could not remove {file_path}: {e}")
    
    def _make_request(self, url: str, description: str) -> Tuple[Optional[Dict], EndpointMetrics]:
        """
        Make HTTP request to MOEX API with comprehensive error handling.
        
        Args:
            url: Full API URL
            description: Human-readable endpoint description
            
        Returns:
            Tuple of (response_data, metrics_object)
        """
        metrics = EndpointMetrics(
            url=url,
            description=description,
            start_time=time.time()
        )
        
        try:
            logger.info(f"Запрос: {description}")
            logger.debug(f"URL: {url}")
            
            response = self.session.get(url, timeout=30)
            metrics.status_code = response.status_code
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    metrics.success = True
                    metrics.data_size = len(response.content)
                    logger.info(f"Успешный ответ: {description} ({metrics.data_size} байт)")
                    
                    # Save raw response for debugging
                    self._save_raw_response(url, response, data)
                    
                    return data, metrics
                    
                except json.JSONDecodeError as e:
                    metrics.error_message = f"Ошибка JSON: {e}"
                    logger.error(f"Невалидный JSON от {description}: {e}")
                    self._save_raw_response(url, response, None)
                    return None, metrics
            else:
                metrics.error_message = f"HTTP {response.status_code}"
                logger.warning(f"Ошибка {response.status_code} для {description}")
                self._save_raw_response(url, response, None)
                return None, metrics
                
        except requests.exceptions.RequestException as e:
            metrics.error_message = str(e)
            logger.error(f"Сбой запроса к {description}: {e}")
            return None, metrics
        finally:
            metrics.end_time = time.time()
            metrics.duration = metrics.end_time - metrics.start_time
    
    def _save_raw_response(self, url: str, response: requests.Response, data: Optional[Dict]):
        """Save raw API response to files for debugging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        parsed_url = urlparse(url)
        
        # Clean path for filename
        path_parts = parsed_url.path.strip('/').replace('/', '_').replace('.json', '')
        if path_parts.startswith('_'):
            path_parts = path_parts[1:]
        
        filename_base = f"{self.isin}_{parsed_url.netloc}_{path_parts}_{timestamp}"
        filename_base = filename_base.replace('__', '_')
        
        # Save JSON if valid data
        if data is not None:
            json_path = f"responses/bond_details/{filename_base}.json"
            try:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.debug(f"Сохранён JSON: {Path(json_path).name}")
            except Exception as e:
                logger.error(f"Ошибка сохранения JSON: {e}")
        
        # Always save text version
        txt_path = f"responses/bond_details/{filename_base}.txt"
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(f"URL: {url}\n")
                f.write(f"Время: {datetime.now().isoformat()}\n")
                f.write(f"Статус: {response.status_code}\n")
                f.write(f"Размер: {len(response.content)} байт\n")
                f.write("\n" + "="*60 + "\n")
                f.write("СЫРОЙ ОТВЕТ:\n")
                f.write("="*60 + "\n")
                
                # Try to pretty print JSON
                content_type = response.headers.get('content-type', '')
                if 'application/json' in content_type:
                    try:
                        json_data = response.json()
                        f.write(json.dumps(json_data, indent=2, ensure_ascii=False))
                    except:
                        f.write(response.text[:50000])
                else:
                    f.write(response.text[:50000])
                    
            logger.debug(f"Сохранён текст: {Path(txt_path).name}")
        except Exception as e:
            logger.error(f"Ошибка сохранения текста: {e}")
    
    # ============================================================================
    # API ENDPOINT COLLECTORS
    # ============================================================================
    
    def collect_security_info(self) -> Tuple[Optional[Dict], EndpointMetrics]:
        """Collect detailed security information - PRIMARY SOURCE."""
        url = f"{self.base_url}/iss/securities/{self.isin}.json?lang=ru"
        return self._make_request(url, "Детальная информация о бумаге")
    
    def collect_bond_market_data(self) -> Tuple[Optional[Dict], EndpointMetrics]:
        """Collect bond market data including securities, marketdata, and dataversion."""
        url = f"{self.base_url}/iss/engines/stock/markets/bonds/boards/TQOB/securities/{self.isin}.json?lang=ru"
        return self._make_request(url, "Рыночные данные облигации")
    
    def collect_bond_statistics(self) -> Tuple[Optional[Dict], EndpointMetrics]:
        """Collect bond statistics and analytics."""
        params = {
            'iss.only': 'bonds',
            'bonds.isin': self.isin,
            'lang': 'ru'
        }
        url = f"{self.base_url}/iss/statistics/engines/stock/markets/bonds/bonds.json?{urlencode(params)}"
        return self._make_request(url, "Статистика по облигации")
    
    # ============================================================================
    # DATA PROCESSING AND EXPORT
    # ============================================================================
    
    def _should_include_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """Determine if a table should be included in Excel output."""
        # Exclude specifically marked tables
        if table_name in self.exclude_tables:
            logger.info(f"Исключаем таблицу (по конфигурации): {table_name}")
            return False
        
        # Exclude completely empty tables
        if df.empty:
            logger.debug(f"Исключаем пустую таблицу: {table_name}")
            return False
        
        # Special case: dataversion with minimal data
        if 'dataversion' in table_name and len(df) <= 1:
            logger.debug(f"Таблица dataversion сохранена для отслеживания версий: {table_name}")
            return True
        
        return True
    
    def save_to_excel(self):
        """Save all processed data to Excel with intelligent formatting."""
        excel_filename = f"{self.isin}.xlsx"
        
        try:
            with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                all_tables_count = 0
                
                # Process each API response
                for endpoint_name, api_response in self.api_responses.items():
                    if not api_response.tables:
                        logger.warning(f"Нет таблиц для {endpoint_name}")
                        continue
                    
                    # Export each table from this response
                    for table_name, df in api_response.tables.items():
                        if self._should_include_table(table_name, df):
                            # Clean sheet name
                            sheet_name = table_name.replace('/', '_')[:31]
                            
                            # Write to Excel
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            
                            # Auto-adjust column widths
                            self._auto_adjust_column_widths(writer, sheet_name, df)
                            
                            all_tables_count += 1
                            logger.info(f"Сохранён лист '{sheet_name}': {len(df)} строк, {len(df.columns)} столбцов")
                
                # Save performance metrics
                self._save_metrics_to_excel(writer)
                
                # Save execution summary
                self._save_summary_to_excel(writer, all_tables_count)
            
            logger.info(f"Файл Excel успешно создан: {excel_filename}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка создания Excel файла: {e}")
            return False
    
    def _auto_adjust_column_widths(self, writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
        """Auto-adjust Excel column widths for better readability."""
        try:
            worksheet = writer.sheets[sheet_name]
            
            for i, column in enumerate(df.columns, 1):
                column_letter = self._get_column_letter(i)
                
                # Calculate max length in column
                max_length = 0
                col_idx = i - 1
                
                # Check header
                max_length = max(max_length, len(str(column)))
                
                # Check data rows (sample first 100 rows)
                sample_size = min(100, len(df))
                for j in range(sample_size):
                    cell_value = str(df.iloc[j, col_idx]) if col_idx < len(df.columns) else ""
                    max_length = max(max_length, len(cell_value))
                
                # Set width with limits
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
                
        except Exception as e:
            logger.debug(f"Не удалось настроить ширину столбцов для {sheet_name}: {e}")
    
    @staticmethod
    def _get_column_letter(col_idx: int) -> str:
        """Convert column index to Excel column letter (1->A, 27->AA, etc.)."""
        result = ""
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            result = chr(65 + remainder) + result
        return result
    
    def _save_metrics_to_excel(self, writer: pd.ExcelWriter):
        """Save detailed performance metrics to Excel."""
        if not self.endpoint_metrics:
            return
        
        metrics_data = []
        
        for endpoint_name, metrics in self.endpoint_metrics.items():
            response_time = f"{metrics.duration:.3f}" if metrics.duration else "N/A"
            
            metrics_data.append({
                'Эндпоинт': endpoint_name,
                'Описание': metrics.description,
                'Статус': 'УСПЕХ' if metrics.success else 'ОШИБКА',
                'Код ответа': metrics.status_code or 'N/A',
                'Время ответа (с)': response_time,
                'Размер данных (байт)': metrics.data_size,
                'Ошибка': metrics.error_message or ''
            })
        
        metrics_df = pd.DataFrame(metrics_data)
        metrics_df.to_excel(writer, sheet_name='metrics', index=False)
        
        # Format metrics sheet
        try:
            worksheet = writer.sheets['metrics']
            for i, column in enumerate(metrics_df.columns, 1):
                column_letter = self._get_column_letter(i)
                worksheet.column_dimensions[column_letter].width = 20
        except:
            pass
    
    def _save_summary_to_excel(self, writer: pd.ExcelWriter, tables_count: int):
        """Save execution summary to Excel."""
        if self.total_start_time and self.total_end_time:
            total_duration = self.total_end_time - self.total_start_time
        else:
            total_duration = 0
        
        successful_endpoints = sum(1 for m in self.endpoint_metrics.values() if m.success)
        total_endpoints = len(self.endpoint_metrics)
        
        summary_data = {
            'ISIN': self.isin,
            'Дата сбора': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Всего эндпоинтов': total_endpoints,
            'Успешных эндпоинтов': successful_endpoints,
            'Эндпоинтов с ошибкой': total_endpoints - successful_endpoints,
            'Всего таблиц в Excel': tables_count,
            'Общее время (с)': f"{total_duration:.2f}",
            'Среднее время ответа (с)': f"{sum(m.duration for m in self.endpoint_metrics.values() if m.duration)/max(total_endpoints, 1):.3f}",
            'Общий объём данных (КБ)': f"{sum(m.data_size for m in self.endpoint_metrics.values())/1024:.1f}"
        }
        
        summary_df = pd.DataFrame([summary_data])
        summary_df.to_excel(writer, sheet_name='summary', index=False)
        
        # Format summary sheet
        try:
            worksheet = writer.sheets['summary']
            for i, column in enumerate(summary_df.columns, 1):
                column_letter = self._get_column_letter(i)
                worksheet.column_dimensions[column_letter].width = 30
        except:
            pass
    
    # ============================================================================
    # MAIN EXECUTION FLOW
    # ============================================================================
    
    def run(self):
        """Main execution method orchestrating data collection and export."""
        logger.info("="*60)
        logger.info(f"НАЧАЛО СБОРА ДАННЫХ ДЛЯ ISIN: {self.isin}")
        logger.info("="*60)
        
        self.total_start_time = time.time()
        
        try:
            # Define endpoints to collect (excluding redundant ones per requirements)
            endpoints = [
                ("security_info", self.collect_security_info),
                ("bond_market_data", self.collect_bond_market_data),
                ("bond_statistics", self.collect_bond_statistics)
            ]
            
            # Collect data from all endpoints
            for endpoint_name, collector_func in endpoints:
                logger.info(f"--- Сбор: {endpoint_name.replace('_', ' ').upper()} ---")
                
                data, metrics = collector_func()
                self.endpoint_metrics[endpoint_name] = metrics
                
                if data:
                    # Process API response
                    api_response = MoexApiResponse(data, endpoint_name)
                    self.api_responses[endpoint_name] = api_response
                    
                    # Update metrics with rows extracted
                    total_rows = sum(len(df) for df in api_response.tables.values())
                    metrics.rows_extracted = total_rows
                    logger.info(f"Извлечено таблиц: {len(api_response.tables)}, строк: {total_rows}")
                else:
                    logger.warning(f"Нет данных от {endpoint_name}")
                
                # Polite delay between requests (except last)
                if endpoint_name != endpoints[-1][0]:
                    time.sleep(1)
            
            # Finalize timing
            self.total_end_time = time.time()
            
            # Export to Excel
            logger.info("--- Сохранение результатов в Excel ---")
            export_success = self.save_to_excel()
            
            # Log final summary
            self._log_execution_summary(export_success)
            
            return export_success
            
        except KeyboardInterrupt:
            logger.warning("Сбор данных прерван пользователем")
            return False
        except Exception as e:
            logger.error(f"Критическая ошибка при сборе данных: {e}")
            self.total_end_time = time.time()
            return False
    
    def _log_execution_summary(self, export_success: bool):
        """Log comprehensive execution summary."""
        total_duration = self.total_end_time - self.total_start_time
        successful_endpoints = sum(1 for m in self.endpoint_metrics.values() if m.success)
        
        logger.info("="*60)
        logger.info("ИТОГИ ВЫПОЛНЕНИЯ")
        logger.info("="*60)
        logger.info(f"ISIN: {self.isin}")
        logger.info(f"Общее время выполнения: {total_duration:.2f} секунд")
        logger.info(f"Эндпоинтов выполнено: {successful_endpoints}/{len(self.endpoint_metrics)}")
        
        # Detailed endpoint breakdown
        for endpoint_name, metrics in self.endpoint_metrics.items():
            status_icon = "✓" if metrics.success else "✗"
            logger.info(f"  {status_icon} {endpoint_name}: {metrics.duration:.3f}с, {metrics.data_size} байт, строк: {metrics.rows_extracted}")
        
        logger.info(f"Excel файл: {'Создан ✓' if export_success else 'Ошибка создания ✗'}")
        logger.info(f"Сырые ответы: responses/bond_details/")
        logger.info("="*60)

# ============================================================================
# ENTRY POINT
# ============================================================================
def main():
    """Main entry point for the script."""
    # Configuration - fixed ISIN as per requirements
    TARGET_ISIN = "RU000A0ZZ885"
    
    # Display startup banner
    print("\n" + "="*60)
    print("MOEX COLLECTOR - Сбор детальной информации по облигациям")
    print(f"Целевой ISIN: {TARGET_ISIN}")
    print(f"Время начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    # Initialize and run collector
    collector = MoexBondDetailsCollector(isin=TARGET_ISIN)
    
    try:
        success = collector.run()
        
        if success:
            print(f"\n✅ Сбор данных завершён успешно!")
            print(f"📊 Файл Excel: {TARGET_ISIN}.xlsx")
            print(f"📁 Ответы API: responses/bond_details/")
            print(f"📝 Логи: logs/moex_bond_details.log")
        else:
            print(f"\n⚠️  Сбор данных завершён с ошибками!")
            
    except Exception as e:
        print(f"\n❌ Непредвиденная ошибка: {e}")
        raise

if __name__ == "__main__":
    main()