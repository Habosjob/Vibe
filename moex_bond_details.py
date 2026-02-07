#!/usr/bin/env python3
"""
MOEX Bond Details Collector - Enhanced version
With proper Excel formatting and cleanup
"""

import os
import json
import time
import logging
import shutil
from datetime import datetime
from urllib.parse import urlparse
import requests
import pandas as pd
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/moex_bond_details.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class EndpointMetrics:
    """Metrics for API endpoint call"""
    url: str
    description: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    success: bool = False
    status_code: Optional[int] = None
    data_size: int = 0
    error_message: Optional[str] = None

class MoexBondDetailsCollector:
    def __init__(self, isin: str = "RU000A0ZZ885"):
        """
        Initialize collector with fixed ISIN for debugging
        
        Args:
            isin: Bond ISIN code
        """
        self.isin = isin
        self.base_url = "https://iss.moex.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        
        # Setup directories
        self._setup_directories()
        
        # Performance metrics
        self.total_start_time: Optional[float] = None
        self.total_end_time: Optional[float] = None
        self.endpoint_metrics: Dict[str, EndpointMetrics] = {}
        self.collected_data: Dict[str, Any] = {}
        
    def _setup_directories(self):
        """Create necessary directories and cleanup old responses"""
        # Create directories
        os.makedirs('logs', exist_ok=True)
        os.makedirs('responses/bond_details', exist_ok=True)
        
        # Cleanup old responses for this ISIN before starting
        self._cleanup_old_responses()
    
    def _cleanup_old_responses(self):
        """Remove old response files for this ISIN"""
        response_dir = Path('responses/bond_details')
        if response_dir.exists():
            # Find files starting with current ISIN
            pattern = f"{self.isin}_*"
            old_files = list(response_dir.glob(pattern))
            
            for file_path in old_files:
                try:
                    file_path.unlink()
                    logger.info(f"Removed old response file: {file_path.name}")
                except Exception as e:
                    logger.warning(f"Could not remove {file_path}: {e}")
    
    def _make_request(self, url: str, description: str) -> Tuple[Optional[Dict], EndpointMetrics]:
        """
        Make HTTP request and collect metrics
        
        Args:
            url: URL to request
            description: Description for logging
            
        Returns:
            Tuple of (response_data, metrics)
        """
        metrics = EndpointMetrics(
            url=url,
            description=description,
            start_time=time.time()
        )
        
        try:
            logger.info(f"Requesting {description} from {url}")
            response = self.session.get(url, timeout=30)
            metrics.status_code = response.status_code
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    metrics.success = True
                    metrics.data_size = len(response.content)
                    logger.info(f"Successfully received {description} ({len(response.content)} bytes)")
                    
                    # Save raw response
                    self._save_response(url, response, data)
                    
                    return data, metrics
                    
                except json.JSONDecodeError as e:
                    metrics.error_message = f"JSON decode failed: {e}"
                    logger.error(f"JSON decode failed for {description}: {e}")
                    # Save as text anyway
                    self._save_response(url, response, None)
                    return None, metrics
                    
            else:
                metrics.error_message = f"HTTP {response.status_code}"
                logger.error(f"HTTP {response.status_code} for {description}")
                # Save error response
                self._save_response(url, response, None)
                return None, metrics
                
        except requests.exceptions.RequestException as e:
            metrics.error_message = str(e)
            logger.error(f"Request failed for {description}: {e}")
            return None, metrics
        finally:
            metrics.end_time = time.time()
            metrics.duration = metrics.end_time - metrics.start_time
    
    def _save_response(self, url: str, response: requests.Response, data: Optional[Dict]):
        """
        Save raw response to file with proper naming
        
        Args:
            url: Request URL
            response: Response object
            data: Parsed JSON data (if available)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        parsed_url = urlparse(url)
        
        # Clean path for filename - remove leading/trailing underscores
        path_parts = parsed_url.path.strip('/').replace('/', '_').replace('.json', '')
        if path_parts.startswith('_'):
            path_parts = path_parts[1:]
        
        # Create filename without double underscores
        filename_base = f"{self.isin}_{parsed_url.netloc}_{path_parts}_{timestamp}"
        filename_base = filename_base.replace('__', '_')
        
        # Save JSON if data is valid
        if data is not None:
            json_filename = f"responses/bond_details/{filename_base}.json"
            try:
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.debug(f"Saved JSON response to {json_filename}")
            except Exception as e:
                logger.error(f"Failed to save JSON: {e}")
        
        # Always save text version
        txt_filename = f"responses/bond_details/{filename_base}.txt"
        try:
            with open(txt_filename, 'w', encoding='utf-8') as f:
                f.write(f"URL: {url}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Status Code: {response.status_code}\n")
                f.write(f"Content-Type: {response.headers.get('content-type', 'unknown')}\n")
                f.write(f"Content-Length: {len(response.content)} bytes\n")
                f.write("\n" + "="*50 + "\n")
                f.write("RESPONSE CONTENT:\n")
                f.write("="*50 + "\n")
                
                # Try to pretty print JSON if possible
                try:
                    if response.headers.get('content-type', '').startswith('application/json'):
                        json_data = response.json()
                        f.write(json.dumps(json_data, indent=2, ensure_ascii=False))
                    else:
                        f.write(response.text[:20000])  # Limit to 20KB
                except:
                    f.write(response.text[:20000])
                    
            logger.debug(f"Saved text response to {txt_filename}")
        except Exception as e:
            logger.error(f"Failed to save text: {e}")
    
    def _extract_tables_from_moex_response(self, data: Dict, source: str) -> Dict[str, pd.DataFrame]:
        """
        Extract tables from MOEX API response
        
        Args:
            data: MOEX API response data
            source: Source identifier for naming
            
        Returns:
            Dictionary of {table_name: DataFrame}
        """
        tables = {}
        
        if not data or not isinstance(data, dict):
            logger.warning(f"No data or invalid data structure from {source}")
            return tables
        
        # MOEX API responses typically have a structure like:
        # {
        #   "securities": {
        #     "metadata": [...],
        #     "data": [...],
        #     "columns": [...]
        #   },
        #   "marketdata": { ... }
        # }
        
        # Check for top-level keys that contain data
        for top_key, top_value in data.items():
            if isinstance(top_value, dict):
                # Look for metadata/data structure
                if 'metadata' in top_value and 'data' in top_value:
                    metadata = top_value['metadata']
                    rows = top_value['data']
                    
                    if metadata and rows:
                        # Convert to DataFrame
                        try:
                            # Extract column names from metadata
                            columns = []
                            for meta in metadata:
                                if isinstance(meta, dict):
                                    columns.append(meta.get('name', 'unknown'))
                                else:
                                    columns.append(str(meta))
                            
                            # Create DataFrame
                            df = pd.DataFrame(rows, columns=columns)
                            
                            # Generate table name
                            table_name = f"{source}_{top_key}"
                            tables[table_name] = df
                            logger.info(f"Extracted table '{table_name}' with {len(df)} rows")
                            
                        except Exception as e:
                            logger.error(f"Failed to create DataFrame for {top_key}: {e}")
                
                # Also check for columns/data structure (alternative MOEX format)
                elif 'columns' in top_value and 'data' in top_value:
                    columns = top_value['columns']
                    rows = top_value['data']
                    
                    if columns and rows:
                        try:
                            df = pd.DataFrame(rows, columns=columns)
                            table_name = f"{source}_{top_key}"
                            tables[table_name] = df
                            logger.info(f"Extracted table '{table_name}' with {len(df)} rows")
                        except Exception as e:
                            logger.error(f"Failed to create DataFrame for {top_key}: {e}")
        
        # If no structured tables found, try to flatten the entire response
        if not tables:
            logger.info(f"No structured tables found in {source}, attempting to flatten...")
            try:
                # Try to create a simple key-value table from the entire response
                flat_data = self._flatten_dict(data)
                if flat_data:
                    df = pd.DataFrame([flat_data])
                    tables[f"{source}_flattened"] = df
                    logger.info(f"Created flattened table from {source}")
            except Exception as e:
                logger.error(f"Failed to flatten data from {source}: {e}")
        
        return tables
    
    def _flatten_dict(self, data: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten nested dictionary for Excel export"""
        items = {}
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.update(self._flatten_dict(v, new_key, sep))
            elif isinstance(v, list):
                # Convert list to string representation
                items[new_key] = json.dumps(v, ensure_ascii=False)[:1000]  # Limit length
            else:
                items[new_key] = v
        
        return items
    
    def collect_security_search(self) -> Tuple[Optional[Dict], EndpointMetrics]:
        """Collect security search data"""
        url = f"{self.base_url}/iss/securities.json?q={self.isin}&lang=en"
        return self._make_request(url, "Security Search")
    
    def collect_security_info(self) -> Tuple[Optional[Dict], EndpointMetrics]:
        """Collect detailed security information"""
        url = f"{self.base_url}/iss/securities/{self.isin}.json?lang=en"
        return self._make_request(url, "Security Info")
    
    def collect_bond_market_data(self) -> Tuple[Optional[Dict], EndpointMetrics]:
        """Collect bond market data"""
        url = f"{self.base_url}/iss/engines/stock/markets/bonds/boards/TQOB/securities/{self.isin}.json?lang=en"
        return self._make_request(url, "Bond Market Data")
    
    def collect_additional_info(self) -> Tuple[Optional[Dict], EndpointMetrics]:
        """Collect additional bond information"""
        url = f"{self.base_url}/iss/statistics/engines/stock/bondization/{self.isin}.json?lang=en"
        return self._make_request(url, "Additional Bond Info")
    
    def save_to_excel(self):
        """
        Save all collected data to Excel file with proper formatting
        """
        excel_filename = f"{self.isin}.xlsx"
        
        try:
            with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                # Export each endpoint's data
                for endpoint_name, data in self.collected_data.items():
                    if data:
                        tables = self._extract_tables_from_moex_response(data, endpoint_name)
                        
                        if tables:
                            for table_name, df in tables.items():
                                # Clean sheet name (Excel limit: 31 chars, no special chars)
                                sheet_name = table_name.replace('/', '_').replace('\\', '_')[:31]
                                
                                # Write to Excel with formatting
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                                
                                # Auto-adjust column widths
                                worksheet = writer.sheets[sheet_name]
                                for column in worksheet.columns:
                                    max_length = 0
                                    column_letter = column[0].column_letter
                                    
                                    for cell in column:
                                        try:
                                            if len(str(cell.value)) > max_length:
                                                max_length = len(str(cell.value))
                                        except:
                                            pass
                                    
                                    adjusted_width = min(max_length + 2, 50)  # Cap at 50
                                    worksheet.column_dimensions[column_letter].width = adjusted_width
                                
                                logger.info(f"Saved sheet '{sheet_name}' with {len(df)} rows")
                        else:
                            # Create a simple sheet with the raw data structure
                            logger.warning(f"No tables extracted from {endpoint_name}, saving raw structure")
                            try:
                                flat_data = self._flatten_dict(data)
                                df = pd.DataFrame([flat_data])
                                sheet_name = endpoint_name[:31]
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                            except Exception as e:
                                logger.error(f"Failed to save {endpoint_name}: {e}")
                
                # Save performance metrics
                self._save_metrics_to_excel(writer)
                
                # Save summary sheet
                self._save_summary_to_excel(writer)
            
            logger.info(f"Excel file successfully saved: {excel_filename}")
            
        except Exception as e:
            logger.error(f"Failed to save Excel file: {e}")
            raise
    
    def _save_metrics_to_excel(self, writer: pd.ExcelWriter):
        """Save performance metrics to Excel"""
        metrics_data = []
        
        for endpoint_name, metrics in self.endpoint_metrics.items():
            metrics_data.append({
                'Endpoint': endpoint_name,
                'Description': metrics.description,
                'Status': 'SUCCESS' if metrics.success else 'FAILED',
                'Status Code': metrics.status_code or 'N/A',
                'Response Time (s)': f"{metrics.duration:.3f}" if metrics.duration else 'N/A',
                'Data Size (bytes)': metrics.data_size,
                'Error Message': metrics.error_message or ''
            })
        
        if metrics_data:
            metrics_df = pd.DataFrame(metrics_data)
            metrics_df.to_excel(writer, sheet_name='performance_metrics', index=False)
            
            # Format the metrics sheet
            worksheet = writer.sheets['performance_metrics']
            for column in worksheet.columns:
                column_letter = column[0].column_letter
                worksheet.column_dimensions[column_letter].width = 20
    
    def _save_summary_to_excel(self, writer: pd.ExcelWriter):
        """Save summary information to Excel"""
        if self.total_start_time and self.total_end_time:
            total_duration = self.total_end_time - self.total_start_time
        else:
            total_duration = 0
        
        success_count = sum(1 for m in self.endpoint_metrics.values() if m.success)
        total_endpoints = len(self.endpoint_metrics)
        
        summary_data = {
            'ISIN': self.isin,
            'Collection Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Total Endpoints Called': total_endpoints,
            'Successful Endpoints': success_count,
            'Failed Endpoints': total_endpoints - success_count,
            'Success Rate': f"{(success_count/total_endpoints*100):.1f}%" if total_endpoints > 0 else "0%",
            'Total Script Time (s)': f"{total_duration:.2f}",
            'Average Response Time (s)': f"{sum(m.duration for m in self.endpoint_metrics.values() if m.duration)/max(total_endpoints, 1):.3f}",
            'Total Data Received (KB)': f"{sum(m.data_size for m in self.endpoint_metrics.values())/1024:.1f}",
            'Output Files': f"{self.isin}.xlsx + {len(list(Path('responses/bond_details').glob(f'{self.isin}_*')))} response files"
        }
        
        summary_df = pd.DataFrame([summary_data])
        summary_df.to_excel(writer, sheet_name='summary', index=False)
        
        # Format summary sheet
        worksheet = writer.sheets['summary']
        for column in worksheet.columns:
            column_letter = column[0].column_letter
            worksheet.column_dimensions[column_letter].width = 30
    
    def run(self):
        """Main execution method"""
        logger.info("="*60)
        logger.info(f"Starting data collection for ISIN: {self.isin}")
        logger.info("="*60)
        
        self.total_start_time = time.time()
        
        try:
            # Collect data from all endpoints
            endpoints = [
                ("security_search", self.collect_security_search),
                ("security_info", self.collect_security_info),
                ("bond_market_data", self.collect_bond_market_data),
                ("additional_info", self.collect_additional_info)
            ]
            
            for endpoint_name, collector_func in endpoints:
                logger.info(f"--- Collecting {endpoint_name.upper().replace('_', ' ')} ---")
                data, metrics = collector_func()
                
                self.endpoint_metrics[endpoint_name] = metrics
                if data:
                    self.collected_data[endpoint_name] = data
                
                # Polite delay between requests
                if endpoint_name != endpoints[-1][0]:
                    time.sleep(1)
            
            # Record total time
            self.total_end_time = time.time()
            
            # Save to Excel
            logger.info("--- Saving results to Excel ---")
            self.save_to_excel()
            
            # Log final summary
            self._log_summary()
            
            return True
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            self.total_end_time = time.time()
            return False
    
    def _log_summary(self):
        """Log summary of collection"""
        total_duration = self.total_end_time - self.total_start_time
        success_count = sum(1 for m in self.endpoint_metrics.values() if m.success)
        
        logger.info("="*60)
        logger.info("COLLECTION SUMMARY")
        logger.info("="*60)
        logger.info(f"ISIN: {self.isin}")
        logger.info(f"Total time: {total_duration:.2f} seconds")
        logger.info(f"Endpoints: {success_count}/{len(self.endpoint_metrics)} successful")
        
        # Log detailed metrics
        for endpoint_name, metrics in self.endpoint_metrics.items():
            status = "✓" if metrics.success else "✗"
            logger.info(f"  {status} {endpoint_name}: {metrics.duration:.3f}s, {metrics.data_size} bytes")
        
        logger.info(f"Output: {self.isin}.xlsx")
        logger.info("="*60)

def main():
    """Main function"""
    # Fixed ISIN for debugging
    isin = "RU000A0ZZ885"
    
    # Add startup banner
    print("\n" + "="*60)
    print(f"MOEX Bond Details Collector")
    print(f"ISIN: {isin}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    collector = MoexBondDetailsCollector(isin=isin)
    
    try:
        success = collector.run()
        if success:
            print(f"\n✅ Collection completed successfully!")
            print(f"📊 Excel file: {isin}.xlsx")
            print(f"📁 Responses: responses/bond_details/")
            print(f"📝 Logs: logs/moex_bond_details.log")
        else:
            print(f"\n❌ Collection completed with errors!")
        
    except KeyboardInterrupt:
        logger.warning("Collection interrupted by user")
        print("\n\n⚠️  Collection interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n❌ Unexpected error: {e}")
        raise

if __name__ == "__main__":
    main()