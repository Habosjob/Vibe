#!/usr/bin/env python3
"""
MOEX Bond Details Collector
Enhanced version with Excel export and performance metrics
"""

import os
import json
import time
import logging
from datetime import datetime
from urllib.parse import urlparse
import requests
import pandas as pd
from typing import Dict, List, Tuple, Any, Optional

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

class MoexBondDetailsCollector:
    def __init__(self, isin: str = "RU000A0ZZ885"):
        """
        Initialize collector with fixed ISIN for debugging
        
        Args:
            isin: Bond ISIN code (fixed for debugging)
        """
        self.isin = isin
        self.base_url = "https://iss.moex.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Create directories if they don't exist
        os.makedirs('logs', exist_ok=True)
        os.makedirs('responses/bond_details', exist_ok=True)
        
        # Performance metrics
        self.metrics = {
            'total_start_time': None,
            'total_end_time': None,
            'endpoints': {}
        }
    
    def _make_request(self, url: str, description: str) -> Tuple[Optional[Dict], Dict]:
        """
        Make HTTP request and collect metrics
        
        Args:
            url: URL to request
            description: Description for logging
            
        Returns:
            Tuple of (response_data, metrics_dict)
        """
        endpoint_metrics = {
            'url': url,
            'description': description,
            'start_time': time.time(),
            'end_time': None,
            'duration': None,
            'success': False,
            'status_code': None,
            'data_size': 0
        }
        
        try:
            logger.info(f"Requesting {description} from {url}")
            response = self.session.get(url, timeout=30)
            endpoint_metrics['status_code'] = response.status_code
            
            if response.status_code == 200:
                data = response.json()
                endpoint_metrics['success'] = True
                endpoint_metrics['data_size'] = len(response.content)
                logger.info(f"Successfully received {description}")
                
                # Save raw response
                self._save_response(url, response)
                
                return data, endpoint_metrics
            else:
                logger.error(f"HTTP {response.status_code} for {description}")
                return None, endpoint_metrics
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {description}: {e}")
            return None, endpoint_metrics
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode failed for {description}: {e}")
            return None, endpoint_metrics
        finally:
            endpoint_metrics['end_time'] = time.time()
            endpoint_metrics['duration'] = endpoint_metrics['end_time'] - endpoint_metrics['start_time']
    
    def _save_response(self, url: str, response: requests.Response):
        """
        Save raw response to file
        
        Args:
            url: Request URL
            response: Response object
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        parsed_url = urlparse(url)
        
        # Create filename
        filename_base = f"{self.isin}_{parsed_url.netloc}_{parsed_url.path.replace('/', '_').replace('.json', '')}_{timestamp}"
        
        # Save JSON
        json_filename = f"responses/bond_details/{filename_base}.json"
        try:
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Failed to save JSON: {e}")
        
        # Save text
        txt_filename = f"responses/bond_details/{filename_base}.txt"
        try:
            with open(txt_filename, 'w', encoding='utf-8') as f:
                f.write(f"URL: {url}\n")
                f.write(f"Status Code: {response.status_code}\n")
                f.write(f"Headers: {dict(response.headers)}\n")
                f.write(f"Content:\n{response.text[:10000]}\n")  # Limit text size
        except Exception as e:
            logger.error(f"Failed to save text: {e}")
    
    def collect_security_search(self) -> Tuple[Optional[Dict], Dict]:
        """Collect security search data"""
        url = f"{self.base_url}/iss/securities.json?q={self.isin}"
        return self._make_request(url, "Security Search")
    
    def collect_security_info(self) -> Tuple[Optional[Dict], Dict]:
        """Collect detailed security information"""
        url = f"{self.base_url}/iss/securities/{self.isin}.json"
        return self._make_request(url, "Security Info")
    
    def collect_bond_market_data(self) -> Tuple[Optional[Dict], Dict]:
        """Collect bond market data"""
        url = f"{self.base_url}/iss/engines/stock/markets/bonds/boards/TQOB/securities/{self.isin}.json"
        return self._make_request(url, "Bond Market Data")
    
    def _flatten_data_structure(self, data: Dict, sheet_name: str) -> Dict[str, List[Dict]]:
        """
        Flatten MOEX data structure into separate tables
        
        Args:
            data: Original MOEX response data
            sheet_name: Base name for sheet
            
        Returns:
            Dictionary of {table_name: list_of_rows}
        """
        tables = {}
        
        if not data or 'securities' not in data:
            return tables
        
        # Process each block in the data
        for block_name, block_data in data.get('securities', {}).items():
            if isinstance(block_data, dict):
                # Handle metadata and data arrays
                if 'metadata' in block_data and 'data' in block_data:
                    metadata = block_data['metadata']
                    rows = block_data['data']
                    
                    # Convert rows to dictionaries
                    table_data = []
                    for row in rows:
                        row_dict = {}
                        for i, field in enumerate(metadata):
                            field_name = field.get('name', f'field_{i}')
                            row_dict[field_name] = row[i] if i < len(row) else None
                        table_data.append(row_dict)
                    
                    if table_data:
                        table_name = f"{sheet_name}_{block_name}"
                        tables[table_name] = table_data
        
        return tables
    
    def save_to_excel(self, all_data: Dict[str, Tuple[Optional[Dict], Dict]]):
        """
        Save all collected data to Excel file
        
        Args:
            all_data: Dictionary with endpoint data and metrics
        """
        excel_filename = f"{self.isin}.xlsx"
        
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            # Save each endpoint's data
            for endpoint_name, (data, metrics) in all_data.items():
                if data:
                    tables = self._flatten_data_structure(data, endpoint_name)
                    
                    if tables:
                        for table_name, table_data in tables.items():
                            df = pd.DataFrame(table_data)
                            # Shorten sheet name if too long (Excel limit: 31 chars)
                            sheet_name = table_name[:31]
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            logger.info(f"Saved {len(table_data)} rows to sheet '{sheet_name}'")
                    else:
                        # Save as JSON string if cannot flatten
                        df = pd.DataFrame([{'raw_data': json.dumps(data, ensure_ascii=False)}])
                        sheet_name = endpoint_name[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        logger.info(f"Saved raw data to sheet '{sheet_name}'")
            
            # Save performance metrics
            self._save_metrics_to_excel(writer)
        
        logger.info(f"Excel file saved: {excel_filename}")
    
    def _save_metrics_to_excel(self, writer: pd.ExcelWriter):
        """Save performance metrics to Excel"""
        metrics_data = []
        
        # Total metrics
        total_duration = self.metrics['total_end_time'] - self.metrics['total_start_time']
        metrics_data.append({
            'Metric': 'Total Script Duration',
            'Value': f"{total_duration:.2f} seconds",
            'Endpoint': 'ALL',
            'Status': 'COMPLETED'
        })
        
        # Endpoint metrics
        for endpoint_name, endpoint_metrics in self.metrics['endpoints'].items():
            metrics_data.append({
                'Metric': 'Response Time',
                'Value': f"{endpoint_metrics.get('duration', 0):.3f} seconds",
                'Endpoint': endpoint_name,
                'Status': 'SUCCESS' if endpoint_metrics.get('success') else 'FAILED'
            })
            
            metrics_data.append({
                'Metric': 'Data Size',
                'Value': f"{endpoint_metrics.get('data_size', 0):,} bytes",
                'Endpoint': endpoint_name,
                'Status': 'SUCCESS' if endpoint_metrics.get('success') else 'FAILED'
            })
            
            metrics_data.append({
                'Metric': 'Status Code',
                'Value': endpoint_metrics.get('status_code', 'N/A'),
                'Endpoint': endpoint_name,
                'Status': 'SUCCESS' if endpoint_metrics.get('success') else 'FAILED'
            })
        
        # Create metrics DataFrame
        metrics_df = pd.DataFrame(metrics_data)
        metrics_df.to_excel(writer, sheet_name='performance_metrics', index=False)
        
        # Summary statistics
        success_count = sum(1 for m in self.metrics['endpoints'].values() if m.get('success'))
        total_endpoints = len(self.metrics['endpoints'])
        
        summary_data = {
            'Total Endpoints Called': total_endpoints,
            'Successful Endpoints': success_count,
            'Failed Endpoints': total_endpoints - success_count,
            'Total Script Time (s)': f"{total_duration:.2f}",
            'Average Response Time (s)': f"{sum(m.get('duration', 0) for m in self.metrics['endpoints'].values()) / max(total_endpoints, 1):.3f}",
            'Total Data Received (KB)': f"{sum(m.get('data_size', 0) for m in self.metrics['endpoints'].values()) / 1024:.1f}"
        }
        
        summary_df = pd.DataFrame([summary_data])
        summary_df.to_excel(writer, sheet_name='summary', index=False)
    
    def run(self):
        """Main execution method"""
        logger.info(f"Starting data collection for ISIN: {self.isin}")
        self.metrics['total_start_time'] = time.time()
        
        # Collect data from all endpoints
        all_data = {}
        
        # 1. Security Search
        search_data, search_metrics = self.collect_security_search()
        all_data['security_search'] = (search_data, search_metrics)
        self.metrics['endpoints']['security_search'] = search_metrics
        time.sleep(1)  # Polite delay
        
        # 2. Security Info
        info_data, info_metrics = self.collect_security_info()
        all_data['security_info'] = (info_data, info_metrics)
        self.metrics['endpoints']['security_info'] = info_metrics
        time.sleep(1)  # Polite delay
        
        # 3. Bond Market Data
        market_data, market_metrics = self.collect_bond_market_data()
        all_data['bond_market_data'] = (market_data, market_metrics)
        self.metrics['endpoints']['bond_market_data'] = market_metrics
        
        # Record total time
        self.metrics['total_end_time'] = time.time()
        
        # Save to Excel
        self.save_to_excel(all_data)
        
        # Log summary
        total_time = self.metrics['total_end_time'] - self.metrics['total_start_time']
        success_count = sum(1 for m in self.metrics['endpoints'].values() if m.get('success'))
        
        logger.info(f"Collection completed in {total_time:.2f} seconds")
        logger.info(f"Successfully collected data from {success_count}/{len(self.metrics['endpoints'])} endpoints")
        
        return all_data

def main():
    """Main function"""
    # Fixed ISIN for debugging (as per requirement 4.2)
    collector = MoexBondDetailsCollector(isin="RU000A0ZZ885")
    
    try:
        collector.run()
        logger.info("Script execution completed successfully")
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":
    main()