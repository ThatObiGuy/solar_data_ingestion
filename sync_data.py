#!/usr/bin/env python3
"""
Data sync script for fetching API data and updating Neon PostgreSQL database
"""

import os
import sys
import logging
import requests
import psycopg2
from datetime import datetime, timezone
import time
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class DataSyncError(Exception):
    """Custom exception for data sync errors"""
    pass

class APIClient:
    """Handle API interactions with retry logic"""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })
    
    def fetch_data(self, endpoint: str, max_retries: int = 3) -> Dict[str, Any]:
        """Fetch data from API with retry logic"""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching data from {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Successfully fetched {len(data) if isinstance(data, list) else 1} records")
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"API request failed (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise DataSyncError(f"API request failed after {max_retries} attempts: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff

class DatabaseManager:
    """Handle database operations"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
    
    def get_connection(self):
        """Get database connection"""
        try:
            return psycopg2.connect(self.database_url)
        except psycopg2.Error as e:
            raise DataSyncError(f"Database connection failed: {e}")
    
    def insert_time_series_data(self, data: List[Dict[str, Any]]) -> int:
        """
        Insert time series data into database
        Adjust this method based on your specific table structure
        """
        if not data:
            logger.info("No data to insert")
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Example query - adjust based on your table structure
            insert_query = """
                INSERT INTO time_series_data (
                    timestamp, 
                    value, 
                    source,
                    created_at
                ) VALUES (
                    %s, %s, %s, %s
                )
                ON CONFLICT (timestamp, source) 
                DO UPDATE SET 
                    value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            records_inserted = 0
            current_time = datetime.now(timezone.utc)
            
            for record in data:
                try:
                    # Adjust these field names based on your API response structure
                    timestamp = record.get('timestamp') or record.get('time') or current_time
                    value = record.get('value') or record.get('price') or record.get('data')
                    source = record.get('source', 'api')
                    
                    # Convert timestamp if it's a string
                    if isinstance(timestamp, str):
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    
                    cursor.execute(insert_query, (
                        timestamp,
                        value,
                        source,
                        current_time
                    ))
                    records_inserted += 1
                    
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Skipping invalid record: {record}, error: {e}")
                    continue
            
            conn.commit()
            logger.info(f"Successfully inserted/updated {records_inserted} records")
            return records_inserted
            
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            raise DataSyncError(f"Database operation failed: {e}")
        finally:
            if conn:
                conn.close()

def main():
    """Main sync process"""
    try:
        # Get environment variables
        database_url = os.getenv('DATABASE_URL')
        api_key = os.getenv('API_KEY')
        api_endpoint = os.getenv('API_ENDPOINT')
        
        if not all([database_url, api_key, api_endpoint]):
            missing = [var for var, val in [
                ('DATABASE_URL', database_url),
                ('API_KEY', api_key),
                ('API_ENDPOINT', api_endpoint)
            ] if not val]
            raise DataSyncError(f"Missing required environment variables: {', '.join(missing)}")
        
        logger.info("Starting data sync process")
        
        # Initialize API client and database manager
        api_client = APIClient(api_key, api_endpoint)
        db_manager = DatabaseManager(database_url)
        
        # Fetch data from API
        # Adjust the endpoint path based on your API
        api_data = api_client.fetch_data('')  # or '/data' or whatever your endpoint is
        
        # Handle different API response formats
        if isinstance(api_data, dict):
            # If API returns an object with data array
            records = api_data.get('data', [api_data])
        elif isinstance(api_data, list):
            # If API returns array directly
            records = api_data
        else:
            records = [api_data]
        
        # Insert data into database
        inserted_count = db_manager.insert_time_series_data(records)
        
        logger.info(f"Data sync completed successfully. Processed {inserted_count} records.")
        
    except DataSyncError as e:
        logger.error(f"Data sync failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during data sync: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()