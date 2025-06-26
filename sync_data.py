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
from math import floor

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
    
    def fetch_data(self, endpoint: str, body: dict = None, max_retries: int = 3) -> Dict[str, Any]:
        """Fetch data from API with retry logic (POST with JSON body)"""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching data from {url} (attempt {attempt + 1})")
                response = self.session.post(url, json=body, timeout=30)
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
        Insert solar data into the solar_data table.
        """
        if not data:
            logger.info("No data to insert")
            return 0

        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            insert_query = """
                INSERT INTO solar_data (
                    site_id,
                    updated_time,
                    production_power_w,
                    consumption_power_w,
                    grid_power_w,
                    purchasing_power_w,
                    feed_in_power_w,
                    battery_power_w,
                    charging_power_w,
                    discharging_power_w,
                    soc_percent,
                    solar_status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (site_id, updated_time)
                DO UPDATE SET
                    production_power_w = EXCLUDED.production_power_w,
                    consumption_power_w = EXCLUDED.consumption_power_w,
                    grid_power_w = EXCLUDED.grid_power_w,
                    purchasing_power_w = EXCLUDED.purchasing_power_w,
                    feed_in_power_w = EXCLUDED.feed_in_power_w,
                    battery_power_w = EXCLUDED.battery_power_w,
                    charging_power_w = EXCLUDED.charging_power_w,
                    discharging_power_w = EXCLUDED.discharging_power_w,
                    soc_percent = EXCLUDED.soc_percent,
                    solar_status = EXCLUDED.solar_status
            """

            records_inserted = 0

            for record in data:
                try:
                    # Map API fields to DB columns
                    site_id = 1
                    raw_timestamp = datetime.fromtimestamp(record.get('lastUpdateTime', time.time()), tz=timezone.utc)
                    updated_time = floor_to_5_minutes(raw_timestamp)
                    production_power_w = record.get('generationPower')
                    consumption_power_w = record.get('usePower')
                    grid_power_w = record.get('gridPower')
                    purchasing_power_w = record.get('purchasePower')
                    feed_in_power_w = record.get('wirePower')
                    battery_power_w = record.get('batteryPower')
                    charging_power_w = record.get('chargePower')
                    discharging_power_w = record.get('dischargePower')
                    soc_percent = record.get('batterySoc')

                    # Generate solar_status string
                    solar_status = ",".join([
                        "1" if (production_power_w or 0) > 0 else "0",
                        "-1" if (battery_power_w or 0) < 0 else ("1" if (battery_power_w or 0) > 0 else "0"),
                        "-1" if (consumption_power_w or 0) > 0 else "0",
                        "1" if (grid_power_w or 0) > 0 else ("-1" if (grid_power_w or 0) < 0 else "0")
                    ])

                    cursor.execute(insert_query, (
                        site_id,
                        updated_time,
                        production_power_w,
                        consumption_power_w,
                        grid_power_w,
                        purchasing_power_w,
                        feed_in_power_w,
                        battery_power_w,
                        charging_power_w,
                        discharging_power_w,
                        soc_percent,
                        solar_status
                    ))
                    records_inserted += 1

                except Exception as e:
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

def floor_to_5_minutes(dt):
    """Floor a datetime object to the nearest 5-minute interval."""
    minutes = dt.minute // 5 * 5
    return dt.replace(minute=minutes, second=0, microsecond=0)

def main():
    """Main sync process"""
    try:
        # Get environment variables
        database_url = os.getenv('DATABASE_URL')
        api_key = os.getenv('API_KEY')
        api_endpoint = os.getenv('API_ENDPOINT')
        station_id = os.getenv('STATION_ID', '50133821')  # Default to 50133821 if not set
        
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
        
        # Prepare POST body
        post_body = {"stationId": int(station_id)}
        
        # Fetch data from API (POST)
        api_data = api_client.fetch_data('/station/v1.0/realTime', body=post_body)
        
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