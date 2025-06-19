import psycopg2
import pandas as pd
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from src.config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class PostgreSQLManager:
    def __init__(self):
        self.connection_params = {
            'host': settings.postgres.host,
            'port': settings.postgres.port,
            'database': settings.postgres.database,
            'user': settings.postgres.username,
            'password': settings.postgres.password
        }
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> bool:
        """Execute a query (INSERT, UPDATE, DELETE)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    conn.commit()
                    logger.info(f"Query executed successfully: {query[:100]}...")
                    return True
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return False
    
    def fetch_data(self, query: str, params: Optional[tuple] = None) -> Optional[pd.DataFrame]:
        """Fetch data and return as pandas DataFrame"""
        try:
            with self.get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                logger.info(f"Fetched {len(df)} rows from database")
                return df
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return None
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> bool:
        """Insert pandas DataFrame into PostgreSQL table"""
        try:
            with self.get_connection() as conn:
                df.to_sql(
                    table_name, 
                    conn, 
                    if_exists=if_exists, 
                    index=False, 
                    method='multi',
                    chunksize=1000
                )
                logger.info(f"Inserted {len(df)} rows into {table_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to insert DataFrame: {e}")
            return False
    
    def create_tables(self) -> bool:
        """Create necessary tables for the project"""
        create_tables_sql = """
        -- Raw taxi trips table
        CREATE TABLE IF NOT EXISTS taxi_trips_raw (
            id SERIAL PRIMARY KEY,
            vendor_id INTEGER,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DECIMAL(8,2),
            pickup_longitude DECIMAL(10,6),
            pickup_latitude DECIMAL(10,6),
            rate_code_id INTEGER,
            store_and_fwd_flag CHAR(1),
            dropoff_longitude DECIMAL(10,6),
            dropoff_latitude DECIMAL(10,6),
            payment_type INTEGER,
            fare_amount DECIMAL(8,2),
            extra DECIMAL(8,2),
            mta_tax DECIMAL(8,2),
            tip_amount DECIMAL(8,2),
            tolls_amount DECIMAL(8,2),
            total_amount DECIMAL(8,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Processed taxi trips table
        CREATE TABLE IF NOT EXISTS taxi_trips_processed (
            id SERIAL PRIMARY KEY,
            trip_date DATE,
            hour_of_day INTEGER,
            day_of_week INTEGER,
            vendor_id INTEGER,
            passenger_count INTEGER,
            trip_distance DECIMAL(8,2),
            trip_duration_minutes INTEGER,
            fare_amount DECIMAL(8,2),
            tip_amount DECIMAL(8,2),
            total_amount DECIMAL(8,2),
            payment_type INTEGER,
            rate_code_id INTEGER,
            is_weekend BOOLEAN,
            distance_category VARCHAR(20),
            fare_per_mile DECIMAL(8,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Daily aggregations table
        CREATE TABLE IF NOT EXISTS daily_trip_stats (
            id SERIAL PRIMARY KEY,
            trip_date DATE UNIQUE,
            total_trips INTEGER,
            total_revenue DECIMAL(12,2),
            avg_trip_distance DECIMAL(8,2),
            avg_fare_amount DECIMAL(8,2),
            avg_tip_amount DECIMAL(8,2),
            avg_trip_duration DECIMAL(8,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_taxi_trips_raw_pickup_datetime ON taxi_trips_raw(pickup_datetime);
        CREATE INDEX IF NOT EXISTS idx_taxi_trips_processed_trip_date ON taxi_trips_processed(trip_date);
        CREATE INDEX IF NOT EXISTS idx_daily_trip_stats_trip_date ON daily_trip_stats(trip_date);
        """
        
        return self.execute_query(create_tables_sql)