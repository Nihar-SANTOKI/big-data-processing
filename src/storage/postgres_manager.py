# src/storage/postgres_manager.py
import psycopg2
import pandas as pd
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from src.config.settings import settings
from src.utils.logger import setup_logger
from sqlalchemy import create_engine
import psycopg2.pool

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
        
        self.engine = None
        self.connection_available = False
        
        # Test connection and create engine
        try:
            self._test_connection()
            # Create SQLAlchemy engine for pandas.to_sql()
            connection_string = f"postgresql://{settings.postgres.username}:{settings.postgres.password}@{settings.postgres.host}:{settings.postgres.port}/{settings.postgres.database}"
            self.engine = create_engine(connection_string)
            self.connection_available = True
            logger.info(f"PostgreSQL connection initialized for {settings.postgres.host}:{settings.postgres.port}/{settings.postgres.database}")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL connection: {e}")
            logger.warning("PostgreSQL operations will be disabled")
            self.connection_available = False
    
    def _test_connection(self):
        """Test if we can connect to PostgreSQL"""
        conn = psycopg2.connect(**self.connection_params)
        conn.close()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        if not self.connection_available:
            raise Exception("PostgreSQL connection not available")
            
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
        if not self.connection_available:
            logger.warning("PostgreSQL not available, skipping query execution")
            return False
            
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
        if not self.connection_available or not self.engine:
            logger.warning("PostgreSQL not available, cannot fetch data")
            return None
            
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                logger.info(f"Fetched {len(df)} rows from database")
                return df
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return None
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> bool:
        """Insert pandas DataFrame into PostgreSQL table"""
        if not self.connection_available or not self.engine:
            logger.warning("PostgreSQL not available, skipping DataFrame insertion")
            return False
            
        try:
            # Convert datetime columns to string to avoid timezone issues
            df_copy = df.copy()
            
            # Handle different data types that might cause issues
            for col in df_copy.columns:
                if df_copy[col].dtype == 'datetime64[ns]':
                    df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                elif df_copy[col].dtype == 'object' and col in ['trip_date']:
                    # Handle date objects
                    try:
                        df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    except:
                        pass  # Keep original if conversion fails
                elif df_copy[col].dtype == 'bool':
                    # Convert boolean to integer for PostgreSQL
                    df_copy[col] = df_copy[col].astype(int)
                elif df_copy[col].dtype == 'category':
                    # Convert categories to strings
                    df_copy[col] = df_copy[col].astype(str)
            
            # Drop rows with all NaN values
            df_copy = df_copy.dropna(how='all')
            
            if len(df_copy) == 0:
                logger.warning(f"No data to insert into {table_name}")
                return True
            
            # Use SQLAlchemy engine with better error handling
            try:
                rows_inserted = df_copy.to_sql(
                    table_name, 
                    self.engine, 
                    if_exists=if_exists, 
                    index=False, 
                    method='multi',
                    chunksize=1000
                )
                logger.info(f"Inserted {len(df_copy)} rows into {table_name}")
                return True
            except Exception as sql_error:
                # Try with basic method if multi method fails
                logger.warning(f"Multi-insert failed, trying basic method: {sql_error}")
                df_copy.to_sql(
                    table_name, 
                    self.engine, 
                    if_exists=if_exists, 
                    index=False,
                    chunksize=500
                )
                logger.info(f"Inserted {len(df_copy)} rows into {table_name} using basic method")
                return True
                
        except Exception as e:
            logger.error(f"Failed to insert DataFrame into {table_name}: {e}")
            # Log the DataFrame info for debugging
            logger.error(f"DataFrame shape: {df.shape}")
            logger.error(f"DataFrame columns: {df.columns.tolist()}")
            logger.error(f"DataFrame dtypes: {df.dtypes.to_dict()}")
            return False
    
    def create_tables(self) -> bool:
        """Create necessary tables for the project"""
        if not self.connection_available:
            logger.warning("PostgreSQL not available, skipping table creation")
            return False
            
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
            vendor_id INTEGER,
            tpep_pickup_datetime TIMESTAMP,  -- Keep original column name
            tpep_dropoff_datetime TIMESTAMP, -- Keep original column name
            passenger_count FLOAT,
            trip_distance FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT,
            trip_date DATE,
            hour_of_day INTEGER,
            day_of_week INTEGER,
            trip_duration_minutes FLOAT,
            is_weekend BOOLEAN,
            distance_category VARCHAR(20),
            fare_per_mile FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Daily aggregations table
        CREATE TABLE IF NOT EXISTS daily_trip_stats (
            id SERIAL PRIMARY KEY,
            trip_date DATE,
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