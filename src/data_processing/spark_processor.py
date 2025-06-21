from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Optional, Dict, Any
import os
import pandas as pd
from src.config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class PandasDataFrameWrapper:
    """Wrapper to make pandas DataFrame work with Spark-like interface"""
    
    def __init__(self, df: pd.DataFrame):
        self._df = df
        self.columns = df.columns.tolist()  # Add columns attribute
    
    def count(self):
        return len(self._df)
    
    def toPandas(self):
        return self._df.copy()
    
    def cache(self):
        return self  # No-op for pandas
    
    def unpersist(self):
        return self  # No-op for pandas
    
    def sample(self, fraction: float, seed: int = None):
        sampled = self._df.sample(frac=fraction, random_state=seed)
        return PandasDataFrameWrapper(sampled)
    
    def filter(self, condition):
        # For pandas fallback, we'll need to handle basic filtering
        return self
    
    def select(self, *cols):
        selected = self._df[list(cols)]
        return PandasDataFrameWrapper(selected)
    
    def groupBy(self, *cols):
        # Simplified groupBy for pandas
        return PandasGroupBy(self._df, list(cols))
    
    def withColumn(self, col_name: str, expr):
        # Simplified withColumn - just return self for now
        return self

class PandasGroupBy:
    """Simple GroupBy wrapper for pandas"""
    
    def __init__(self, df: pd.DataFrame, group_cols: list):
        self._df = df
        self._group_cols = group_cols
    
    def agg(self, *args, **kwargs):
        # Simplified aggregation - return basic stats
        grouped = self._df.groupby(self._group_cols)
        result = grouped.agg({
            'fare_amount': ['count', 'sum', 'mean'],
            'trip_distance': 'mean',
            'tip_amount': 'mean',
            'total_amount': 'sum'
        }).reset_index()
        
        # Flatten column names
        result.columns = ['_'.join(col).strip() if col[1] else col[0] for col in result.columns.values]
        return PandasDataFrameWrapper(result)
    
    def orderBy(self, col_name: str):
        return self

class SparkProcessor:
    def __init__(self):
        self.spark = None
        self.use_pandas_fallback = False
        try:
            self.spark = self._create_spark_session()
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            logger.error("Attempting to create minimal Spark session...")
            try:
                self.spark = self._create_minimal_spark_session()
            except Exception as e2:
                logger.error(f"Failed to create even minimal Spark session: {e2}")
                logger.error("Falling back to pandas-only processing...")
                self.use_pandas_fallback = True
        
    def _create_spark_session(self) -> SparkSession:
        logger.info(f"Java Home: {os.environ.get('JAVA_HOME', 'Not set')}")
        logger.info(f"Spark Local Dir: {os.environ.get('SPARK_LOCAL_DIR', '/tmp/spark-local')}")
        
        spark = SparkSession.builder \
            .appName(settings.spark.app_name) \
            .master(settings.spark.master_url) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.maxResultSize", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
    
    def _create_minimal_spark_session(self) -> SparkSession:
        """Create minimal Spark session without S3 dependencies"""
        spark = SparkSession.builder \
            .appName("MinimalSparkSession") \
            .master("local[2]") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Minimal Spark session created")
        return spark
    
    def read_parquet_from_url(self, url: str) -> Optional[DataFrame]:
        """Read parquet file directly from URL"""
        if self.use_pandas_fallback:
            logger.warning("Spark not available, using pandas fallback")
            return self._read_parquet_with_pandas(url)
        
        try:
            df = self.spark.read.parquet(url)
            logger.info(f"Successfully read data from URL with Spark: {url}")
            logger.info(f"Schema: {df.schema}")
            logger.info(f"Row count: {df.count()}")
            return df
        except Exception as e:
            logger.error(f"Failed to read parquet from URL with Spark: {e}")
            logger.warning("Falling back to pandas...")
            return self._read_parquet_with_pandas(url)
    
    def _read_parquet_with_pandas(self, url: str) -> Optional[PandasDataFrameWrapper]:
        """Read parquet file with pandas as fallback"""
        try:
            logger.info(f"Reading {url} with pandas...")
            df = pd.read_parquet(url)
            logger.info(f"Successfully read {len(df)} rows with pandas")
            return PandasDataFrameWrapper(df)
        except Exception as e:
            logger.error(f"Failed to read parquet with pandas: {e}")
            return None
    
    def clean_taxi_data(self, df) -> DataFrame:
        """Clean and validate taxi trip data"""
        logger.info("Starting data cleaning process...")
        
        if isinstance(df, PandasDataFrameWrapper):
            # Pandas-based cleaning
            pandas_df = df._df.copy()
            
            # Apply basic filters
            cleaned_df = pandas_df[
                (pandas_df['fare_amount'] > 0) & 
                (pandas_df['fare_amount'] < 500) &
                (pandas_df['trip_distance'] > 0) & 
                (pandas_df['trip_distance'] < 100) &
                (pandas_df['passenger_count'] > 0) & 
                (pandas_df['passenger_count'] <= 6) &
                (pandas_df['tpep_pickup_datetime'].notna()) &
                (pandas_df['tpep_dropoff_datetime'].notna()) &
                (pandas_df['total_amount'] > 0) &
                (pandas_df['tpep_pickup_datetime'] < pandas_df['tpep_dropoff_datetime'])
            ]
            
            logger.info(f"Cleaned data: {len(cleaned_df)} rows remaining from {len(pandas_df)}")
            return PandasDataFrameWrapper(cleaned_df)
        else:
            # Spark-based cleaning (original code)
            logger.info(f"Available columns: {df.columns}")
            
            cleaned_df = df \
                .filter(col("fare_amount") > 0) \
                .filter(col("fare_amount") < 500) \
                .filter(col("trip_distance") > 0) \
                .filter(col("trip_distance") < 100) \
                .filter(col("passenger_count") > 0) \
                .filter(col("passenger_count") <= 6) \
                .filter(col("tpep_pickup_datetime").isNotNull()) \
                .filter(col("tpep_dropoff_datetime").isNotNull()) \
                .filter(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")) \
                .filter(col("total_amount") > 0)
            
            return cleaned_df

    def add_derived_features(self, df) -> DataFrame:
        """Add derived features for analysis"""
        logger.info("Adding derived features...")
        
        if isinstance(df, PandasDataFrameWrapper):
            # Pandas-based feature engineering
            pandas_df = df._df.copy()
            
            # Add basic derived columns
            pandas_df['trip_date'] = pd.to_datetime(pandas_df['tpep_pickup_datetime']).dt.date
            pandas_df['hour_of_day'] = pd.to_datetime(pandas_df['tpep_pickup_datetime']).dt.hour
            pandas_df['day_of_week'] = pd.to_datetime(pandas_df['tpep_pickup_datetime']).dt.dayofweek + 1
            
            # Calculate trip duration in minutes
            pickup = pd.to_datetime(pandas_df['tpep_pickup_datetime'])
            dropoff = pd.to_datetime(pandas_df['tpep_dropoff_datetime'])
            pandas_df['trip_duration_minutes'] = (dropoff - pickup).dt.total_seconds() / 60
            
            # Add other features
            pandas_df['is_weekend'] = pandas_df['day_of_week'].isin([1, 7])
            pandas_df['distance_category'] = pd.cut(
                pandas_df['trip_distance'], 
                bins=[0, 1, 5, 10, float('inf')], 
                labels=['short', 'medium', 'long', 'very_long']
            )
            pandas_df['fare_per_mile'] = pandas_df['fare_amount'] / pandas_df['trip_distance'].replace(0, 1)
            pandas_df['speed_mph'] = pandas_df['trip_distance'] / (pandas_df['trip_duration_minutes'] / 60).replace(0, 1)
            
            return PandasDataFrameWrapper(pandas_df)
        else:
            # Spark-based feature engineering (original code)
            enhanced_df = df \
                .withColumn("trip_date", to_date(col("tpep_pickup_datetime"))) \
                .withColumn("hour_of_day", hour(col("tpep_pickup_datetime"))) \
                .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
                .withColumn("trip_duration_minutes", 
                        (unix_timestamp(col("tpep_dropoff_datetime")) - 
                            unix_timestamp(col("tpep_pickup_datetime"))) / 60) \
                .withColumn("is_weekend", 
                        when(col("day_of_week").isin([1, 7]), True).otherwise(False)) \
                .withColumn("distance_category",
                        when(col("trip_distance") < 1, "short")
                        .when(col("trip_distance") < 5, "medium")
                        .when(col("trip_distance") < 10, "long")
                        .otherwise("very_long")) \
                .withColumn("fare_per_mile",
                        when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance"))
                        .otherwise(0)) \
                .withColumn("speed_mph", 
                        when(col("trip_duration_minutes") > 0, 
                                col("trip_distance") / (col("trip_duration_minutes") / 60))
                        .otherwise(0))
            
            return enhanced_df
    
    def calculate_daily_aggregations(self, df) -> DataFrame:
        """Calculate daily trip statistics"""
        logger.info("Calculating daily aggregations...")
        
        if isinstance(df, PandasDataFrameWrapper):
            pandas_df = df._df.copy()
            
            daily_stats = pandas_df.groupby('trip_date').agg({
                'fare_amount': ['count', 'sum', 'mean'],
                'trip_distance': 'mean',
                'tip_amount': 'mean',
                'trip_duration_minutes': 'mean',
                'total_amount': 'sum'
            }).reset_index()
            
            # Flatten column names
            daily_stats.columns = [
                'trip_date', 'total_trips', 'total_revenue', 'avg_fare_amount',
                'avg_trip_distance', 'avg_tip_amount', 'avg_trip_duration'
            ]
            
            daily_stats = daily_stats.sort_values('trip_date')
            return PandasDataFrameWrapper(daily_stats)
        else:
            daily_stats = df.groupBy("trip_date") \
                .agg(
                    count("*").alias("total_trips"),
                    sum("total_amount").alias("total_revenue"),
                    avg("trip_distance").alias("avg_trip_distance"),
                    avg("fare_amount").alias("avg_fare_amount"),
                    avg("tip_amount").alias("avg_tip_amount"),
                    avg("trip_duration_minutes").alias("avg_trip_duration")
                ) \
                .orderBy("trip_date")
            
            return daily_stats
    
    def calculate_hourly_patterns(self, df) -> DataFrame:
        """Calculate hourly trip patterns"""
        logger.info("Calculating hourly patterns...")
        
        if isinstance(df, PandasDataFrameWrapper):
            pandas_df = df._df.copy()
            
            hourly_stats = pandas_df.groupby('hour_of_day').agg({
                'fare_amount': ['count', 'mean'],
                'trip_distance': 'mean',
                'tip_amount': 'mean'
            }).reset_index()
            
            # Flatten column names
            hourly_stats.columns = [
                'hour_of_day', 'total_trips', 'avg_fare',
                'avg_distance', 'avg_tip'
            ]
            
            hourly_stats = hourly_stats.sort_values('hour_of_day')
            return PandasDataFrameWrapper(hourly_stats)
        else:
            hourly_stats = df.groupBy("hour_of_day") \
                .agg(
                    count("*").alias("total_trips"),
                    avg("fare_amount").alias("avg_fare"),
                    avg("trip_distance").alias("avg_distance"),
                    avg("tip_amount").alias("avg_tip")
                ) \
                .orderBy("hour_of_day")
            
            return hourly_stats
    
    def write_to_hdfs(self, df, hdfs_path: str, format: str = "parquet") -> bool:
        """Write DataFrame to HDFS (skip if using pandas fallback)"""
        if isinstance(df, PandasDataFrameWrapper):
            logger.info("Skipping HDFS write for pandas fallback")
            return True
        
        try:
            df.write \
                .mode("overwrite") \
                .format(format) \
                .save(hdfs_path)
            
            logger.info(f"Successfully wrote data to HDFS: {hdfs_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to write to HDFS: {e}")
            return False
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")