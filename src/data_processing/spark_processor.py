from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Optional, Dict, Any
import os
from src.config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class SparkProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        spark = SparkSession.builder \
            .appName(settings.spark.app_name) \
            .master(settings.spark.master_url) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.maxResultSize", settings.spark.max_result_size) \
            .config("spark.driver.memory", settings.spark.driver_memory) \
            .config("spark.executor.memory", settings.spark.executor_memory) \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0,org.apache.hadoop:hadoop-aws:3.3.0") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
    
    def read_parquet_from_hdfs(self, hdfs_path: str) -> Optional[DataFrame]:
        """Read parquet file from HDFS"""
        try:
            df = self.spark.read.parquet(hdfs_path)
            logger.info(f"Successfully read data from HDFS: {hdfs_path}")
            logger.info(f"Schema: {df.schema}")
            logger.info(f"Row count: {df.count()}")
            return df
        except Exception as e:
            logger.error(f"Failed to read parquet from HDFS: {e}")
            return None
    
    def read_parquet_from_url(self, url: str) -> Optional[DataFrame]:
        """Read parquet file directly from URL"""
        try:
            df = self.spark.read.parquet(url)
            logger.info(f"Successfully read data from URL: {url}")
            logger.info(f"Schema: {df.schema}")
            logger.info(f"Row count: {df.count()}")
            return df
        except Exception as e:
            logger.error(f"Failed to read parquet from URL: {e}")
            return None
    
    def clean_taxi_data(self, df: DataFrame) -> DataFrame:
        """Clean and validate taxi trip data"""
        logger.info("Starting data cleaning process...")
        
        # Check actual column names first
        logger.info(f"Available columns: {df.columns}")
        
        # Use correct column names for 2023 data
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
            .filter(col("total_amount") > 0)  # Add this validation
        
        return cleaned_df

    def add_derived_features(self, df: DataFrame) -> DataFrame:
        """Add derived features for analysis"""
        logger.info("Adding derived features...")
        
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
    
    def calculate_daily_aggregations(self, df: DataFrame) -> DataFrame:
        """Calculate daily trip statistics"""
        logger.info("Calculating daily aggregations...")
        
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
        
        logger.info("Daily aggregations calculated successfully")
        return daily_stats
    
    def calculate_hourly_patterns(self, df: DataFrame) -> DataFrame:
        """Calculate hourly trip patterns"""
        logger.info("Calculating hourly patterns...")
        
        hourly_stats = df.groupBy("hour_of_day") \
            .agg(
                count("*").alias("total_trips"),
                avg("fare_amount").alias("avg_fare"),
                avg("trip_distance").alias("avg_distance"),
                avg("tip_amount").alias("avg_tip")
            ) \
            .orderBy("hour_of_day")
        
        logger.info("Hourly patterns calculated successfully")
        return hourly_stats
    
    def write_to_postgresql(self, df: DataFrame, table_name: str, mode: str = "append") -> bool:
        """Write DataFrame to PostgreSQL"""
        try:
            df.write \
                .format("jdbc") \
                .option("url", settings.postgres.connection_string) \
                .option("dbtable", table_name) \
                .option("user", settings.postgres.username) \
                .option("password", settings.postgres.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
            
            logger.info(f"Successfully wrote data to PostgreSQL table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL: {e}")
            return False
    
    def write_to_hdfs(self, df: DataFrame, hdfs_path: str, format: str = "parquet") -> bool:
        """Write DataFrame to HDFS"""
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