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
        """Create and configure Spark session"""
        spark = SparkSession.builder \
            .appName(settings.spark.app_name) \
            .master(settings.spark.master_url) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.maxResultSize", settings.spark.max_result_size) \
            .config("spark.driver.memory", settings.spark.driver_memory) \
            .config("spark.executor.memory", settings.spark.executor_memory) \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
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
        """Clean and validate taxi trip data (no datetime processing)"""
        logger.info("Starting data cleaning process...")
        
        # Original count
        original_count = df.count()
        
        # Basic cleaning - removed datetime filters
        cleaned_df = df \
            .filter(col("fare_amount") > 0) \
            .filter(col("fare_amount") < 500) \
            .filter(col("trip_distance") > 0) \
            .filter(col("trip_distance") < 100) \
            .filter(col("passenger_count") > 0) \
            .filter(col("passenger_count") <= 6)
        
        # Remove outliers using IQR for fare_amount
        quantiles = cleaned_df.select(
            expr("percentile_approx(fare_amount, 0.25)").alias("q1"),
            expr("percentile_approx(fare_amount, 0.75)").alias("q3")
        ).collect()[0]
        
        iqr = quantiles["q3"] - quantiles["q1"]
        lower_bound = quantiles["q1"] - 1.5 * iqr
        upper_bound = quantiles["q3"] + 1.5 * iqr
        
        cleaned_df = cleaned_df.filter(
            (col("fare_amount") >= lower_bound) & 
            (col("fare_amount") <= upper_bound)
        )
        
        final_count = cleaned_df.count()
        logger.info(f"Data cleaning completed. Rows: {original_count} -> {final_count}")
        
        return cleaned_df
    
    def add_derived_features(self, df: DataFrame) -> DataFrame:
        """Add derived features for analysis (no datetime features)"""
        logger.info("Adding derived features...")
        
        enhanced_df = df \
            .withColumn("distance_category",
                       when(col("trip_distance") < 1, "short")
                       .when(col("trip_distance") < 5, "medium")
                       .when(col("trip_distance") < 10, "long")
                       .otherwise("very_long")) \
            .withColumn("fare_per_mile",
                       when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance"))
                       .otherwise(0))
        
        logger.info("Derived features added successfully")
        return enhanced_df
    
    def calculate_aggregations(self, df: DataFrame) -> DataFrame:
        """Calculate trip statistics by vendor"""
        logger.info("Calculating aggregations...")
        
        stats = df.groupBy("VendorID") \
            .agg(
                count("*").alias("total_trips"),
                sum("total_amount").alias("total_revenue"),
                avg("trip_distance").alias("avg_trip_distance"),
                avg("fare_amount").alias("avg_fare_amount"),
                avg("tip_amount").alias("avg_tip_amount")
            ) \
            .orderBy("VendorID")
        
        logger.info("Aggregations calculated successfully")
        return stats
    
    def calculate_distance_patterns(self, df: DataFrame) -> DataFrame:
        """Calculate patterns by distance category"""
        logger.info("Calculating distance patterns...")
        
        distance_stats = df.groupBy("distance_category") \
            .agg(
                count("*").alias("total_trips"),
                avg("fare_amount").alias("avg_fare"),
                avg("trip_distance").alias("avg_distance"),
                avg("tip_amount").alias("avg_tip")
            ) \
            .orderBy("distance_category")
        
        logger.info("Distance patterns calculated successfully")
        return distance_stats
    
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