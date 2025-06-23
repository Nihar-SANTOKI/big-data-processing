import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
import os
from src.config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class SparkProcessor:
    def __init__(self):
        self.spark = None
        self.use_pandas_fallback = True
        
        # Try to create Spark session, fall back to pandas if it fails
        try:
            self.spark = self._create_spark_session()
            self.use_pandas_fallback = False
            logger.info("Using Spark processing")
        except Exception as e:
            logger.warning(f"Spark initialization failed: {e}")
            logger.info("Falling back to pandas processing")
            self.use_pandas_fallback = True
        
    def _create_spark_session(self):
        """Create and configure Spark session"""
        from pyspark.sql import SparkSession
        
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
    
    def read_parquet_from_url(self, url: str) -> Optional[pd.DataFrame]:
        """Read parquet file from URL using pandas"""
        try:
            if self.use_pandas_fallback:
                df = pd.read_parquet(url)
                logger.info(f"Successfully read data from URL using pandas: {url}")
                logger.info(f"Shape: {df.shape}")
                return df
            else:
                # Use Spark if available
                df = self.spark.read.parquet(url)
                logger.info(f"Successfully read data from URL using Spark: {url}")
                logger.info(f"Schema: {df.schema}")
                logger.info(f"Row count: {df.count()}")
                return df
        except Exception as e:
            logger.error(f"Failed to read parquet from URL: {e}")
            return None
    
    def clean_taxi_data(self, df) -> pd.DataFrame:
        """Clean and validate taxi trip data"""
        logger.info("Starting data cleaning process...")
        
        if self.use_pandas_fallback:
            return self._clean_taxi_data_pandas(df)
        else:
            return self._clean_taxi_data_spark(df)
    
    def _clean_taxi_data_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean taxi data using pandas"""
        original_count = len(df)
        
        # Basic cleaning
        cleaned_df = df[
            (df['fare_amount'] > 0) & (df['fare_amount'] < 500) &
            (df['trip_distance'] > 0) & (df['trip_distance'] < 100) &
            (df['passenger_count'] > 0) & (df['passenger_count'] <= 6) &
            (df['total_amount'] > 0) & (df['total_amount'] < 1000)
        ].copy()
        
        # Remove outliers using IQR for fare_amount
        try:
            Q1 = cleaned_df['fare_amount'].quantile(0.25)
            Q3 = cleaned_df['fare_amount'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            cleaned_df = cleaned_df[
                (cleaned_df['fare_amount'] >= lower_bound) & 
                (cleaned_df['fare_amount'] <= upper_bound)
            ]
        except Exception as e:
            logger.warning(f"Could not apply IQR outlier removal: {e}")
        
        final_count = len(cleaned_df)
        logger.info(f"Data cleaning completed. Rows: {original_count} -> {final_count}")
        
        return cleaned_df
    
    def _clean_taxi_data_spark(self, df):
        """Clean taxi data using Spark (original implementation)"""
        from pyspark.sql.functions import col, expr
        
        original_count = df.count()
        
        cleaned_df = df \
            .filter(col("fare_amount") > 0) \
            .filter(col("fare_amount") < 500) \
            .filter(col("trip_distance") > 0) \
            .filter(col("trip_distance") < 100) \
            .filter(col("passenger_count") > 0) \
            .filter(col("passenger_count") <= 6) \
            .filter(col("total_amount") > 0) \
            .filter(col("total_amount") < 1000)
        
        try:
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
        except Exception as e:
            logger.warning(f"Could not apply IQR outlier removal: {e}")
        
        final_count = cleaned_df.count()
        logger.info(f"Data cleaning completed. Rows: {original_count} -> {final_count}")
        
        return cleaned_df
    
    def add_derived_features(self, df) -> pd.DataFrame:
        """Add derived features for analysis"""
        logger.info("Adding derived features...")
        
        if self.use_pandas_fallback:
            return self._add_derived_features_pandas(df)
        else:
            return self._add_derived_features_spark(df)
    
    def _add_derived_features_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features using pandas"""
        enhanced_df = df.copy()
        
        # Distance category
        enhanced_df['distance_category'] = pd.cut(
            enhanced_df['trip_distance'],
            bins=[0, 1, 5, 10, float('inf')],
            labels=['short', 'medium', 'long', 'very_long'],
            include_lowest=True
        )
        
        # Fare per mile
        enhanced_df['fare_per_mile'] = np.where(
            enhanced_df['trip_distance'] > 0,
            enhanced_df['fare_amount'] / enhanced_df['trip_distance'],
            0
        )
        
        # Tip percentage
        enhanced_df['tip_percentage'] = np.where(
            enhanced_df['fare_amount'] > 0,
            (enhanced_df['tip_amount'] / enhanced_df['fare_amount']) * 100,
            0
        )
        
        logger.info("Derived features added successfully")
        return enhanced_df
    
    def _add_derived_features_spark(self, df):
        """Add derived features using Spark (original implementation)"""
        from pyspark.sql.functions import col, when
        
        enhanced_df = df \
            .withColumn("distance_category",
                       when(col("trip_distance") < 1, "short")
                       .when(col("trip_distance") < 5, "medium")
                       .when(col("trip_distance") < 10, "long")
                       .otherwise("very_long")) \
            .withColumn("fare_per_mile",
                       when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance"))
                       .otherwise(0)) \
            .withColumn("tip_percentage",
                       when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100)
                       .otherwise(0))
        
        logger.info("Derived features added successfully")
        return enhanced_df
    
    def calculate_aggregations(self, df) -> pd.DataFrame:
        """Calculate trip statistics by vendor"""
        logger.info("Calculating aggregations...")
        
        if self.use_pandas_fallback:
            stats = df.groupby('VendorID').agg({
                'VendorID': 'count',
                'total_amount': 'sum',
                'trip_distance': 'mean',
                'fare_amount': 'mean',
                'tip_amount': 'mean',
                'passenger_count': 'mean'
            }).rename(columns={
                'VendorID': 'total_trips',
                'total_amount': 'total_revenue',
                'trip_distance': 'avg_trip_distance',
                'fare_amount': 'avg_fare_amount',
                'tip_amount': 'avg_tip_amount',
                'passenger_count': 'avg_passenger_count'
            }).reset_index()
        else:
            # Spark implementation would go here
            from pyspark.sql.functions import count, sum, avg
            stats = df.groupBy("VendorID") \
                .agg(
                    count("*").alias("total_trips"),
                    sum("total_amount").alias("total_revenue"),
                    avg("trip_distance").alias("avg_trip_distance"),
                    avg("fare_amount").alias("avg_fare_amount"),
                    avg("tip_amount").alias("avg_tip_amount"),
                    avg("passenger_count").alias("avg_passenger_count")
                ) \
                .orderBy("VendorID")
        
        logger.info("Aggregations calculated successfully")
        return stats
        
    def calculate_distance_patterns(self, df) -> pd.DataFrame:
        """Calculate patterns by distance category"""
        logger.info("Calculating distance patterns...")
        
        if self.use_pandas_fallback:
            distance_stats = df.groupby('distance_category').agg({
                'distance_category': 'count',
                'fare_amount': 'mean',
                'trip_distance': 'mean',
                'tip_amount': 'mean',
                'fare_per_mile': 'mean'
            }).rename(columns={
                'distance_category': 'total_trips',
                'fare_amount': 'avg_fare',
                'trip_distance': 'avg_distance',
                'tip_amount': 'avg_tip',
                'fare_per_mile': 'avg_fare_per_mile'
            }).reset_index()
        else:
            # Spark implementation
            from pyspark.sql.functions import count, avg
            distance_stats = df.groupBy("distance_category") \
                .agg(
                    count("*").alias("total_trips"),
                    avg("fare_amount").alias("avg_fare"),
                    avg("trip_distance").alias("avg_distance"),
                    avg("tip_amount").alias("avg_tip"),
                    avg("fare_per_mile").alias("avg_fare_per_mile")
                ) \
                .orderBy("distance_category")
        
        logger.info("Distance patterns calculated successfully")
        return distance_stats
    
    def calculate_payment_patterns(self, df) -> pd.DataFrame:
        """Calculate patterns by payment type"""
        logger.info("Calculating payment patterns...")
        
        if self.use_pandas_fallback:
            payment_stats = df.groupby('payment_type').agg({
                'payment_type': 'count',
                'fare_amount': 'mean',
                'tip_amount': 'mean',
                'total_amount': 'mean'
            }).rename(columns={
                'payment_type': 'total_trips',
                'fare_amount': 'avg_fare',
                'tip_amount': 'avg_tip',
                'total_amount': 'avg_total'
            }).reset_index()
            
            # Calculate tip percentage
            payment_stats['avg_tip_percentage'] = (
                payment_stats['avg_tip'] / payment_stats['avg_fare'] * 100
            ).fillna(0)
        else:
            # Spark implementation
            from pyspark.sql.functions import count, avg
            payment_stats = df.groupBy("payment_type") \
                .agg(
                    count("*").alias("total_trips"),
                    avg("fare_amount").alias("avg_fare"),
                    avg("tip_amount").alias("avg_tip"),
                    avg("total_amount").alias("avg_total"),
                    (avg("tip_amount") / avg("fare_amount") * 100).alias("avg_tip_percentage")
                ) \
                .orderBy("payment_type")
        
        logger.info("Payment patterns calculated successfully")
        return payment_stats
    
    def prepare_for_postgres(self, df) -> pd.DataFrame:
        """Prepare DataFrame for PostgreSQL insertion"""
        logger.info("Preparing DataFrame for PostgreSQL...")
        
        if self.use_pandas_fallback:
            # Select and rename columns for pandas
            postgres_df = df[[
                'VendorID', 'passenger_count', 'trip_distance', 'fare_amount',
                'tip_amount', 'total_amount', 'payment_type', 'RatecodeID',
                'distance_category', 'fare_per_mile'
            ]].copy()
            
            postgres_df = postgres_df.rename(columns={
                'VendorID': 'vendor_id',
                'RatecodeID': 'rate_code_id'
            })
            
            # Ensure proper data types
            postgres_df['passenger_count'] = postgres_df['passenger_count'].astype('float32')
            postgres_df['trip_distance'] = postgres_df['trip_distance'].astype('float32')
            postgres_df['fare_amount'] = postgres_df['fare_amount'].astype('float32')
            postgres_df['tip_amount'] = postgres_df['tip_amount'].astype('float32')
            postgres_df['total_amount'] = postgres_df['total_amount'].astype('float32')
            postgres_df['payment_type'] = postgres_df['payment_type'].astype('int32')
            postgres_df['rate_code_id'] = postgres_df['rate_code_id'].astype('int32')
            postgres_df['fare_per_mile'] = postgres_df['fare_per_mile'].astype('float32')
            
        else:
            # Spark implementation (original)
            from pyspark.sql.functions import col
            postgres_df = df.select(
                col("VendorID").alias("vendor_id"),
                col("passenger_count").cast("float"),
                col("trip_distance").cast("float"),
                col("fare_amount").cast("float"),
                col("tip_amount").cast("float"),
                col("total_amount").cast("float"),
                col("payment_type").cast("int"),
                col("RatecodeID").alias("rate_code_id").cast("int"),
                col("distance_category"),
                col("fare_per_mile").cast("float")
            )
        
        logger.info("DataFrame prepared for PostgreSQL")
        return postgres_df
    
    def get_basic_stats(self, df) -> Dict[str, Any]:
        """Get basic statistics about the dataset"""
        logger.info("Calculating basic statistics...")
        
        if self.use_pandas_fallback:
            stats = {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "columns": list(df.columns)
            }
            
            # Get numerical column statistics
            numerical_cols = ["fare_amount", "trip_distance", "tip_amount", "total_amount", "passenger_count"]
            for col_name in numerical_cols:
                if col_name in df.columns:
                    col_stats = df[col_name].describe()
                    stats[f"{col_name}_stats"] = {
                        "min": float(col_stats['min']),
                        "max": float(col_stats['max']),
                        "avg": round(float(col_stats['mean']), 2),
                        "stddev": round(float(col_stats['std']), 2)
                    }
        else:
            # Spark implementation (original)
            from pyspark.sql.functions import min, max, avg, stddev, col
            stats = {
                "total_rows": df.count(),
                "total_columns": len(df.columns),
                "columns": df.columns
            }
            
            numerical_cols = ["fare_amount", "trip_distance", "tip_amount", "total_amount", "passenger_count"]
            for col_name in numerical_cols:
                if col_name in df.columns:
                    col_stats = df.select(
                        min(col(col_name)).alias("min"),
                        max(col(col_name)).alias("max"),
                        avg(col(col_name)).alias("avg"),
                        stddev(col(col_name)).alias("stddev")
                    ).collect()[0]
                    
                    stats[f"{col_name}_stats"] = {
                        "min": col_stats["min"],
                        "max": col_stats["max"], 
                        "avg": round(col_stats["avg"], 2) if col_stats["avg"] else None,
                        "stddev": round(col_stats["stddev"], 2) if col_stats["stddev"] else None
                    }
        
        logger.info(f"Basic statistics calculated: {stats}")
        return stats
    
    def stop(self):
        """Stop Spark session if it exists"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")
        else:
            logger.info("No Spark session to stop (using pandas fallback)")