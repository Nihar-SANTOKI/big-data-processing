import os
import sys
from datetime import datetime
from src.config.settings import settings
from src.utils.logger import setup_logger
from src.data_processing.spark_processor import SparkProcessor
from src.data_processing.data_validator import DataValidator
from src.storage.s3_manager import S3Manager
from src.storage.postgres_manager import PostgreSQLManager

logger = setup_logger(__name__)

class NYCTaxiDataPipeline:
    def __init__(self):
        self.spark_processor = SparkProcessor()
        self.data_validator = DataValidator()
        self.s3_manager = S3Manager()
        self.postgres_manager = PostgreSQLManager()
        
    def setup_database(self) -> bool:
        """Setup PostgreSQL database tables"""
        logger.info("Setting up database tables...")
        return self.postgres_manager.create_tables()
    
    def download_and_process_data(self) -> bool:
        """Main data processing pipeline"""
        try:
            logger.info("Starting NYC Taxi Data Processing Pipeline...")
            
            # Step 1: Read data from source
            logger.info("Step 1: Reading data from source...")
            df = self.spark_processor.read_parquet_from_url(settings.data.source_url)
            if df is None:
                logger.error("Failed to read source data")
                return False
            
            # Step 2: Data validation
            logger.info("Step 2: Validating raw data...")
            validation_results = self.data_validator.validate_data_quality(df)
            logger.info(f"Data quality results: {validation_results}")
            
            # Step 3: Data cleaning
            logger.info("Step 3: Cleaning data...")
            cleaned_df = self.spark_processor.clean_taxi_data(df)
            
            # Step 4: Add derived features
            logger.info("Step 4: Adding derived features...")
            enhanced_df = self.spark_processor.add_derived_features(cleaned_df)
            
            # Step 5: Save raw data to HDFS
            logger.info("Step 5: Saving raw data to HDFS...")
            hdfs_raw_path = f"{settings.data.hdfs_base_path}/raw/taxi_trips_raw"
            self.spark_processor.write_to_hdfs(cleaned_df, hdfs_raw_path)
            
            # Step 6: Save processed data to HDFS
            logger.info("Step 6: Saving processed data to HDFS...")
            hdfs_processed_path = f"{settings.data.hdfs_base_path}/processed/taxi_trips_processed"
            self.spark_processor.write_to_hdfs(enhanced_df, hdfs_processed_path)
            
            # Step 7: Save to PostgreSQL
            logger.info("Step 7: Saving to PostgreSQL...")
            
            # Select columns for processed table
            processed_columns = [
                "trip_date", "hour_of_day", "day_of_week", "VendorID",
                "passenger_count", "trip_distance", "trip_duration_minutes",
                "fare_amount", "tip_amount", "total_amount", "payment_type",
                "RatecodeID", "is_weekend", "distance_category", "fare_per_mile"
            ]
            
            # Rename columns to match database schema
            processed_df = enhanced_df.select(
                col("trip_date"),
                col("hour_of_day"),
                col("day_of_week"),
                col("VendorID").alias("vendor_id"),
                col("passenger_count"),
                col("trip_distance"),
                col("trip_duration_minutes"),
                col("fare_amount"),
                col("tip_amount"),
                col("total_amount"),
                col("payment_type"),
                col("RatecodeID").alias("rate_code_id"),
                col("is_weekend"),
                col("distance_category"),
                col("fare_per_mile")
            )
            
            self.spark_processor.write_to_postgresql(processed_df, "taxi_trips_processed")
            
            # Step 8: Calculate and save daily aggregations
            logger.info("Step 8: Calculating daily aggregations...")
            daily_stats = self.spark_processor.calculate_daily_aggregations(enhanced_df)
            self.spark_processor.write_to_postgresql(daily_stats, "daily_trip_stats", mode="overwrite")
            
            # Step 9: Calculate hourly patterns and save to S3
            logger.info("Step 9: Calculating hourly patterns...")
            hourly_stats = self.spark_processor.calculate_hourly_patterns(enhanced_df)
            
            # Convert to Pandas and save to S3
            hourly_pandas = hourly_stats.toPandas()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"analytics/hourly_patterns_{timestamp}.parquet"
            self.s3_manager.upload_dataframe(hourly_pandas, s3_key)
            
            # Step 10: Save summary statistics to S3
            logger.info("Step 10: Saving summary statistics...")
            daily_pandas = daily_stats.toPandas()
            s3_key = f"analytics/daily_stats_{timestamp}.parquet"
            self.s3_manager.upload_dataframe(daily_pandas, s3_key)
            
            logger.info("Pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return False
        finally:
            self.spark_processor.stop()
    
    def generate_analytics_report(self) -> bool:
        """Generate analytics report from processed data"""
        try:
            logger.info("Generating analytics report...")
            
            # Query daily statistics
            daily_query = """
            SELECT 
                trip_date,
                total_trips,
                total_revenue,
                avg_trip_distance,
                avg_fare_amount,
                avg_tip_amount
            FROM daily_trip_stats 
            ORDER BY trip_date DESC 
            LIMIT 30
            """
            
            daily_data = self.postgres_manager.fetch_data(daily_query)
            if daily_data is not None:
                # Save report to S3
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                report_key = f"reports/daily_analytics_{timestamp}.csv"
                self.s3_manager.upload_dataframe(daily_data, report_key, format='csv')
                logger.info(f"Analytics report saved to S3: {report_key}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to generate analytics report: {e}")
            return False

def main():
    """Main execution function"""
    pipeline = NYCTaxiDataPipeline()
    
    # Setup database
    if not pipeline.setup_database():
        logger.error("Failed to setup database")
        sys.exit(1)
    
    # Run data processing pipeline
    if not pipeline.download_and_process_data():
        logger.error("Pipeline execution failed")
        sys.exit(1)
    
    # Generate analytics report
    if not pipeline.generate_analytics_report():
        logger.error("Failed to generate analytics report")
        sys.exit(1)
    
    logger.info("All pipeline tasks completed successfully!")

if __name__ == "__main__":
    main()