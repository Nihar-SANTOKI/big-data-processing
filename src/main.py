import os
import sys
from datetime import datetime, timedelta
from src.config.settings import settings
from src.utils.logger import setup_logger
from src.data_processing.spark_processor import SparkProcessor
from src.data_processing.data_validator import DataValidator
from src.storage.postgres_manager import PostgreSQLManager
from src.monitoring.pipeline_monitor import PipelineMonitor
from pyspark.sql.functions import col
import pandas as pd

# Use local file storage instead of S3 if AWS not configured
try:
    from src.storage.s3_manager import S3Manager
    if not settings.aws.access_key_id or not settings.aws.s3_bucket:
        raise ImportError("AWS not configured")
    storage_manager = S3Manager()
    logger.info("Using S3 for storage")
except:
    from src.storage.local_file_manager import LocalFileManager
    storage_manager = LocalFileManager()
    logger.info("Using local file storage")

logger = setup_logger(__name__)

class NYCTaxiDataPipeline:
    def __init__(self):
        self.spark_processor = SparkProcessor()
        self.data_validator = DataValidator()
        self.storage_manager = storage_manager  # Could be S3Manager or LocalFileManager
        self.postgres_manager = PostgreSQLManager()
        self.monitor = PipelineMonitor()
        
    def setup_database(self) -> bool:
        """Setup PostgreSQL database tables"""
        logger.info("Setting up database tables...")
        return self.postgres_manager.create_tables()
    
    def download_and_process_data(self) -> bool:
        """Main data processing pipeline with local storage fallback"""
        try:
            logger.info("Starting NYC Taxi Data Processing Pipeline...")
            self.monitor.log_system_metrics()
            
            # For testing, use just one month of data
            data_urls = [
                "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
            ]
            
            processed_months = []
            
            for url in data_urls:
                month_name = url.split('/')[-1].replace('.parquet', '')
                logger.info(f"Processing {month_name}...")
                
                # Read data
                df = self.spark_processor.read_parquet_from_url(url)
                if df is None:
                    logger.error(f"Failed to read data from {url}")
                    continue
                
                initial_count = df.count()
                logger.info(f"Initial dataset size: {initial_count:,} records")
                
                # Data validation
                if not self.data_validator.validate_taxi_data_schema(df):
                    logger.error("Schema validation failed")
                    continue
                
                # Data quality assessment
                quality_metrics = self.data_validator.validate_data_quality(df)
                logger.info(f"Data quality score: {quality_metrics['overall_quality_score']}%")
                
                # Data cleaning
                cleaned_df = self.spark_processor.clean_taxi_data(df)
                cleaned_count = cleaned_df.count()
                logger.info(f"After cleaning: {cleaned_count:,} records")
                
                # Feature engineering
                enhanced_df = self.spark_processor.add_derived_features(cleaned_df)
                enhanced_df.cache()
                
                # Store to HDFS
                hdfs_path = f"{settings.data.hdfs_base_path}/processed/{month_name}"
                self.spark_processor.write_to_hdfs(enhanced_df, hdfs_path)
                
                # Sample for PostgreSQL (10% sample to avoid memory issues)
                sample_df = enhanced_df.sample(fraction=0.1, seed=42)
                sample_pandas = sample_df.toPandas()
                
                # Prepare for PostgreSQL
                postgres_data = sample_pandas.rename(columns={
                    'VendorID': 'vendor_id',
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime'
                })
                
                # Store to PostgreSQL
                self.postgres_manager.insert_dataframe(postgres_data, 'taxi_trips_processed')
                
                # Calculate aggregations
                daily_stats = self.spark_processor.calculate_daily_aggregations(enhanced_df)
                daily_stats_pandas = daily_stats.toPandas()
                
                self.postgres_manager.insert_dataframe(daily_stats_pandas, 'daily_trip_stats', 'replace')
                
                hourly_stats = self.spark_processor.calculate_hourly_patterns(enhanced_df)
                hourly_pandas = hourly_stats.toPandas()
                
                # Store results (locally or S3)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Daily stats
                daily_key = f"analytics/daily_stats_{month_name}_{timestamp}.parquet"
                self.storage_manager.upload_dataframe(daily_stats_pandas, daily_key, 'parquet')
                
                # Hourly patterns
                hourly_key = f"analytics/hourly_patterns_{month_name}_{timestamp}.parquet"
                self.storage_manager.upload_dataframe(hourly_pandas, hourly_key, 'parquet')
                
                # Quality metrics
                quality_df = pd.DataFrame([quality_metrics])
                quality_key = f"quality/quality_metrics_{month_name}_{timestamp}.csv"
                self.storage_manager.upload_dataframe(quality_df, quality_key, 'csv')
                
                processed_months.append(month_name)
                enhanced_df.unpersist()
                
                logger.info(f"Successfully processed {month_name}")
                self.monitor.log_system_metrics()
            
            logger.info(f"Pipeline completed! Processed {len(processed_months)} months")
            return len(processed_months) > 0
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return False
        finally:
            self.spark_processor.stop()
    
    def generate_comprehensive_report(self) -> bool:
        """Generate analytics report with local storage"""
        try:
            logger.info("Generating comprehensive analytics report...")
            
            # Query database for reports
            daily_query = """
            SELECT trip_date, total_trips, total_revenue, avg_trip_distance, 
                   avg_fare_amount, avg_tip_amount, avg_trip_duration
            FROM daily_trip_stats 
            ORDER BY trip_date DESC LIMIT 100
            """
            daily_data = self.postgres_manager.fetch_data(daily_query)
            
            weekend_query = """
            SELECT is_weekend, COUNT(*) as trip_count, AVG(fare_amount) as avg_fare,
                   AVG(tip_amount) as avg_tip, AVG(trip_distance) as avg_distance
            FROM taxi_trips_processed GROUP BY is_weekend
            """
            weekend_data = self.postgres_manager.fetch_data(weekend_query)
            
            # Save reports
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if daily_data is not None:
                report_key = f"reports/daily_analytics_{timestamp}.csv"
                self.storage_manager.upload_dataframe(daily_data, report_key, 'csv')
            
            if weekend_data is not None:
                report_key = f"reports/weekend_analysis_{timestamp}.csv"
                self.storage_manager.upload_dataframe(weekend_data, report_key, 'csv')
            
            # Generate summary
            if daily_data is not None:
                summary_stats = {
                    'execution_date': datetime.now().isoformat(),
                    'total_records': len(daily_data),
                    'total_trips': daily_data['total_trips'].sum(),
                    'total_revenue': daily_data['total_revenue'].sum(),
                    'avg_distance': daily_data['avg_trip_distance'].mean(),
                    'avg_fare': daily_data['avg_fare_amount'].mean()
                }
                
                summary_df = pd.DataFrame([summary_stats])
                summary_key = f"reports/pipeline_summary_{timestamp}.csv"
                self.storage_manager.upload_dataframe(summary_df, summary_key, 'csv')
                
                logger.info(f"Pipeline Summary: {summary_stats}")
            
            # List all generated files
            if hasattr(self.storage_manager, 'list_objects'):
                files = self.storage_manager.list_objects()
                logger.info(f"Generated files: {files}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            return False

def main():
    """Main execution with local testing support"""
    logger.info("=== NYC Taxi Data Processing Pipeline Started ===")
    logger.info(f"Storage mode: {'S3' if hasattr(storage_manager, 's3_client') else 'Local Files'}")
    
    pipeline = NYCTaxiDataPipeline()
    
    # Step 1: Setup database
    logger.info("Step 1: Setting up database...")
    if not pipeline.setup_database():
        logger.error("Failed to setup database")
        sys.exit(1)
    
    # Step 2: Process data
    logger.info("Step 2: Processing data...")
    if not pipeline.download_and_process_data():
        logger.error("Pipeline failed")
        sys.exit(1)
    
    # Step 3: Generate reports
    logger.info("Step 3: Generating reports...")
    if not pipeline.generate_comprehensive_report():
        logger.error("Report generation failed")
        sys.exit(1)
    
    logger.info("=== Pipeline completed successfully! ===")
    
    # Show where results are stored
    if hasattr(storage_manager, 'base_path'):
        logger.info(f"Results stored in: {os.path.abspath(storage_manager.base_path)}")
        logger.info("Check these folders:")
        logger.info("- analytics/ : Daily and hourly statistics")
        logger.info("- reports/ : Comprehensive analysis reports")
        logger.info("- quality/ : Data quality metrics")
    else:
        logger.info("Results stored in S3 bucket")
    
    logger.info("PostgreSQL tables: daily_trip_stats, taxi_trips_processed")

if __name__ == "__main__":
    main()