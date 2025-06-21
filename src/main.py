import os
import sys
from datetime import datetime

# Ensure project root is on Python path
# This allows imports like 'from src.config.settings import settings'
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from src.config.settings import settings
from src.utils.logger import setup_logger
from src.data_processing.spark_processor import SparkProcessor
from src.data_processing.data_validator import DataValidator
from src.storage.postgres_manager import PostgreSQLManager
from src.monitoring.pipeline_monitor import PipelineMonitor

import pandas as pd
from pyspark.sql.functions import col

# Determine storage manager (S3 or local)
try:
    from src.storage.s3_manager import S3Manager
    if not settings.aws.access_key_id or not settings.aws.s3_bucket:
        raise ImportError
    storage_manager = S3Manager()
    logger = setup_logger(__name__)
    logger.info("Using S3 for storage")
except ImportError:
    from src.storage.local_file_manager import LocalFileManager
    storage_manager = LocalFileManager()
    logger = setup_logger(__name__)
    logger.info("Using local file storage")

class NYCTaxiDataPipeline:
    def __init__(self):
        # Initialize Spark processor first to ensure Spark context is available
        self.spark_processor = SparkProcessor()
        # Then initialize other components that might use Spark functions
        self.data_validator = DataValidator()
        self.storage_manager = storage_manager
        self.postgres_manager = PostgreSQLManager()
        self.monitor = PipelineMonitor()

    def setup_database(self) -> bool:
        logger.info("Setting up database tables...")
        return self.postgres_manager.create_tables()

    def download_and_process_data(self) -> bool:
        logger.info("Starting NYC Taxi Data Processing Pipeline...")
        self.monitor.log_system_metrics()

        data_urls = [
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
        ]
        processed_months = []

        try:
            for url in data_urls:
                month_name = url.split('/')[-1].replace('.parquet', '')
                logger.info(f"Processing {month_name}...")

                # Read data first
                df = self.spark_processor.read_parquet_from_url(url)
                if df is None:
                    logger.error(f"Failed to read data from {url}")
                    continue

                initial_count = df.count()
                logger.info(f"Initial dataset size: {initial_count:,} records")

                # Validate schema first (doesn't use Spark functions heavily)
                if not self.data_validator.validate_taxi_data_schema(df):
                    logger.error("Schema validation failed")
                    continue

                # Now run quality validation (uses Spark functions)
                quality_metrics = self.data_validator.validate_data_quality(df)
                logger.info(f"Data quality score: {quality_metrics['overall_quality_score']}%")

                # Continue with rest of processing...
                cleaned_df = self.spark_processor.clean_taxi_data(df)
                cleaned_count = cleaned_df.count()
                logger.info(f"After cleaning: {cleaned_count:,} records")

                enhanced_df = self.spark_processor.add_derived_features(cleaned_df)
                enhanced_df.cache()

                hdfs_path = f"{settings.data.hdfs_base_path}/processed/{month_name}"
                self.spark_processor.write_to_hdfs(enhanced_df, hdfs_path)

                sample_df = enhanced_df.sample(fraction=0.1, seed=42)
                sample_pandas = sample_df.toPandas()

                postgres_data = sample_pandas.rename(columns={
                    'VendorID': 'vendor_id',
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime'
                })
                self.postgres_manager.insert_dataframe(postgres_data, 'taxi_trips_processed')

                daily_stats = self.spark_processor.calculate_daily_aggregations(enhanced_df)
                self.postgres_manager.insert_dataframe(daily_stats.toPandas(), 'daily_trip_stats', 'replace')

                hourly_stats = self.spark_processor.calculate_hourly_patterns(enhanced_df)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                # Upload analytics
                self.storage_manager.upload_dataframe(daily_stats.toPandas(),
                                                      f"analytics/daily_stats_{month_name}_{timestamp}.parquet", 'parquet')
                self.storage_manager.upload_dataframe(hourly_stats.toPandas(),
                                                      f"analytics/hourly_patterns_{month_name}_{timestamp}.parquet", 'parquet')

                # Upload quality metrics
                quality_df = pd.DataFrame([quality_metrics])
                self.storage_manager.upload_dataframe(quality_df,
                                                      f"quality/quality_metrics_{month_name}_{timestamp}.csv", 'csv')

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
        logger.info("Generating comprehensive analytics report...")
        try:
            daily_query = """
            SELECT trip_date, total_trips, total_revenue, avg_trip_distance,
                   avg_fare_amount, avg_tip_amount, avg_trip_duration
            FROM daily_trip_stats
            ORDER BY trip_date DESC LIMIT 100
            """
            daily_data = self.postgres_manager.fetch_data(daily_query)

            weekend_query = """
            SELECT is_weekend, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare,
                   AVG(tip_amount) AS avg_tip, AVG(trip_distance) AS avg_distance
            FROM taxi_trips_processed GROUP BY is_weekend
            """
            weekend_data = self.postgres_manager.fetch_data(weekend_query)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if daily_data is not None:
                self.storage_manager.upload_dataframe(daily_data,
                                                      f"reports/daily_analytics_{timestamp}.csv", 'csv')
            if weekend_data is not None:
                self.storage_manager.upload_dataframe(weekend_data,
                                                      f"reports/weekend_analysis_{timestamp}.csv", 'csv')

            summary_stats = {}
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
                self.storage_manager.upload_dataframe(summary_df,
                                                      f"reports/pipeline_summary_{timestamp}.csv", 'csv')
                logger.info(f"Pipeline Summary: {summary_stats}")

            if hasattr(self.storage_manager, 'list_objects'):
                files = self.storage_manager.list_objects()
                logger.info(f"Generated files: {files}")

            return True
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            return False


def main():
    logger = setup_logger(__name__)
    logger.info("=== NYC Taxi Data Processing Pipeline Started ===")
    logger.info(f"Storage mode: {'S3' if hasattr(storage_manager, 's3_client') else 'Local Files'}")

    pipeline = NYCTaxiDataPipeline()

    if not pipeline.setup_database():
        logger.error("Failed to setup database")
        sys.exit(1)
    if not pipeline.download_and_process_data():
        logger.error("Pipeline failed")
        sys.exit(1)
    if not pipeline.generate_comprehensive_report():
        logger.error("Report generation failed")
        sys.exit(1)

    logger.info("=== Pipeline completed successfully! ===")
    if hasattr(storage_manager, 'base_path'):
        logger.info(f"Results stored in: {os.path.abspath(storage_manager.base_path)}")
    else:
        logger.info("Results stored in S3 bucket")

if __name__ == "__main__":
    main()
