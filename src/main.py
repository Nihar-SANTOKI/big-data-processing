import os
import sys
from datetime import datetime

# Ensure project root is on Python path
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

# Determine storage manager (S3 or local)
try:
    from src.storage.s3_manager import S3Manager
    if not settings.aws.access_key_id or not settings.aws.s3_bucket:
        raise ImportError("AWS credentials not configured")
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
        # Initialize components
        self.spark_processor = SparkProcessor()
        self.data_validator = DataValidator()
        self.storage_manager = storage_manager
        self.postgres_manager = PostgreSQLManager()
        self.monitor = PipelineMonitor()

    def setup_database(self) -> bool:
        """Setup database tables"""
        logger.info("Setting up database tables...")
        try:
            return self.postgres_manager.create_tables()
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            return False

    def download_and_process_data(self) -> bool:
        """Main data processing pipeline"""
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

                # Validate schema first
                if not self.data_validator.validate_taxi_data_schema(df):
                    logger.error("Schema validation failed")
                    continue

                # Run quality validation
                try:
                    quality_metrics = self.data_validator.validate_data_quality(df)
                    logger.info(f"Data quality score: {quality_metrics['overall_quality_score']}%")
                except Exception as e:
                    logger.error(f"Data quality validation failed: {e}")
                    # Continue processing even if quality validation fails
                    quality_metrics = {"overall_quality_score": 0, "error": str(e)}

                # Continue with data processing
                cleaned_df = self.spark_processor.clean_taxi_data(df)
                cleaned_count = cleaned_df.count()
                logger.info(f"After cleaning: {cleaned_count:,} records")

                enhanced_df = self.spark_processor.add_derived_features(cleaned_df)
                
                # Cache only if it's a Spark DataFrame
                if hasattr(enhanced_df, 'cache') and not isinstance(enhanced_df, type(df)):
                    enhanced_df.cache()

                # Write to HDFS (skip for pandas fallback)
                hdfs_path = f"{settings.data.hdfs_base_path}/processed/{month_name}"
                self.spark_processor.write_to_hdfs(enhanced_df, hdfs_path)

                # Sample data for PostgreSQL
                sample_df = enhanced_df.sample(fraction=0.1, seed=42)
                sample_pandas = sample_df.toPandas()

                # Prepare data for PostgreSQL with proper column mapping
                postgres_columns = {
                    'VendorID': 'vendor_id',
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'passenger_count': 'passenger_count',
                    'trip_distance': 'trip_distance',
                    'fare_amount': 'fare_amount',
                    'tip_amount': 'tip_amount',
                    'total_amount': 'total_amount',
                    'trip_date': 'trip_date',
                    'hour_of_day': 'hour_of_day',
                    'day_of_week': 'day_of_week',
                    'trip_duration_minutes': 'trip_duration_minutes',
                    'is_weekend': 'is_weekend',
                    'distance_category': 'distance_category',
                    'fare_per_mile': 'fare_per_mile'
                }
                
                # Select only available columns
                available_columns = [col for col in postgres_columns.keys() if col in sample_pandas.columns]
                postgres_data = sample_pandas[available_columns].rename(columns={
                    col: postgres_columns[col] for col in available_columns
                })
                
                # Insert into PostgreSQL
                try:
                    self.postgres_manager.insert_dataframe(postgres_data, 'taxi_trips_processed')
                    logger.info("Successfully inserted sample data into PostgreSQL")
                except Exception as e:
                    logger.error(f"Failed to insert into PostgreSQL: {e}")

                # Calculate aggregations
                try:
                    daily_stats = self.spark_processor.calculate_daily_aggregations(enhanced_df)
                    daily_pandas = daily_stats.toPandas()
                    
                    # Ensure column names match database schema
                    if 'total_revenue' not in daily_pandas.columns and 'total_amount_sum' in daily_pandas.columns:
                        daily_pandas['total_revenue'] = daily_pandas['total_amount_sum']
                    
                    self.postgres_manager.insert_dataframe(daily_pandas, 'daily_trip_stats', 'replace')
                    logger.info("Successfully inserted daily stats into PostgreSQL")
                except Exception as e:
                    logger.error(f"Failed to calculate/insert daily stats: {e}")
                    daily_stats = None

                try:
                    hourly_stats = self.spark_processor.calculate_hourly_patterns(enhanced_df)
                    hourly_pandas = hourly_stats.toPandas()
                except Exception as e:
                    logger.error(f"Failed to calculate hourly patterns: {e}")
                    hourly_stats = None

                # Upload analytics to storage
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                if daily_stats:
                    try:
                        self.storage_manager.upload_dataframe(
                            daily_pandas,
                            f"analytics/daily_stats_{month_name}_{timestamp}.parquet", 
                            'parquet'
                        )
                    except Exception as e:
                        logger.error(f"Failed to upload daily stats: {e}")

                if hourly_stats:
                    try:
                        self.storage_manager.upload_dataframe(
                            hourly_pandas,
                            f"analytics/hourly_patterns_{month_name}_{timestamp}.parquet", 
                            'parquet'
                        )
                    except Exception as e:
                        logger.error(f"Failed to upload hourly patterns: {e}")

                # Upload quality metrics
                try:
                    quality_df = pd.DataFrame([quality_metrics])
                    self.storage_manager.upload_dataframe(
                        quality_df,
                        f"quality/quality_metrics_{month_name}_{timestamp}.csv", 
                        'csv'
                    )
                except Exception as e:
                    logger.error(f"Failed to upload quality metrics: {e}")

                # Cleanup
                if hasattr(enhanced_df, 'unpersist'):
                    enhanced_df.unpersist()

                processed_months.append(month_name)
                logger.info(f"Successfully processed {month_name}")
                self.monitor.log_system_metrics()

            logger.info(f"Pipeline completed! Processed {len(processed_months)} months")
            return len(processed_months) > 0
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return False
        finally:
            if hasattr(self.spark_processor, 'stop'):
                self.spark_processor.stop()

    def generate_comprehensive_report(self) -> bool:
        """Generate analytics reports from processed data"""
        logger.info("Generating comprehensive analytics report...")
        try:
            # Query daily statistics
            daily_query = """
            SELECT trip_date, total_trips, total_revenue, avg_trip_distance,
                   avg_fare_amount, avg_tip_amount, avg_trip_duration
            FROM daily_trip_stats
            ORDER BY trip_date DESC LIMIT 100
            """
            daily_data = self.postgres_manager.fetch_data(daily_query)

            # Query weekend analysis
            weekend_query = """
            SELECT is_weekend, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare,
                   AVG(tip_amount) AS avg_tip, AVG(trip_distance) AS avg_distance
            FROM taxi_trips_processed 
            GROUP BY is_weekend
            """
            weekend_data = self.postgres_manager.fetch_data(weekend_query)

            # Generate reports
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if daily_data is not None and len(daily_data) > 0:
                self.storage_manager.upload_dataframe(
                    daily_data,
                    f"reports/daily_analytics_{timestamp}.csv", 
                    'csv'
                )
                logger.info(f"Generated daily analytics report with {len(daily_data)} records")
            else:
                logger.warning("No daily data available for report")

            if weekend_data is not None and len(weekend_data) > 0:
                self.storage_manager.upload_dataframe(
                    weekend_data,
                    f"reports/weekend_analysis_{timestamp}.csv", 
                    'csv'
                )
                logger.info(f"Generated weekend analysis report with {len(weekend_data)} records")
            else:
                logger.warning("No weekend data available for report")

            # Generate summary statistics
            summary_stats = {}
            if daily_data is not None and len(daily_data) > 0:
                summary_stats = {
                    'execution_date': datetime.now().isoformat(),
                    'total_records': len(daily_data),
                    'total_trips': int(daily_data['total_trips'].sum()) if 'total_trips' in daily_data.columns else 0,
                    'total_revenue': float(daily_data['total_revenue'].sum()) if 'total_revenue' in daily_data.columns else 0,
                    'avg_distance': float(daily_data['avg_trip_distance'].mean()) if 'avg_trip_distance' in daily_data.columns else 0,
                    'avg_fare': float(daily_data['avg_fare_amount'].mean()) if 'avg_fare_amount' in daily_data.columns else 0
                }
                
                summary_df = pd.DataFrame([summary_stats])
                self.storage_manager.upload_dataframe(
                    summary_df,
                    f"reports/pipeline_summary_{timestamp}.csv", 
                    'csv'
                )
                logger.info(f"Pipeline Summary: {summary_stats}")
            else:
                logger.warning("No data available for summary statistics")

            # List generated files
            if hasattr(self.storage_manager, 'list_objects'):
                try:
                    files = self.storage_manager.list_objects()
                    logger.info(f"Generated {len(files)} files: {files[:10]}...")  # Show first 10 files
                except Exception as e:
                    logger.error(f"Failed to list generated files: {e}")

            return True
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}", exc_info=True)
            return False


def main():
    """Main entry point"""
    logger = setup_logger(__name__)
    logger.info("=== NYC Taxi Data Processing Pipeline Started ===")
    logger.info(f"Storage mode: {'S3' if hasattr(storage_manager, 's3_client') else 'Local Files'}")
    logger.info(f"Processing mode: {'Spark' if not hasattr('use_pandas_fallback') else 'Pandas Fallback'}")

    pipeline = NYCTaxiDataPipeline()

    # Setup database
    if not pipeline.setup_database():
        logger.error("Failed to setup database")
        sys.exit(1)
    
    # Process data
    if not pipeline.download_and_process_data():
        logger.error("Pipeline failed")
        sys.exit(1)
    
    # Generate reports
    if not pipeline.generate_comprehensive_report():
        logger.error("Report generation failed")
        sys.exit(1)

    logger.info("=== Pipeline completed successfully! ===")
    
    # Show storage location
    if hasattr(storage_manager, 'base_path'):
        logger.info(f"Results stored in: {os.path.abspath(storage_manager.base_path)}")
    else:
        logger.info("Results stored in S3 bucket")


if __name__ == "__main__":
    main()