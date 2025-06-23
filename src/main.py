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
from src.storage.local_file_manager import LocalFileManager
from src.monitoring.pipeline_monitor import PipelineMonitor

import pandas as pd

logger = setup_logger(__name__)

def get_storage_manager():
    """Use local file storage only"""
    storage_manager = LocalFileManager()
    logger.info("Using local file storage")
    return storage_manager

# Initialize storage manager
storage_manager = get_storage_manager()

class NYCTaxiDataPipeline:
    def __init__(self):
        # Initialize components
        self.spark_processor = SparkProcessor()
        self.data_validator = DataValidator()
        self.storage_manager = storage_manager
        self.postgres_manager = None
        self.monitor = PipelineMonitor()
        
        # Try to initialize PostgreSQL with better error handling
        try:
            self.postgres_manager = PostgreSQLManager()
            if self.postgres_manager.connection_available:
                logger.info("PostgreSQL manager initialized successfully")
            else:
                logger.warning("PostgreSQL manager created but connection not available")
                self.postgres_manager = None
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL manager: {e}")
            logger.warning("Pipeline will continue without PostgreSQL storage")
            self.postgres_manager = None

    def setup_database(self) -> bool:
        """Setup database tables with better error handling"""
        if not self.postgres_manager:
            logger.warning("PostgreSQL not available, skipping database setup")
            return True
            
        logger.info("Setting up database tables...")
        try:
            return self.postgres_manager.create_tables()
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            logger.warning("Continuing without database setup")
            return True  # Don't fail the entire pipeline

    def download_and_process_data(self) -> bool:
        """Main data processing pipeline with better error handling"""
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

                initial_count = df.count() if hasattr(df, 'count') else len(df)
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
                    quality_metrics = {"overall_quality_score": 0, "error": str(e)}

                # Continue with data processing
                cleaned_df = self.spark_processor.clean_taxi_data(df)
                cleaned_count = cleaned_df.count() if hasattr(cleaned_df, 'count') else len(cleaned_df)
                logger.info(f"After cleaning: {cleaned_count:,} records")

                enhanced_df = self.spark_processor.add_derived_features(cleaned_df)
                
                # Cache only if it's a Spark DataFrame
                if hasattr(enhanced_df, 'cache'):
                    enhanced_df.cache()

                # Write to HDFS (skip for pandas fallback)
                hdfs_path = f"{settings.data.hdfs_base_path}/processed/{month_name}"
                try:
                    if hasattr(self.spark_processor, 'write_to_hdfs'):
                        self.spark_processor.write_to_hdfs(enhanced_df, hdfs_path)
                except Exception as e:
                    logger.warning(f"HDFS write failed (expected with pandas fallback): {e}")

                # Sample data for PostgreSQL with better error handling
                try:
                    if hasattr(enhanced_df, 'sample'):  # Spark DataFrame
                        sample_df = enhanced_df.sample(fraction=0.01, seed=42)
                        sample_pandas = sample_df.toPandas()
                    else:  # Pandas DataFrame
                        sample_pandas = enhanced_df.sample(frac=0.01, random_state=42)

                    # Only try PostgreSQL if it's available
                    if self.postgres_manager:
                        # Prepare data for PostgreSQL using the method from spark_processor
                        postgres_df = self.spark_processor.prepare_for_postgres(enhanced_df)
                        
                        if hasattr(postgres_df, 'toPandas'):
                            postgres_pandas = postgres_df.toPandas()
                        else:
                            postgres_pandas = postgres_df
                        
                        # Take a sample for insertion
                        if len(postgres_pandas) > 1000:
                            postgres_sample = postgres_pandas.sample(n=1000, random_state=42)
                        else:
                            postgres_sample = postgres_pandas
                        
                        # Insert into PostgreSQL
                        try:
                            success = self.postgres_manager.insert_dataframe(
                                postgres_sample, 
                                'taxi_trips_processed',
                                if_exists='append'
                            )
                            if success:
                                logger.info("Successfully inserted sample data into PostgreSQL")
                            else:
                                logger.error("Failed to insert sample data into PostgreSQL")
                        except Exception as e:
                            logger.error(f"Failed to insert into PostgreSQL: {e}")
                    else:
                        logger.info("PostgreSQL not available, skipping database insertion")
                        
                except Exception as e:
                    logger.error(f"Failed to prepare sample data: {e}")

                # Calculate aggregations with better error handling
                try:
                    # Use existing methods from spark_processor
                    vendor_stats = self.spark_processor.calculate_aggregations(enhanced_df)
                    distance_stats = self.spark_processor.calculate_distance_patterns(enhanced_df)
                    payment_stats = self.spark_processor.calculate_payment_patterns(enhanced_df)
                    
                    # Convert to pandas for storage
                    if hasattr(vendor_stats, 'toPandas'):
                        vendor_pandas = vendor_stats.toPandas()
                        distance_pandas = distance_stats.toPandas()
                        payment_pandas = payment_stats.toPandas()
                    else:
                        vendor_pandas = vendor_stats
                        distance_pandas = distance_stats
                        payment_pandas = payment_stats
                    
                    logger.info("Successfully calculated aggregations")
                    
                    # Insert aggregated stats into PostgreSQL if available
                    if self.postgres_manager:
                        try:
                            # Create summary stats for daily_trip_stats table
                            summary_stats = pd.DataFrame([{
                                'total_trips': int(vendor_pandas['total_trips'].sum()),
                                'total_revenue': float(vendor_pandas['total_revenue'].sum()),
                                'avg_trip_distance': float(vendor_pandas['avg_trip_distance'].mean()),
                                'avg_fare_amount': float(vendor_pandas['avg_fare_amount'].mean()),
                                'avg_tip_amount': float(vendor_pandas['avg_tip_amount'].mean())
                            }])
                            
                            success = self.postgres_manager.insert_dataframe(
                                summary_stats, 
                                'daily_trip_stats', 
                                if_exists='append'
                            )
                            if success:
                                logger.info("Successfully inserted summary stats into PostgreSQL")
                        except Exception as e:
                            logger.error(f"Failed to insert summary stats: {e}")
                            
                except Exception as e:
                    logger.error(f"Failed to calculate aggregations: {e}")
                    vendor_pandas = None
                    distance_pandas = None
                    payment_pandas = None

                # Upload analytics to local storage
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Upload vendor statistics
                if 'vendor_pandas' in locals() and vendor_pandas is not None and len(vendor_pandas) > 0:
                    try:
                        success = self.storage_manager.upload_dataframe(
                            vendor_pandas,
                            f"analytics/vendor_stats_{month_name}_{timestamp}.parquet", 
                            'parquet'
                        )
                        if success:
                            logger.info("Successfully uploaded vendor stats to local storage")
                    except Exception as e:
                        logger.error(f"Failed to upload vendor stats: {e}")

                # Upload distance patterns
                if 'distance_pandas' in locals() and distance_pandas is not None and len(distance_pandas) > 0:
                    try:
                        success = self.storage_manager.upload_dataframe(
                            distance_pandas,
                            f"analytics/distance_patterns_{month_name}_{timestamp}.parquet", 
                            'parquet'
                        )
                        if success:
                            logger.info("Successfully uploaded distance patterns to local storage")
                    except Exception as e:
                        logger.error(f"Failed to upload distance patterns: {e}")

                # Upload payment patterns
                if 'payment_pandas' in locals() and payment_pandas is not None and len(payment_pandas) > 0:
                    try:
                        success = self.storage_manager.upload_dataframe(
                            payment_pandas,
                            f"analytics/payment_patterns_{month_name}_{timestamp}.parquet", 
                            'parquet'
                        )
                        if success:
                            logger.info("Successfully uploaded payment patterns to local storage")
                    except Exception as e:
                        logger.error(f"Failed to upload payment patterns: {e}")

                # Upload quality metrics
                if quality_metrics:
                    try:
                        quality_df = pd.DataFrame([quality_metrics])
                        success = self.storage_manager.upload_dataframe(
                            quality_df,
                            f"quality/quality_metrics_{month_name}_{timestamp}.csv", 
                            'csv'
                        )
                        if success:
                            logger.info("Successfully uploaded quality metrics to local storage")
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
            reports_generated = 0
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Try PostgreSQL first if available
            if self.postgres_manager:
                try:
                    # Query summary statistics
                    summary_query = """
                    SELECT total_trips, total_revenue, avg_trip_distance,
                           avg_fare_amount, avg_tip_amount
                    FROM daily_trip_stats
                    ORDER BY id DESC LIMIT 10
                    """
                    summary_data = self.postgres_manager.fetch_data(summary_query)

                    # Query processed trip data
                    trips_query = """
                    SELECT vendor_id, distance_category, COUNT(*) AS trip_count, 
                           AVG(fare_amount) AS avg_fare, AVG(tip_amount) AS avg_tip,
                           AVG(trip_distance) AS avg_distance
                    FROM taxi_trips_processed 
                    GROUP BY vendor_id, distance_category
                    ORDER BY vendor_id, distance_category
                    LIMIT 100
                    """
                    trips_data = self.postgres_manager.fetch_data(trips_query)

                    if summary_data is not None and len(summary_data) > 0:
                        success = self.storage_manager.upload_dataframe(
                            summary_data,
                            f"reports/summary_analytics_{timestamp}.csv", 
                            'csv'
                        )
                        if success:
                            logger.info(f"Generated summary analytics report with {len(summary_data)} records")
                            reports_generated += 1

                    if trips_data is not None and len(trips_data) > 0:
                        success = self.storage_manager.upload_dataframe(
                            trips_data,
                            f"reports/trips_analysis_{timestamp}.csv", 
                            'csv'
                        )
                        if success:
                            logger.info(f"Generated trips analysis report with {len(trips_data)} records")
                            reports_generated += 1

                    # Generate pipeline summary from available data
                    if summary_data is not None and len(summary_data) > 0:
                        pipeline_summary = {
                            'execution_date': datetime.now().isoformat(),
                            'total_records': len(summary_data),
                            'total_trips': int(summary_data['total_trips'].sum()) if 'total_trips' in summary_data.columns else 0,
                            'total_revenue': float(summary_data['total_revenue'].sum()) if 'total_revenue' in summary_data.columns else 0,
                            'avg_distance': float(summary_data['avg_trip_distance'].mean()) if 'avg_trip_distance' in summary_data.columns else 0,
                            'avg_fare': float(summary_data['avg_fare_amount'].mean()) if 'avg_fare_amount' in summary_data.columns else 0,
                            'data_source': 'PostgreSQL'
                        }
                        
                        summary_df = pd.DataFrame([pipeline_summary])
                        success = self.storage_manager.upload_dataframe(
                            summary_df,
                            f"reports/pipeline_summary_{timestamp}.csv", 
                            'csv'
                        )
                        if success:
                            logger.info(f"Pipeline Summary: {pipeline_summary}")
                            reports_generated += 1
                        
                except Exception as e:
                    logger.error(f"Failed to generate PostgreSQL-based reports: {e}")
            
            # Generate file-based report
            try:
                files = self.storage_manager.list_objects()
                
                report_data = {
                    'execution_date': datetime.now().isoformat(),
                    'total_files_generated': len(files),
                    'analytics_files': len([f for f in files if 'analytics/' in f]),
                    'quality_files': len([f for f in files if 'quality/' in f]),
                    'report_files': len([f for f in files if 'reports/' in f]),
                    'storage_type': 'Local',
                    'storage_path': os.path.abspath(self.storage_manager.base_path)
                }
                
                report_df = pd.DataFrame([report_data])
                success = self.storage_manager.upload_dataframe(
                    report_df,
                    f"reports/storage_summary_{timestamp}.csv",
                    'csv'
                )
                
                if success:
                    logger.info(f"Generated storage summary report: {report_data}")
                    reports_generated += 1
                
                logger.info(f"Generated {len(files)} total files, {reports_generated} reports in this run")
                if files:
                    logger.info(f"Files generated: {files}")
                    
            except Exception as e:
                logger.error(f"Failed to generate file-based report: {e}")

            return reports_generated > 0
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}", exc_info=True)
            return False


def main():
    """Main entry point"""
    logger = setup_logger(__name__)
    logger.info("=== NYC Taxi Data Processing Pipeline Started ===")
    
    # Log configuration
    logger.info("Storage mode: Local Files")
    
    # Create a SparkProcessor instance to check processing mode
    temp_processor = SparkProcessor()
    processing_mode = 'Pandas Fallback' if hasattr(temp_processor, 'use_pandas_fallback') and temp_processor.use_pandas_fallback else 'Spark'
    logger.info(f"Processing mode: {processing_mode}")
    temp_processor.stop()

    pipeline = NYCTaxiDataPipeline()

    # Setup database (don't fail if it doesn't work)
    database_setup = pipeline.setup_database()
    if database_setup:
        logger.info("Database setup completed successfully")
    else:
        logger.warning("Database setup failed, continuing with file-only processing")
    
    # Process data
    if not pipeline.download_and_process_data():
        logger.error("Pipeline failed")
        sys.exit(1)
    
    # Generate reports
    if not pipeline.generate_comprehensive_report():
        logger.error("Report generation failed, but pipeline data processing was successful")
        # Don't exit with error code since the main processing worked

    logger.info("=== Pipeline completed successfully! ===")
    
    # Show storage location
    logger.info(f"Results stored in: {os.path.abspath(storage_manager.base_path)}")


if __name__ == "__main__":
    main()