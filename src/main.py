import os
import sys
from datetime import datetime, timedelta
from src.config.settings import settings
from src.utils.logger import setup_logger
from src.data_processing.spark_processor import SparkProcessor
from src.data_processing.data_validator import DataValidator
from src.storage.s3_manager import S3Manager
from src.storage.postgres_manager import PostgreSQLManager
from src.monitoring.pipeline_monitor import PipelineMonitor
from pyspark.sql.functions import col
import pandas as pd

logger = setup_logger(__name__)

class NYCTaxiDataPipeline:
    def __init__(self):
        self.spark_processor = SparkProcessor()
        self.data_validator = DataValidator()
        self.s3_manager = S3Manager()
        self.postgres_manager = PostgreSQLManager()
        self.monitor = PipelineMonitor()
        
    def setup_database(self) -> bool:
        """Setup PostgreSQL database tables"""
        logger.info("Setting up database tables...")
        return self.postgres_manager.create_tables()
    
    def download_and_process_data(self) -> bool:
        """Main data processing pipeline with comprehensive processing"""
        try:
            logger.info("Starting NYC Taxi Data Processing Pipeline...")
            self.monitor.log_system_metrics()
            
            # Step 1: Read data with retry logic
            max_retries = 3
            df = None
            
            # Multiple data sources for comprehensive processing
            data_urls = [
                "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
                "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
                "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet"
            ]
            
            processed_months = []
            
            for url in data_urls:
                month_name = url.split('/')[-1].replace('.parquet', '')
                logger.info(f"Processing {month_name}...")
                
                for attempt in range(max_retries):
                    try:
                        logger.info(f"Attempt {attempt + 1}: Reading data from {url}...")
                        df = self.spark_processor.read_parquet_from_url(url)
                        if df is not None:
                            break
                    except Exception as e:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}")
                        if attempt == max_retries - 1:
                            logger.error(f"All retry attempts failed for {url}")
                            continue
                
                if df is None:
                    logger.error(f"Failed to read data from {url}")
                    continue
                
                # Data size monitoring
                initial_count = df.count()
                logger.info(f"Initial dataset size for {month_name}: {initial_count:,} records")
                
                # Step 2: Data validation
                logger.info("Validating data schema...")
                if not self.data_validator.validate_taxi_data_schema(df):
                    logger.error("Schema validation failed")
                    continue
                
                # Step 3: Data quality assessment
                quality_metrics = self.data_validator.validate_data_quality(df)
                logger.info(f"Data quality score: {quality_metrics['overall_quality_score']}%")
                
                if quality_metrics['overall_quality_score'] < 80:
                    logger.warning("Data quality below threshold, but continuing processing...")
                
                # Step 4: Data cleaning
                logger.info("Cleaning data...")
                cleaned_df = self.spark_processor.clean_taxi_data(df)
                cleaned_count = cleaned_df.count()
                logger.info(f"After cleaning: {cleaned_count:,} records ({cleaned_count/initial_count*100:.1f}% retained)")
                
                # Step 5: Feature engineering
                logger.info("Adding derived features...")
                enhanced_df = self.spark_processor.add_derived_features(cleaned_df)
                
                # Step 6: Cache for multiple operations
                enhanced_df.cache()
                
                # Step 7: Store processed data to HDFS
                hdfs_path = f"{settings.data.hdfs_base_path}/processed/{month_name}"
                if self.spark_processor.write_to_hdfs(enhanced_df, hdfs_path):
                    logger.info(f"Successfully stored processed data to HDFS: {hdfs_path}")
                
                # Step 8: Sample data for PostgreSQL (to avoid memory issues)
                sample_df = enhanced_df.sample(fraction=0.1, seed=42)  # 10% sample
                sample_pandas = sample_df.toPandas()
                
                # Prepare data for PostgreSQL
                postgres_data = sample_pandas.rename(columns={
                    'VendorID': 'vendor_id',
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime'
                })
                
                # Step 9: Store to PostgreSQL
                if self.postgres_manager.insert_dataframe(postgres_data, 'taxi_trips_processed'):
                    logger.info(f"Successfully stored sample data to PostgreSQL for {month_name}")
                
                # Step 10: Calculate and store daily aggregations
                daily_stats = self.spark_processor.calculate_daily_aggregations(enhanced_df)
                daily_stats_pandas = daily_stats.toPandas()
                
                if self.postgres_manager.insert_dataframe(daily_stats_pandas, 'daily_trip_stats', 'replace'):
                    logger.info(f"Successfully stored daily aggregations for {month_name}")
                
                # Step 11: Calculate hourly patterns
                hourly_stats = self.spark_processor.calculate_hourly_patterns(enhanced_df)
                
                # Step 12: Store analytics results to S3
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Store daily stats
                daily_key = f"analytics/daily_stats_{month_name}_{timestamp}.parquet"
                if self.s3_manager.upload_dataframe(daily_stats_pandas, daily_key, 'parquet'):
                    logger.info(f"Daily stats uploaded to S3: {daily_key}")
                
                # Store hourly patterns
                hourly_pandas = hourly_stats.toPandas()
                hourly_key = f"analytics/hourly_patterns_{month_name}_{timestamp}.parquet"
                if self.s3_manager.upload_dataframe(hourly_pandas, hourly_key, 'parquet'):
                    logger.info(f"Hourly patterns uploaded to S3: {hourly_key}")
                
                # Store quality metrics
                quality_df = pd.DataFrame([quality_metrics])
                quality_key = f"quality/quality_metrics_{month_name}_{timestamp}.csv"
                if self.s3_manager.upload_dataframe(quality_df, quality_key, 'csv'):
                    logger.info(f"Quality metrics uploaded to S3: {quality_key}")
                
                processed_months.append(month_name)
                enhanced_df.unpersist()  # Free up memory
                
                logger.info(f"Successfully processed {month_name}")
                self.monitor.log_system_metrics()
            
            logger.info(f"Pipeline completed successfully! Processed {len(processed_months)} months of data")
            logger.info(f"Processed months: {processed_months}")
            
            return len(processed_months) > 0
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
            return False
        finally:
            self.spark_processor.stop()
    
    def generate_comprehensive_report(self) -> bool:
        """Generate comprehensive analytics report"""
        try:
            logger.info("Generating comprehensive analytics report...")
            
            # 1. Daily statistics
            daily_query = """
            SELECT 
                trip_date,
                total_trips,
                total_revenue,
                avg_trip_distance,
                avg_fare_amount,
                avg_tip_amount,
                avg_trip_duration
            FROM daily_trip_stats 
            ORDER BY trip_date DESC 
            LIMIT 100
            """
            
            daily_data = self.postgres_manager.fetch_data(daily_query)
            
            # 2. Weekend vs Weekday analysis
            weekend_query = """
            SELECT 
                is_weekend,
                COUNT(*) as trip_count,
                AVG(fare_amount) as avg_fare,
                AVG(tip_amount) as avg_tip,
                AVG(trip_distance) as avg_distance,
                AVG(trip_duration_minutes) as avg_duration
            FROM taxi_trips_processed
            GROUP BY is_weekend
            """
            
            weekend_data = self.postgres_manager.fetch_data(weekend_query)
            
            # 3. Hourly patterns
            hourly_query = """
            SELECT 
                hour_of_day,
                COUNT(*) as trip_count,
                AVG(fare_amount) as avg_fare,
                AVG(trip_distance) as avg_distance
            FROM taxi_trips_processed
            GROUP BY hour_of_day
            ORDER BY hour_of_day
            """
            
            hourly_data = self.postgres_manager.fetch_data(hourly_query)
            
            # 4. Distance category analysis
            distance_query = """
            SELECT 
                distance_category,
                COUNT(*) as trip_count,
                AVG(fare_amount) as avg_fare,
                AVG(fare_per_mile) as avg_fare_per_mile
            FROM taxi_trips_processed
            GROUP BY distance_category
            ORDER BY 
                CASE distance_category
                    WHEN 'short' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'long' THEN 3
                    WHEN 'very_long' THEN 4
                END
            """
            
            distance_data = self.postgres_manager.fetch_data(distance_query)
            
            # Save all reports to S3
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            reports = {
                'daily_analytics': daily_data,
                'weekend_vs_weekday': weekend_data,
                'hourly_patterns': hourly_data,
                'distance_categories': distance_data
            }
            
            for report_name, data in reports.items():
                if data is not None:
                    report_key = f"reports/{report_name}_{timestamp}.csv"
                    if self.s3_manager.upload_dataframe(data, report_key, format='csv'):
                        logger.info(f"{report_name} report saved to S3: {report_key}")
            
            # Generate summary statistics
            summary_stats = {
                'pipeline_execution_date': datetime.now().isoformat(),
                'total_daily_records': len(daily_data) if daily_data is not None else 0,
                'total_processed_trips': daily_data['total_trips'].sum() if daily_data is not None else 0,
                'total_revenue': daily_data['total_revenue'].sum() if daily_data is not None else 0,
                'avg_trip_distance': daily_data['avg_trip_distance'].mean() if daily_data is not None else 0,
                'avg_fare_amount': daily_data['avg_fare_amount'].mean() if daily_data is not None else 0
            }
            
            summary_df = pd.DataFrame([summary_stats])
            summary_key = f"reports/pipeline_summary_{timestamp}.csv"
            if self.s3_manager.upload_dataframe(summary_df, summary_key, format='csv'):
                logger.info(f"Pipeline summary saved to S3: {summary_key}")
            
            logger.info("Comprehensive analytics report generation completed!")
            logger.info(f"Summary: {summary_stats}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate comprehensive report: {e}")
            return False

def main():
    """Main execution function"""
    logger.info("=== NYC Taxi Data Processing Pipeline Started ===")
    
    pipeline = NYCTaxiDataPipeline()
    
    # Setup database
    logger.info("Step 1: Setting up database...")
    if not pipeline.setup_database():
        logger.error("Failed to setup database")
        sys.exit(1)
    
    # Run data processing pipeline
    logger.info("Step 2: Running data processing pipeline...")
    if not pipeline.download_and_process_data():
        logger.error("Pipeline execution failed")
        sys.exit(1)
    
    # Generate comprehensive analytics report
    logger.info("Step 3: Generating comprehensive analytics report...")
    if not pipeline.generate_comprehensive_report():
        logger.error("Failed to generate analytics report")
        sys.exit(1)
    
    logger.info("=== All pipeline tasks completed successfully! ===")
    logger.info("Check the following locations for results:")
    logger.info("- PostgreSQL: daily_trip_stats, taxi_trips_processed tables")
    logger.info("- HDFS: /user/data/taxi/processed/")
    logger.info("- S3: analytics/, reports/, quality/ folders")

if __name__ == "__main__":
    main()