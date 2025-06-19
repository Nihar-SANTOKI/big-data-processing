import os
import sys
from datetime import datetime
from src.config.settings import settings
from src.utils.logger import setup_logger
from src.data_processing.spark_processor import SparkProcessor
from src.data_processing.data_validator import DataValidator
from src.storage.s3_manager import S3Manager
from src.storage.postgres_manager import PostgreSQLManager
from pyspark.sql.functions import col
import pandas as pd
from datetime import datetime, timedelta

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
        """Main data processing pipeline with enhanced error handling"""
        try:
            logger.info("Starting NYC Taxi Data Processing Pipeline...")
            
            # Step 1: Read data with retry logic
            max_retries = 3
            df = None
            for attempt in range(max_retries):
                try:
                    logger.info(f"Attempt {attempt + 1}: Reading data from source...")
                    df = self.spark_processor.read_parquet_from_url(settings.data.source_url)
                    if df is not None:
                        break
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        logger.error("All retry attempts failed")
                        return False
            
            if df is None:
                logger.error("Failed to read source data after retries")
                return False
            
            # Add data size monitoring
            initial_count = df.count()
            logger.info(f"Initial dataset size: {initial_count:,} records")
            
            # Continue with processing...
            # [rest of your pipeline code]
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
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