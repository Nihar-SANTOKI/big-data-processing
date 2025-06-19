import great_expectations as gx
from great_expectations.core import ExpectationSuite
from pyspark.sql import DataFrame
from typing import Dict, Any, List
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class DataValidator:
    def __init__(self):
        self.context = gx.get_context()
        
    def validate_taxi_data(self, df: DataFrame) -> Dict[str, Any]:
        """Validate taxi trip data using Great Expectations"""
        logger.info("Starting data validation...")
        
        # Convert Spark DataFrame to Pandas for Great Expectations
        pandas_df = df.toPandas()
        
        # Create expectation suite
        suite = self.context.create_expectation_suite(
            expectation_suite_name="taxi_data_validation",
            overwrite_existing=True
        )
        
        # Add expectations
        suite.expect_column_values_to_not_be_null("fare_amount")
        suite.expect_column_values_to_be_between("fare_amount", min_value=0, max_value=500)
        suite.expect_column_values_to_be_between("trip_distance", min_value=0, max_value=100)
        suite.expect_column_values_to_be_between("passenger_count", min_value=1, max_value=6)
        suite.expect_column_values_to_not_be_null("tpep_pickup_datetime")
        suite.expect_column_values_to_not_be_null("tpep_dropoff_datetime")
        
        # Create validator
        validator = self.context.get_validator(
            batch_request=self.context.sources.add_pandas("taxi_data").add_asset("trips").build_batch_request(dataframe=pandas_df),
            expectation_suite=suite
        )
        
        # Run validation
        results = validator.validate()
        
        validation_summary = {
            "success": results.success,
            "total_expectations": len(results.results),
            "successful_expectations": sum(1 for r in results.results if r.success),
            "failed_expectations": sum(1 for r in results.results if not r.success),
            "statistics": results.statistics
        }
        
        logger.info(f"Data validation completed: {validation_summary}")
        return validation_summary
    
    def validate_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """Basic data quality checks"""
        logger.info("Running data quality checks...")
        
        total_rows = df.count()
        
        quality_metrics = {
            "total_rows": total_rows,
            "null_counts": {},
            "duplicate_count": 0,
            "data_types": {}
        }
        
        # Check for nulls in each column
        for col_name in df.columns:
            null_count = df.filter(df[col_name].isNull()).count()
            quality_metrics["null_counts"][col_name] = null_count
        
        # Check for duplicates
        distinct_rows = df.distinct().count()
        quality_metrics["duplicate_count"] = total_rows - distinct_rows
        
        # Data types
        for col_name, col_type in df.dtypes:
            quality_metrics["data_types"][col_name] = col_type