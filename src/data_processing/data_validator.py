import great_expectations as gx
from great_expectations.data_context import get_context, FileDataContext
from great_expectations.core import ExpectationSuite
from pyspark.sql import DataFrame
from typing import Dict, Any, List
import pandas as pd
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class DataValidator:
    def __init__(self):
        # Use new GX API
        try:
            self.context = gx.get_context()
        except:
            self.context = None
            logger.warning("Great Expectations context not available")
    
    def create_expectation_suite(self, df, suite_name="taxi_data_suite"):
        """Create expectations for taxi data"""
        if not self.context:
            logger.warning("GX context not available, skipping expectation suite creation")
            return None
            
        suite = self.context.add_expectation_suite(expectation_suite_name=suite_name)
        
        # Add specific expectations (removed datetime expectations)
        suite.expect_column_values_to_not_be_null("fare_amount")
        suite.expect_column_values_to_be_between("fare_amount", min_value=0, max_value=500)
        suite.expect_column_values_to_be_between("trip_distance", min_value=0, max_value=100)
        suite.expect_column_values_to_be_between("passenger_count", min_value=1, max_value=6)
        
        return suite
    
    def validate_data_quality(self, df) -> Dict[str, Any]:
        """Enhanced data quality checks - works with both Spark and Pandas (no datetime validation)"""
        logger.info("Running comprehensive data quality checks...")
        
        # Check if this is a PandasDataFrameWrapper (our fallback class)
        if hasattr(df, '_df') and hasattr(df, 'toPandas'):
            return self._validate_pandas_quality(df._df)
        # Check if this is a regular pandas DataFrame
        elif isinstance(df, pd.DataFrame):
            return self._validate_pandas_quality(df)
        # Otherwise assume it's a Spark DataFrame
        else:
            return self._validate_spark_quality(df)
    
    def _validate_pandas_quality(self, pandas_df: pd.DataFrame) -> Dict[str, Any]:
        """Data quality validation using pandas operations (no datetime checks)"""
        logger.info("Running pandas-based data quality checks...")
        
        total_rows = len(pandas_df)
        
        quality_metrics = {
            "total_rows": total_rows,
            "null_counts": {},
            "duplicate_count": 0,
            "data_types": {},
            "outliers": {},
            "data_completeness": {},
            "business_rule_violations": {}
        }
        
        # Check for nulls in each column
        for col_name in pandas_df.columns:
            null_count = pandas_df[col_name].isnull().sum()
            quality_metrics["null_counts"][col_name] = int(null_count)
            quality_metrics["data_completeness"][col_name] = (total_rows - null_count) / total_rows * 100
        
        # Check for duplicates
        distinct_rows = len(pandas_df.drop_duplicates())
        quality_metrics["duplicate_count"] = total_rows - distinct_rows
        
        # Data types
        for col_name, col_type in pandas_df.dtypes.items():
            quality_metrics["data_types"][col_name] = str(col_type)
        
        # Business logic validations (removed datetime validations)
        invalid_fares = len(pandas_df[(pandas_df['fare_amount'] < 0) | (pandas_df['fare_amount'] > 1000)])
        invalid_distances = len(pandas_df[(pandas_df['trip_distance'] < 0) | (pandas_df['trip_distance'] > 200)])
        invalid_passengers = 0
        
        if 'passenger_count' in pandas_df.columns:
            invalid_passengers = len(pandas_df[(pandas_df['passenger_count'] < 1) | (pandas_df['passenger_count'] > 6)])
        
        quality_metrics["business_rule_violations"] = {
            "invalid_fares": invalid_fares,
            "invalid_distances": invalid_distances,
            "invalid_passengers": invalid_passengers
        }
        
        # Calculate data quality score
        total_violations = sum(quality_metrics["business_rule_violations"].values())
        quality_score = max(0, (total_rows - total_violations) / total_rows * 100)
        quality_metrics["overall_quality_score"] = round(quality_score, 2)
        
        logger.info(f"Data quality assessment completed: {quality_metrics}")
        return quality_metrics
    
    def _validate_spark_quality(self, df: DataFrame) -> Dict[str, Any]:
        """Data quality validation using Spark operations (no datetime checks)"""
        # Import PySpark functions here to ensure Spark context is active
        from pyspark.sql.functions import col, count, when, isnan, isnull
        
        logger.info("Running Spark-based data quality checks...")
        
        total_rows = df.count()
        
        quality_metrics = {
            "total_rows": total_rows,
            "null_counts": {},
            "duplicate_count": 0,
            "data_types": {},
            "outliers": {},
            "data_completeness": {},
            "business_rule_violations": {}
        }
        
        # Check for nulls in each column
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            quality_metrics["null_counts"][col_name] = null_count
            quality_metrics["data_completeness"][col_name] = (total_rows - null_count) / total_rows * 100
        
        # Check for duplicates
        distinct_rows = df.distinct().count()
        quality_metrics["duplicate_count"] = total_rows - distinct_rows
        
        # Data types
        for col_name, col_type in df.dtypes:
            quality_metrics["data_types"][col_name] = col_type
        
        # Business logic validations (removed datetime validations)
        invalid_fares = df.filter((col("fare_amount") < 0) | (col("fare_amount") > 1000)).count()
        invalid_distances = df.filter((col("trip_distance") < 0) | (col("trip_distance") > 200)).count()
        invalid_passengers = 0
        
        if 'passenger_count' in df.columns:
            invalid_passengers = df.filter((col("passenger_count") < 1) | (col("passenger_count") > 6)).count()
        
        quality_metrics["business_rule_violations"] = {
            "invalid_fares": invalid_fares,
            "invalid_distances": invalid_distances,
            "invalid_passengers": invalid_passengers
        }
        
        # Calculate data quality score
        total_violations = sum(quality_metrics["business_rule_violations"].values())
        quality_score = max(0, (total_rows - total_violations) / total_rows * 100)
        quality_metrics["overall_quality_score"] = round(quality_score, 2)
        
        logger.info(f"Data quality assessment completed: {quality_metrics}")
        return quality_metrics
    
    def validate_taxi_data_schema(self, df) -> bool:
        """Validate that the DataFrame has expected taxi data schema (removed datetime columns)"""
        expected_columns = {
            'VendorID', 'passenger_count', 'trip_distance', 'fare_amount', 'tip_amount', 'total_amount'
        }
        
        # Handle both Spark and Pandas DataFrames
        if hasattr(df, '_df'):  # PandasDataFrameWrapper
            actual_columns = set(df._df.columns)
        elif isinstance(df, pd.DataFrame):  # Regular pandas DataFrame
            actual_columns = set(df.columns)
        else:  # Spark DataFrame
            actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        
        if missing_columns:
            logger.warning(f"Missing expected columns (may be optional): {missing_columns}")
            # Check for critical columns only
            critical_columns = {'fare_amount', 'trip_distance'}
            critical_missing = critical_columns - actual_columns
            
            if critical_missing:
                logger.error(f"Missing critical columns: {critical_missing}")
                return False
        
        logger.info("Schema validation passed")
        return True