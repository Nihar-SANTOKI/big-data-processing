# In src/data_processing/data_validator.py
import great_expectations as gx
from great_expectations.core import ExpectationSuite
from pyspark.sql import DataFrame
from typing import Dict, Any, List
import pandas as pd
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class DataValidator:
    def __init__(self):
        # Use new GX API
        self.context = gx.get_context()
        
    def validate_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """Enhanced data quality checks"""
        logger.info("Running comprehensive data quality checks...")
        
        total_rows = df.count()
        
        quality_metrics = {
            "total_rows": total_rows,
            "null_counts": {},
            "duplicate_count": 0,
            "data_types": {},
            "outliers": {},
            "data_completeness": {}
        }
        
        # Check for nulls in each column
        for col_name in df.columns:
            null_count = df.filter(df[col_name].isNull()).count()
            quality_metrics["null_counts"][col_name] = null_count
            quality_metrics["data_completeness"][col_name] = (total_rows - null_count) / total_rows * 100
        
        # Check for duplicates
        distinct_rows = df.distinct().count()
        quality_metrics["duplicate_count"] = total_rows - distinct_rows
        
        # Data types
        for col_name, col_type in df.dtypes:
            quality_metrics["data_types"][col_name] = col_type
        
        # Business logic validations
        invalid_fares = df.filter((col("fare_amount") < 0) | (col("fare_amount") > 1000)).count()
        invalid_distances = df.filter((col("trip_distance") < 0) | (col("trip_distance") > 200)).count()
        
        quality_metrics["business_rule_violations"] = {
            "invalid_fares": invalid_fares,
            "invalid_distances": invalid_distances
        }
        
        logger.info(f"Data quality assessment completed: {quality_metrics}")
        return quality_metrics