import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from src.data_processing.spark_processor import SparkProcessor

class TestSparkProcessor:
    @pytest.fixture
    def spark_processor(self):
        with patch('src.data_processing.spark_processor.SparkSession.builder') as mock_builder:
            mock_spark = Mock()
            mock_builder.appName.return_value.master.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
            processor = SparkProcessor()
            yield processor
            
    def test_spark_session_creation(self, spark_processor):
        assert spark_processor.spark is not None
        
    def test_clean_taxi_data(self, spark_processor):
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value.collect.return_value = [{"q1": 5.0, "q3": 15.0}]
        
        result = spark_processor.clean_taxi_data(mock_df)
        assert result is not None
