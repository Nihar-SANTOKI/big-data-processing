import pytest
from src.main import NYCTaxiDataPipeline
from unittest.mock import patch, Mock

class TestIntegration:
    def test_full_pipeline_execution(self):
        """Test the complete pipeline execution"""
        with patch('src.main.SparkProcessor') as mock_spark:
            pipeline = NYCTaxiDataPipeline()
            # Mock successful pipeline execution
            mock_spark.return_value.read_parquet_from_url.return_value = Mock()
            
            result = pipeline.download_and_process_data()
            assert result is True