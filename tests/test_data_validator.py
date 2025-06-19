import pytest
from unittest.mock import Mock, patch
import pandas as pd
from src.data_processing.data_validator import DataValidator

class TestDataValidator:
    @pytest.fixture
    def data_validator(self):
        with patch('src.data_processing.data_validator.gx.get_context'):
            return DataValidator()
    
    def test_validate_data_quality(self, data_validator):
        # Mock Spark DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.columns = ['fare_amount', 'trip_distance']
        mock_df.filter.return_value.count.return_value = 0
        mock_df.distinct.return_value.count.return_value = 1000
        mock_df.dtypes = [('fare_amount', 'double'), ('trip_distance', 'double')]
        
        result = data_validator.validate_data_quality(mock_df)
        
        assert result['total_rows'] == 1000
        assert 'null_counts' in result
        assert 'duplicate_count' in result