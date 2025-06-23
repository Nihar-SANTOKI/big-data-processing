import os
from dotenv import load_dotenv
from dataclasses import dataclass

load_dotenv()

@dataclass
class PostgreSQLConfig:
    host: str = os.getenv('POSTGRES_HOST', '')
    port: int = int(os.getenv('POSTGRES_PORT', 5432))
    database: str = os.getenv('POSTGRES_DB', '')
    username: str = os.getenv('POSTGRES_USER', '')
    password: str = os.getenv('POSTGRES_PASSWORD', '')
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class SparkConfig:
    master_url: str = os.getenv('SPARK_MASTER_URL', 'local[*]')
    app_name: str = os.getenv('SPARK_APP_NAME', 'NYCTaxiProcessor')
    max_result_size: str = '4g'
    driver_memory: str = os.getenv('SPARK_DRIVER_MEMORY', '2g')
    executor_memory: str = os.getenv('SPARK_EXECUTOR_MEMORY', '2g')
    executor_cores: str = '2'
    sql_shuffle_partitions: str = '200'

@dataclass
class DataConfig:
    source_url: str = os.getenv('DATA_SOURCE_URL', '')
    hdfs_base_path: str = os.getenv('HDFS_BASE_PATH', '/user/data/taxi')
    batch_size: int = int(os.getenv('BATCH_SIZE', 100000))

@dataclass
class Settings:
    def __init__(self):
        self.postgres = PostgreSQLConfig()
        self.spark = SparkConfig()
        self.data = DataConfig()
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.log_file = os.getenv('LOG_FILE', 'logs/app.log')
        
        # Add validation
        self._validate_config()
    
    def _validate_config(self):
        """Validate critical configuration"""
        if not self.postgres.host:
            raise ValueError("POSTGRES_HOST must be set")

settings = Settings()