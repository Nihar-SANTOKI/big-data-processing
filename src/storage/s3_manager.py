import boto3
import pandas as pd
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError
from src.config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class S3Manager:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws.access_key_id,
            aws_secret_access_key=settings.aws.secret_access_key,
            region_name=settings.aws.region
        )
        self.bucket_name = settings.aws.s3_bucket
        
    def upload_file(self, local_file_path: str, s3_key: str) -> bool:
        """Upload a file to S3"""
        try:
            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key)
            logger.info(f"Successfully uploaded {local_file_path} to s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {e}")
            return False
    
    # IN s3_manager.py - Fix buffer handling
    def upload_dataframe(self, df: pd.DataFrame, s3_key: str, format: str = 'parquet') -> bool:
        try:
            import io
            
            if format.lower() == 'parquet':
                # Fix: Use BytesIO buffer
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)
                body = buffer.getvalue()
            elif format.lower() == 'csv':
                body = df.to_csv(index=False).encode('utf-8')
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=body
            )
            return True
        except Exception as e:
            logger.error(f"Failed to upload DataFrame to S3: {e}")
            return False
    
    def download_file(self, s3_key: str, local_file_path: str) -> bool:
        """Download a file from S3"""
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_file_path)
            logger.info(f"Successfully downloaded s3://{self.bucket_name}/{s3_key} to {local_file_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download file from S3: {e}")
            return False
    
    def list_objects(self, prefix: str = '') -> list:
        """List objects in S3 bucket with given prefix"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return []