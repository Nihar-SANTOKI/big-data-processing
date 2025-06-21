# src/storage/local_file_manager.py
import os
import pandas as pd
from typing import Optional
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class LocalFileManager:
    """Alternative to S3Manager for local file storage"""
    
    def __init__(self, base_path: str = "local_storage"):
        self.base_path = base_path
        self.enabled = True
        
        # Create base directory
        os.makedirs(base_path, exist_ok=True)
        os.makedirs(f"{base_path}/analytics", exist_ok=True)
        os.makedirs(f"{base_path}/reports", exist_ok=True)
        os.makedirs(f"{base_path}/quality", exist_ok=True)
        
        logger.info(f"Local file storage initialized at: {os.path.abspath(base_path)}")
    
    def upload_file(self, local_file_path: str, key: str) -> bool:
        """Copy file to local storage directory"""
        try:
            import shutil
            dest_path = os.path.join(self.base_path, key)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.copy2(local_file_path, dest_path)
            logger.info(f"File copied to: {dest_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to copy file: {e}")
            return False
    
    def upload_dataframe(self, df: pd.DataFrame, key: str, format: str = 'parquet') -> bool:
        """Save DataFrame to local storage"""
        try:
            file_path = os.path.join(self.base_path, key)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            if format.lower() == 'parquet':
                df.to_parquet(file_path, index=False)
            elif format.lower() == 'csv':
                df.to_csv(file_path, index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"DataFrame saved to: {os.path.abspath(file_path)}")
            return True
        except Exception as e:
            logger.error(f"Failed to save DataFrame: {e}")
            return False
    
    def download_file(self, key: str, local_file_path: str) -> bool:
        """Copy file from local storage"""
        try:
            import shutil
            source_path = os.path.join(self.base_path, key)
            shutil.copy2(source_path, local_file_path)
            logger.info(f"File copied from: {source_path} to: {local_file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to copy file: {e}")
            return False
    
    def list_objects(self, prefix: str = '') -> list:
        """List files in local storage with given prefix"""
        try:
            files = []
            search_path = os.path.join(self.base_path, prefix)
            
            if os.path.exists(search_path):
                for root, dirs, filenames in os.walk(search_path):
                    for filename in filenames:
                        file_path = os.path.join(root, filename)
                        # Return relative path from base_path
                        rel_path = os.path.relpath(file_path, self.base_path)
                        files.append(rel_path.replace(os.sep, '/'))  # Use forward slashes
            
            return files
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []