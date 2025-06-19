import requests
import os
from src.config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def download_nyc_taxi_data():
    """Download NYC taxi data"""
    
    # Create data directory
    os.makedirs("data", exist_ok=True)
    
    # URLs for multiple months of data
    data_urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet"
    ]
    
    for url in data_urls:
        filename = url.split("/")[-1]
        filepath = f"data/{filename}"
        
        if os.path.exists(filepath):
            logger.info(f"File already exists: {filepath}")
            continue
        
        logger.info(f"Downloading {url}...")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded: {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")

if __name__ == "__main__":
    download_nyc_taxi_data()