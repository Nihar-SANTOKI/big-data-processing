import time
import psutil
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class PipelineMonitor:
    def __init__(self):
        self.start_time = time.time()
        
    def log_system_metrics(self):
        """Log system resource usage"""
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        
        logger.info(f"CPU Usage: {cpu_percent}%")
        logger.info(f"Memory Usage: {memory.percent}%")
        logger.info(f"Available Memory: {memory.available / (1024**3):.2f} GB")