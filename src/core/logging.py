# src/core/logging.py
import logging
from datetime import datetime, timezone, timedelta
import os

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

def setup_logging(level: str = 'INFO', log_file: str = 'logs/application.log'):
    """
    Sets up global logging configuration.
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')

    logging.basicConfig(
        level=numeric_level,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def log_with_timestamp(message: str, name: str = "Pipeline", level: str = "info"):
    """Log message with Tehran timestamp"""
    # Tehran is UTC+3:30
    tehran_tz = timezone(timedelta(hours=3, minutes=30))
    timestamp = datetime.now(tehran_tz).strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {name}: {message}"

    if level.lower() == "info":
        logging.info(log_message)
    elif level.lower() == "warning":
        logging.warning(log_message)
    elif level.lower() == "error":
        logging.error(log_message)
    elif level.lower() == "debug":
        logging.debug(log_message)
    else:
        logging.info(log_message) # Default to info