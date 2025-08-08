# logging_config.py
import logging
from logging.handlers import RotatingFileHandler
import os


def configure_logging(
    log_directory: str = '/app/logs',
    log_file: str = 'sec_filings.log',
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    level: int = logging.INFO,
    console_output: bool = True,
) -> None:
    """Configure logging for the application.

    Args:
        log_file: Path to the log file
        max_bytes: Maximum size of log file before rotation
        backup_count: Number of backup logs to keep
        level: Logging level
        console_output: Whether to output to console
    """
    os.makedirs(log_directory, exist_ok=True)
    log_path = os.path.join(log_directory, log_file)

    # Create a custom formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(level)

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler if enabled
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # Suppress overly verbose logs from libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('psycopg2').setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
