import logging
import json
from typing import Any

from datetime import datetime

def load_config(config_file: Any="config.json")-> Any:
    """
    Load the configuration from a JSON file.
    """
    with open(config_file, "r") as file:
        return json.load(file)
    
config = load_config()

def setup_logger() -> logging.Logger:
    logging_config = config.get("logging", {})

    log_level = logging_config.get("log_level", "INFO").upper()  # Default to INFO if not specified
    log_format = logging_config.get("log_format", "%(asctime)s - %(levelname)s - %(message)s")
    log_file = logging_config.get("log_file", "default.log")  # Default to app.log if not specified

    logging.basicConfig(
        format=log_format,
        level=log_level,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Logs to console as well
        ]
    )

    logger = logging.getLogger(__name__)
    logger.debug("Logger configured successfully with level: %s", log_level)
    return logger

logger = setup_logger()

def safe_float(value: Any) -> float:
    if isinstance(value, (int, float, str)) and value != "":
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0  # Default fallback


def convert_millis_to_datetime(ms: int) -> datetime:
    """
    Convert a Unix timestamp in milliseconds to a Python datetime object.

    Args:
        ms (int): Unix timestamp in milliseconds since January 1, 1970 (UTC).

    Returns:
        datetime: A Python datetime object representing the converted timestamp.

    Raises:
        ValueError: If the input `ms` is not a valid integer or out of range.

    Example:
        >>> timestamp_ms = 1734394653982
        >>> convert_millis_to_datetime(timestamp_ms)
        datetime.datetime(2024, 12, 17, 1, 10, 53)
    """
    try:
        return datetime.fromtimestamp(ms / 1000.0)
    except (TypeError, ValueError, OverflowError) as e:
        raise ValueError(f"Invalid milliseconds timestamp {ms}: {e}")
      