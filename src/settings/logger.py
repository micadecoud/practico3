from datetime import datetime
from logging import Logger, Formatter, StreamHandler, FileHandler, DEBUG
from logging import getLogger
import os


def custom_logger(logger_name: str) -> Logger:
    """
    Function for returning a Logger object with specified settings

    Args:
        logger_name (str): The name of the logger

    Returns:
        Logger: A Logger object with specified settings
    """

    # Create logs directory if it doesn't exist
    os.makedirs("data/logs", exist_ok=True)

    logger = getLogger(f"{logger_name} - ")
    logger.setLevel(DEBUG)

    if not logger.hasHandlers():
        # Console handler (existing)
        console_handler = StreamHandler()
        console_handler.setLevel(DEBUG)
        console_formatter = Formatter(
            "%(asctime)s.%(msecs)03d - %(name)s%(levelname)s: %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S",
        )
        console_handler.setFormatter(console_formatter)

        # File handler (new)
        log_file = f"data/logs/{datetime.now().strftime('%Y-%m-%d')}.log"
        file_handler = FileHandler(log_file)
        file_handler.setLevel(DEBUG)
        file_formatter = Formatter(
            "%(asctime)s.%(msecs)03d - %(name)s%(levelname)s: %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S",
        )
        file_handler.setFormatter(file_formatter)

        # Add both handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger
