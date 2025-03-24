import functools
import logging
import sys
from uvicorn.logging import ColourizedFormatter
from typing import Any, Callable

# Custom colorized formatter to apply colors specifically to log levels
class CustomColourizedFormatter(ColourizedFormatter):
    def format(self, record: logging.LogRecord) -> str:
        level_color_map = {
            "DEBUG": "\033[34m",    # Blue
            "INFO": "\033[32m",     # Green
            "WARNING": "\033[33m",  # Yellow
            "ERROR": "\033[31m",    # Red
            "CRITICAL": "\033[41m", # Red background
        }

        reset = "\033[0m"

        record.levelname = f"{level_color_map.get(record.levelname, '')}{record.levelname}{reset}"

        return super().format(record)

def get_logger(name: str) -> logging.Logger:
    """Creates a logger object

    Args:
        name (str): name given to the logger

    Returns:
        logging.Logger: logger object to be used for logging 
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.hasHandlers():
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)

        formatter = CustomColourizedFormatter(
            "{asctime} | {levelname:<8} | {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M:%S",
            use_colors=True
        )

        ch.setFormatter(formatter)

        logger.addHandler(ch)

    return logger

def log_function_call_debug(logger: logging.Logger) -> Callable:
    """A decorator that logs the function calls and results.

    Args:
        logger (logging.Logger): The logger instance to use for logging.
    
    Returns:
        Callable: A wrapper function that logs the execution details.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger.debug(f"Calling {func.__name__} with args: {args} and kwargs: {kwargs}")
            result = func(*args, **kwargs)
            logger.debug(f"{func.__name__} returned {result}")
            return result
        return wrapper
    return decorator
