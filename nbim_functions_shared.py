"""

** nbim_functions_shared.py **

Shared logging helpers for the NBIM data harvesting scripts.

Provides:
    - setup_logging(log_name, log_file)     Set up console and file handlers
    - configure_file_logging(level)         Attach file handler at the appropriate level
    - log_important(msg)                    Write to console and always to file, even in STRICT mode

Usage:
    from nbim_functions_shared import setup_logging, configure_file_logging, log_important

    log = setup_logging("my_script", "my_script.log")
    configure_file_logging(args.log)
    log_important("=== Script started ===")

"""

import logging
import sys


def setup_logging(log_name: str, log_file: str):
    """
    Create and return a logger with a console handler attached.
    The file handler is created but not attached until configure_file_logging() is called.
    Returns the logger instance.
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Attach file_handler to logger so configure_file_logging and log_important can find it
    logger._file_handler = file_handler

    return logger


def configure_file_logging(logger, level: str) -> None:
    """Attach and configure the file handler based on --log argument."""
    file_handler = logger._file_handler
    if level == "OFF":
        return
    if level == "STRICT":
        file_handler.setLevel(logging.ERROR)
    else:  # FULL
        file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)


def log_important(logger, msg: str) -> None:
    """Log a message at INFO to console, and always write to file regardless of STRICT level."""
    logger.info(msg)
    file_handler = logger._file_handler
    if file_handler in logger.handlers and file_handler.level > logging.INFO:
        record = logger.makeRecord(logger.name, logging.INFO, "", 0, msg, (), None)
        file_handler.emit(record)
