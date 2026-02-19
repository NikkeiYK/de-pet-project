import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Создает логгер с единым форматом.
    В Airflow эти логи попадут в stdout/stderr задачи и будут видны в UI.
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
