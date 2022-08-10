import logging
from pathlib import Path


class Logger:
    def __init__(self, filename: str, level=logging.INFO):
        logger = logging.getLogger(__name__)
        logger.setLevel(level)
        root_dir = Path().cwd().parent
        logs_dir = root_dir / "logs"

        if not logs_dir.exists():
            logs_dir.mkdir()

        file_handler = logging.FileHandler(root_dir / f"logs/{filename}.log")
        formatter = logging.Formatter(
            "[%(asctime)s] : %(levelname)s : {%(filename)s : %(lineno)d} - %(message)s",
            "%m-%d-%Y %H:%M:%S",
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        self.logger = logger

    def get_app_logger(self) -> logging.Logger:
        return self.logger
