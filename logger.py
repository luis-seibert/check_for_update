import logging
import time
import os


class Logger:
    """Logger class for logging messages."""

    def __init__(self):
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            level=logging.INFO,
        )
        logging.getLogger("selenium").setLevel(logging.WARNING)

    def log_error(self, error_message: str) -> None:
        """Log error messages."""

        logging.error(error_message)

    def log_exception(self, exception_message: str) -> None:
        """Log exception messages."""

        logging.exception(exception_message)

    def log_info(self, info_message: str) -> None:
        """Log info messages."""

        logging.info(info_message)

    def log_warning(self, warning_message: str) -> None:
        """Log warning messages."""

        logging.warning(warning_message)

    def log_last_new_appartment(self, scanned_flats_csv: str) -> None:
        """Log the time since the last update of listings."""

        current_time = time.time()

        if os.path.exists(scanned_flats_csv):
            elapsed_time_since_last_fetch = current_time - time.mktime(
                time.localtime(os.path.getmtime(scanned_flats_csv))
            )
        else:
            elapsed_time_since_last_fetch = 0

        logging.info(
            "Time since last update: %d minutes",
            round(elapsed_time_since_last_fetch / 60),
        )
