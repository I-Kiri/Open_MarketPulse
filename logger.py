# logger.py
"""
body for logger implementation
"""

import os
import logging
from logging.handlers import RotatingFileHandler


# ── Functions ─────────────────────────────────────────────────────────────────────────────────────────────────────────
def create_logger(
        tlogging_time,
        tlogging_dir
) -> (logging.getLogger(), str):
    """Creating process logger"""
    # make dir if doesn't exist
    os.makedirs(tlogging_dir, exist_ok=True)

    logger_file_path = f"{tlogging_dir}/ttc_pulse_{tlogging_time.replace(' ', '_').replace(':', '-')}.log"
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # file handler
    file_handler = RotatingFileHandler(
        logger_file_path
    )
    file_handler.setFormatter(formatter)

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # logger
    tlogger = logging.getLogger()
    tlogger.setLevel(logging.INFO)

    tlogger.addHandler(file_handler)
    tlogger.addHandler(console_handler)

    return tlogger, logger_file_path


# ── Main ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pass
