import logging

def setup_logger(log_level: str):
    mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARN,
        "error": logging.ERROR,
    }
    lv = logging.ERROR
    if log_level in mapping:
        lv = mapping[log_level]
    logging.getLogger().setLevel(lv)
