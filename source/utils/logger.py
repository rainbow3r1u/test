"""
统一日志工具
"""
import logging
import sys
from pathlib import Path
from datetime import datetime


_loggers = {}


def setup_logger(name: str = None, level: int = logging.INFO, log_file: str = None) -> logging.Logger:
    if name in _loggers:
        return _loggers[name]
    
    logger = logging.getLogger(name or 'crypto_scanner')
    logger.setLevel(level)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    _loggers[name] = logger
    return logger


def get_logger(name: str = None) -> logging.Logger:
    return _loggers.get(name) or setup_logger(name)
