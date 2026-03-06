"""
统一日志配置模块 — 基于 uvicorn 的 logging 框架。

使用方式：
    from common.logger import setup_logging
    setup_logging()          # 默认 INFO
    setup_logging("DEBUG")   # 指定级别

所有模块继续使用 logging.getLogger(__name__) 即可，
日志格式和级别由此处统一控制。
"""

import logging
import logging.handlers
import sys
from pathlib import Path

# 日志目录
LOG_DIR = Path(__file__).parent.parent / "log"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = str(LOG_DIR / "app.log")
ERROR_LOG_FILE = str(LOG_DIR / "error.log")

# 日志格式：与 uvicorn 风格保持一致
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# uvicorn 内部使用的 logger 名称
UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def setup_logging(level: str = "INFO") -> None:
    """配置项目全局日志，同时统一 uvicorn 的日志格式。"""
    log_level = getattr(logging, level.upper(), logging.INFO)

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

    # 控制台 handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # 文件 handler — 普通日志输出到 app.log
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    # 错误日志 handler — ERROR 及以上级别输出到 error.log
    error_file_handler = logging.handlers.RotatingFileHandler(
        ERROR_LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    error_file_handler.setFormatter(formatter)
    error_file_handler.setLevel(logging.ERROR)

    handlers = [console_handler, file_handler, error_file_handler]

    # 配置 root logger
    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers = handlers

    # 统一 uvicorn 自身 logger 的格式和级别
    for name in UVICORN_LOGGERS:
        uv_logger = logging.getLogger(name)
        uv_logger.setLevel(log_level)
        uv_logger.handlers = handlers
        uv_logger.propagate = False


# 供 uvicorn.run(log_config=...) 使用的 dict config
UVICORN_LOG_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": LOG_FORMAT,
            "datefmt": DATE_FORMAT,
        },
        "access": {
            "format": '%(asctime)s | %(levelname)-8s | %(name)s - %(client_addr)s - "%(request_line)s" %(status_code)s',
            "datefmt": DATE_FORMAT,
        },
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
        "access": {
            "class": "logging.StreamHandler",
            "formatter": "access",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": LOG_FILE,
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 5,
            "encoding": "utf-8",
        },
        "error_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": ERROR_LOG_FILE,
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 5,
            "encoding": "utf-8",
            "level": "ERROR",
        },
        "access_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "access",
            "filename": LOG_FILE,
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 5,
            "encoding": "utf-8",
        },
    },
    "loggers": {
        "uvicorn": {
            "handlers": ["default", "file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.error": {
            "handlers": ["default", "file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["access", "access_file"],
            "level": "INFO",
            "propagate": False,
        },
    },
    "root": {
        "handlers": ["default", "file", "error_file"],
        "level": "INFO",
    },
}
