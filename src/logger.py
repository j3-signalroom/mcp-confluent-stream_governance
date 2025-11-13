"""
Logging configuration for MCP Confluent Server.
Provides structured logging with JSON output and Kafka logger integration.
"""

import logging
import sys
import json
import os
from typing import Dict, Any, Optional, Literal
from datetime import datetime, timezone

# Type alias for log levels
LogLevel = Literal["fatal", "error", "warn", "info", "debug", "trace"]

# Log level mappings
LOG_LEVELS = {
    "fatal": logging.CRITICAL,
    "error": logging.ERROR,
    "warn": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "trace": logging.DEBUG,  # Python logging doesn't have TRACE, use DEBUG
}

# Kafka log level constants (from librdkafka)
KAFKA_LOG_NOTHING = 0
KAFKA_LOG_EMERG = 0
KAFKA_LOG_ALERT = 1
KAFKA_LOG_CRIT = 2
KAFKA_LOG_ERR = 3
KAFKA_LOG_WARNING = 4
KAFKA_LOG_NOTICE = 5
KAFKA_LOG_INFO = 6
KAFKA_LOG_DEBUG = 7

# Map Kafka log levels to Python levels (similar to TypeScript's levelMap)
KAFKA_LEVEL_MAP: Dict[int, int] = {
    KAFKA_LOG_NOTHING: logging.DEBUG,   # TRACE equivalent
    KAFKA_LOG_EMERG: logging.CRITICAL,
    KAFKA_LOG_ALERT: logging.CRITICAL,
    KAFKA_LOG_CRIT: logging.CRITICAL,
    KAFKA_LOG_ERR: logging.ERROR,
    KAFKA_LOG_WARNING: logging.WARNING,
    KAFKA_LOG_NOTICE: logging.INFO,
    KAFKA_LOG_INFO: logging.INFO,
    KAFKA_LOG_DEBUG: logging.DEBUG,
}


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured JSON logs similar to Pino.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_data = {
            "level": record.levelname.lower(),
            "time": datetime.now(timezone.utc).isoformat(),
            "name": record.name,
            "msg": record.getMessage(),
        }

        # Add extra fields if present
        if hasattr(record, "args") and record.args:
            log_data["args"] = record.args

        # Add exception info if present
        if record.exc_info:
            log_data["err"] = self.formatException(record.exc_info)

        # Add any extra attributes
        for key, value in record.__dict__.items():
            if key not in [
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName",
                "relativeCreated", "thread", "threadName", "exc_info",
                "exc_text", "stack_info",
            ]:
                log_data[key] = value

        return json.dumps(log_data)


def _setup_logger() -> logging.Logger:
    """Set up the main logger with structured JSON output"""
    log_level = LOG_LEVELS.get(
        str(os.environ.get("LOG_LEVEL", "info")).lower(),
        logging.INFO
    )

    # Create logger
    logger = logging.getLogger("-cloud")
    logger.setLevel(log_level)
    logger.propagate = False

    # Clear any existing handlers
    logger.handlers.clear()

    # Create handler that writes to stderr (file descriptor 2)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)

    # Set structured formatter
    formatter = StructuredFormatter()
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


# Create the main logger instance
logger = _setup_logger()


class KafkaLoggerAdapter:
    """
    Adapter for Kafka client logging that bridges to Python's logging system.
    Implements a logger interface compatible with confluent-kafka-python.
    """

    def __init__(
        self,
        base_logger: logging.Logger,
        namespace: str = "kafka",
        log_level: Optional[int] = None
    ):
        self.logger = base_logger.getChild(namespace)
        self._namespace = namespace
        
        # Set log level if provided (using the level map)
        if log_level is not None:
            python_level = KAFKA_LEVEL_MAP.get(log_level, logging.INFO)
            self.logger.setLevel(python_level)

    def namespace(
        self,
        sub_namespace: str,
        log_level: Optional[int] = None
    ) -> "KafkaLoggerAdapter":
        """Create a child logger with a sub-namespace"""
        return KafkaLoggerAdapter(self.logger, sub_namespace, log_level)

    def error(self, message: str, *args: Any) -> None:
        """Log error message"""
        self.logger.error(message, extra={"args": args} if args else {})

    def warn(self, message: str, *args: Any) -> None:
        """Log warning message"""
        self.logger.warning(message, extra={"args": args} if args else {})

    def info(self, message: str, *args: Any) -> None:
        """Log info message"""
        self.logger.info(message, extra={"args": args} if args else {})

    def debug(self, message: str, *args: Any) -> None:
        """Log debug message"""
        self.logger.debug(message, extra={"args": args} if args else {})

    def set_log_level(self, level: int) -> None:
        """Set the log level for this logger (using Kafka log level constants)"""
        python_level = KAFKA_LEVEL_MAP.get(level, logging.INFO)
        self.logger.setLevel(python_level)


# Create the Kafka logger instance
kafka_logger = KafkaLoggerAdapter(logger, "kafka")


def set_log_level(level: LogLevel) -> None:
    """
    Update the logger's level after environment is initialized.
    
    Args:
        level: The log level to set (fatal, error, warn, info, debug, trace)
    """
    python_level = LOG_LEVELS.get(level, logging.INFO)
    logger.setLevel(python_level)
    
    # Update all handlers
    for handler in logger.handlers:
        handler.setLevel(python_level)


def kafka_logger_callback(level: int, facility: str, message: str) -> None:
    """
    Callback function for confluent-kafka logger.
    This can be passed to the Kafka client configuration.
    
    Args:
        level: Log level from librdkafka (0-7)
        facility: Log facility string
        message: Log message
    """
    python_level = KAFKA_LEVEL_MAP.get(level, logging.INFO)
    kafka_logger.logger.log(python_level, message, extra={"facility": facility})


# Export log level constants for convenience
log_levels = {
    "fatal": 60,
    "error": 50,
    "warn": 40,
    "info": 30,
    "debug": 20,
    "trace": 10,
}