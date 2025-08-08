

import logging
import os
import json
from datetime import datetime
from typing import Dict, Any, Set
import weakref


# Custom filter to show only logs of the EXACT specified level
class ExactLevelFilter(logging.Filter):
    """Filter that only allows logs of the exact specified level"""

    def __init__(self, exact_level: int):
        super().__init__()
        self.exact_level = exact_level

    def filter(self, record):
        # Only allow records that match the EXACT level
        return record.levelno == self.exact_level


# Global logger registry to track all created loggers
_logger_registry: Set[weakref.ReferenceType] = set()
_current_filter_level = logging.INFO
_current_log_file = None

# Global logger configuration
logger_config = {
    "current_level": "INFO",
    "last_updated": datetime.now().isoformat()
}

CONFIG_FILE = "log_config.json"


def _register_logger(logger_instance):
    """Register a logger instance for level updates"""
    global _logger_registry
    # Clean up dead references
    _logger_registry = {ref for ref in _logger_registry if ref() is not None}
    # Add new logger reference
    _logger_registry.add(weakref.ref(logger_instance))


def load_log_config() -> Dict[str, Any]:
    """Load log configuration from file"""
    global logger_config, _current_filter_level
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                saved_config = json.load(f)
                logger_config.update(saved_config)
                _current_filter_level = getattr(logging, logger_config["current_level"])
    except Exception as e:
        print(f"Error loading log config: {e}")
    return logger_config


def save_log_config():
    """Save log configuration to file"""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(logger_config, f, indent=2)
    except Exception as e:
        print(f"Error saving log config: {e}")


def get_current_log_level() -> Dict[str, Any]:
    """Get the current logging level"""
    load_log_config()
    return {
        "log_level": logger_config["current_level"],
        "last_updated": logger_config["last_updated"],
        "message": f"Current log level is {logger_config['current_level']} (EXACT LEVEL ONLY)",
        "filtering_mode": "EXACT_LEVEL_ONLY"
    }


def get_daily_log_filename() -> str:
    """Generate daily log filename in the format: log-YYYY-MM-DD.log"""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    return f"logs/log-{date_str}.log"


def _update_all_loggers_filters(new_level_numeric: int):
    """Update filters for all registered loggers"""
    global _logger_registry

    # Clean up dead references
    _logger_registry = {ref for ref in _logger_registry if ref() is not None}

    # Update all registered loggers
    for logger_ref in _logger_registry:
        logger_instance = logger_ref()
        if logger_instance and logger_instance.handlers:
            # Set logger to DEBUG level so all messages reach handlers
            logger_instance.setLevel(logging.DEBUG)

            # Update all handlers with exact level filter
            for handler in logger_instance.handlers:
                # Clear existing filters
                handler.filters.clear()
                # Add exact level filter
                handler.addFilter(ExactLevelFilter(new_level_numeric))

    # Update root logger as well
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    for handler in root_logger.handlers:
        handler.filters.clear()
        handler.addFilter(ExactLevelFilter(new_level_numeric))

    # Update all loggers in the logger manager
    for logger_name in logging.Logger.manager.loggerDict:
        logger_instance = logging.getLogger(logger_name)
        if logger_instance.handlers:
            logger_instance.setLevel(logging.DEBUG)
            for handler in logger_instance.handlers:
                handler.filters.clear()
                handler.addFilter(ExactLevelFilter(new_level_numeric))


def update_log_level(new_level: str) -> Dict[str, Any]:
    """
    Update the logging level for all loggers.
    This will show ONLY logs of the exact specified level.
    """
    global logger_config, _current_filter_level

    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    new_level = new_level.upper()

    if new_level not in valid_levels:
        raise ValueError(f"Invalid log level. Must be one of: {', '.join(valid_levels)}")

    old_level = logger_config["current_level"]

    # Update global config
    logger_config["current_level"] = new_level
    logger_config["last_updated"] = datetime.now().isoformat()
    _current_filter_level = getattr(logging, new_level)

    # Save to file
    save_log_config()

    # Get numeric level
    numeric_level = getattr(logging, new_level)

    # Update all existing loggers
    _update_all_loggers_filters(numeric_level)

    # Create a temporary logger to show the change message at WARNING level
    # This ensures the change message is visible regardless of current level
    temp_logger = logging.getLogger("log_level_change_notification")
    temp_logger.handlers.clear()  # Clear any existing handlers
    temp_logger.setLevel(logging.WARNING)

    # Create a console handler for the change notification
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    console_handler.addFilter(ExactLevelFilter(logging.WARNING))
    temp_logger.addHandler(console_handler)
    temp_logger.propagate = False

    # Log the change
    temp_logger.warning(
        f"=== LOG LEVEL CHANGED FROM {old_level} TO {new_level} (EXACT LEVEL ONLY) AT {logger_config['last_updated']} ==="
    )

    # Clean up temporary logger
    temp_logger.handlers.clear()

    return {
        "log_level": new_level,
        "previous_level": old_level,
        "updated_at": logger_config["last_updated"],
        "message": f"Log level updated to {new_level}. Now showing ONLY {new_level} level logs.",
        "filtering_mode": "EXACT_LEVEL_ONLY"
    }


def setup_logger(name: str = "app_logger") -> logging.Logger:
    """
    Set up a logger that shows ONLY logs of the current configured level.
    All logs go to a single daily file: log-YYYY-MM-DD.log
    """
    global _current_filter_level, _current_log_file

    load_log_config()

    # Get or create logger
    logger = logging.getLogger(name)

    # Get current daily log file
    current_log_file = get_daily_log_filename()

    # Check if we need to update handlers (new day or first time setup)
    needs_handler_update = (
            not logger.handlers or
            _current_log_file != current_log_file
    )

    if needs_handler_update:
        # Clear existing handlers
        logger.handlers.clear()
        _current_log_file = current_log_file

        # Set logger level to DEBUG (lowest) so all messages reach the handler,
        # then let the filter decide what to show
        logger.setLevel(logging.DEBUG)

        # Create formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Get current level configuration
        current_level = logger_config["current_level"]
        numeric_level = getattr(logging, current_level)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.addFilter(ExactLevelFilter(numeric_level))
        logger.addHandler(console_handler)

        # Daily file handler - ensure directory exists
        log_dir = os.path.dirname(current_log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = logging.FileHandler(current_log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.addFilter(ExactLevelFilter(numeric_level))
        logger.addHandler(file_handler)

        # Prevent propagation to avoid duplicate messages
        logger.propagate = False

        # Register this logger for future level updates
        _register_logger(logger)

    return logger


def get_app_logger() -> logging.Logger:
    """
    Convenience function to get the main application logger.
    Creates a single daily log file that all parts of the application can use.
    """
    return setup_logger("app_logger")


# Monkey patch the logging module to ensure new loggers get the correct filter
_original_getLogger = logging.getLogger


def patched_getLogger(name=None):
    """Patched version of getLogger that applies current filter to new loggers"""
    logger = _original_getLogger(name)

    # If this logger has handlers and they don't have the current filter, update them
    if logger.handlers and hasattr(logger, '_filter_level_applied'):
        current_numeric_level = getattr(logging, logger_config["current_level"])
        if getattr(logger, '_filter_level_applied', None) != current_numeric_level:
            logger.setLevel(logging.DEBUG)
            for handler in logger.handlers:
                handler.filters.clear()
                handler.addFilter(ExactLevelFilter(current_numeric_level))
            logger._filter_level_applied = current_numeric_level

    return logger


# Apply the monkey patch
logging.getLogger = patched_getLogger

# Initialize configuration on module import
load_log_config()

# Example usage demonstrating exact level filtering:
if __name__ == "__main__":
    # Create main application logger
    logger = get_app_logger()

    print("=== Testing EXACT LEVEL filtering with single daily log ===\n")
    print(f"Log file: {get_daily_log_filename()}")

    # Test INFO level (should show ONLY INFO messages)
    print("1. Setting level to INFO (should show ONLY INFO messages):")
    update_log_level("INFO")

    logger.debug("This DEBUG message should NOT appear")
    logger.info("This INFO message SHOULD appear")
    logger.warning("This WARNING message should NOT appear")
    logger.error("This ERROR message should NOT appear")

    print("\n" + "=" * 50 + "\n")

    # Test WARNING level (should show ONLY WARNING messages)
    print("2. Setting level to WARNING (should show ONLY WARNING messages):")
    update_log_level("WARNING")

    logger.debug("This DEBUG message should NOT appear")
    logger.info("This INFO message should NOT appear")
    logger.warning("This WARNING message SHOULD appear")
    logger.error("This ERROR message should NOT appear")

    print("\n" + "=" * 50 + "\n")

    # Test DEBUG level (should show ONLY DEBUG messages)
    print("3. Setting level to DEBUG (should show ONLY DEBUG messages):")
    update_log_level("DEBUG")

    logger.debug("This DEBUG message SHOULD appear")
    logger.info("This INFO message should NOT appear")
    logger.warning("This WARNING message should NOT appear")
    logger.error("This ERROR message should NOT appear")

    print("\n" + "=" * 50 + "\n")

    # Test ERROR level (should show ONLY ERROR messages)
    print("4. Setting level to ERROR (should show ONLY ERROR messages):")
    update_log_level("ERROR")

    logger.debug("This DEBUG message should NOT appear")
    logger.info("This INFO message should NOT appear")
    logger.warning("This WARNING message should NOT appear")
    logger.error("This ERROR message SHOULD appear")
    logger.critical("This CRITICAL message should NOT appear")

    print("\n" + "=" * 50 + "\n")

    # Test creating a new logger after level change - should use same daily file
    print("5. Testing another logger - should use same daily file:")
    another_logger = setup_logger("another_component")

    another_logger.debug("Another logger DEBUG - should NOT appear")
    another_logger.info("Another logger INFO - should NOT appear")
    another_logger.warning("Another logger WARNING - should NOT appear")
    another_logger.error("Another logger ERROR - SHOULD appear")
    another_logger.critical("Another logger CRITICAL - should NOT appear")