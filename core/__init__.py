"""
Core utilities package.

This package contains fundamental utilities with minimal dependencies:
- Logging configuration and functions
- String manipulation utilities
"""

from . import logging
from . import strings

# Import commonly used functions for convenience
from .logging import (
    init_logger,
    get_logger,
    log_debug,
    log_info,
    log_warning,
    log_error,
    log_critical,
    set_log_level
)

from .strings import (
    remove_wrapping_quotes_and_trim,
    remove_quotes,  # Alias
    sanitize_string_for_filename,
    sanitize_filename,  # Alias
    sanitize_column_names,
    truncate_string,
    normalize_whitespace,
    slugify
)

__all__ = [
    # Submodules
    "logging",
    "strings",

    # Logging functions
    "init_logger",
    "get_logger",
    "log_debug",
    "log_info",
    "log_warning",
    "log_error",
    "log_critical",
    "set_log_level",

    # String functions
    "remove_wrapping_quotes_and_trim",
    "remove_quotes",
    "sanitize_string_for_filename",
    "sanitize_filename",
    "sanitize_column_names",
    "truncate_string",
    "normalize_whitespace",
    "slugify",
]
