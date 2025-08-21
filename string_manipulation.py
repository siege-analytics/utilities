# python stdlib imports
from logging import lastResort

# ============================================================================
# STANDARDIZED LOGGING IMPORT
# ============================================================================
try:
    from utilities.logging_utils import setup_module_logging
    setup_module_logging(globals(), __name__)
except ImportError:
    # Handle case where package has different name
    import sys
    package_name = __name__.split('.')[0]
    try:
        logging_module = sys.modules[f"{package_name}.logging_utils"]
        setup_func = getattr(logging_module, 'setup_module_logging', None)
        if setup_func:
            setup_func(globals(), __name__)
        else:
            # Individual function import fallback
            log_info = getattr(logging_module, 'log_info', lambda x: print(f"INFO: {x}"))
            log_debug = getattr(logging_module, 'log_debug', lambda x: print(f"DEBUG: {x}"))
            log_warning = getattr(logging_module, 'log_warning', lambda x: print(f"WARNING: {x}"))
            log_error = getattr(logging_module, 'log_error', lambda x: print(f"ERROR: {x}"))
            log_critical = getattr(logging_module, 'log_critical', lambda x: print(f"CRITICAL: {x}"))
    except:
        # Fallback functions
        def log_info(msg): print(f"INFO: {msg}")
        def log_debug(msg): print(f"DEBUG: {msg}")
        def log_warning(msg): print(f"WARNING: {msg}")
        def log_error(msg): print(f"ERROR: {msg}")
        def log_critical(msg): print(f"CRITICAL: {msg}")

# custom functions and data

# This module doesn't actually need utilities imports for its core function
# Remove the circular dependency

# logging

import logging

logging.getLogger(__name__)


def remove_wrapping_quotes_and_trim(target_string: str) -> str:
    """
    Removes wrapping quotes (single or double) from a string and trims whitespace

    Args:
        target_string: String that may have wrapping quotes and whitespace

    Returns:
        String with any wrapping quotes and whitespace removed
    """
    # Check for None or empty strings first
    if target_string is None:
        return ""

    # Handle empty strings and newlines
    strings_to_ignore = ["", "\n"]
    if target_string in strings_to_ignore:
        return target_string.strip()

    # Start by trimming whitespace
    return_string = target_string.strip()

    # Check for wrapping quotes
    wrapping_characters = ['"', "'"]

    # Only proceed if string has at least 2 characters (needed for first/last char check)
    if len(return_string) >= 2:
        first_char = return_string[0]
        last_char = return_string[-1]

        # Check if string is wrapped in quotes
        if first_char == last_char and first_char in wrapping_characters:
            # Remove the wrapping quotes and trim any resulting whitespace
            return_string = return_string[1:-1].strip()

    return return_string
