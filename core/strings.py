# python stdlib imports
from logging import lastResort

# custom functions and data

# Fixed: from utilities import *
# Import specific functions instead

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


# Aliases for commonly used functions
remove_quotes = remove_wrapping_quotes_and_trim


def sanitize_string_for_filename(target_string: str) -> str:
    """
    Sanitize a string to make it safe for use as a filename.
    
    Args:
        target_string: String to sanitize
        
    Returns:
        String safe for use as filename
    """
    if target_string is None:
        return ""
    
    # Remove or replace problematic characters
    import re
    # Replace problematic characters with underscores
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', target_string)
    # Remove control characters
    sanitized = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', sanitized)
    # Trim whitespace and dots (can cause issues)
    sanitized = sanitized.strip(' .')
    
    return sanitized


# Alias
sanitize_filename = sanitize_string_for_filename


def sanitize_column_names(columns):
    """
    Sanitize column names for dataframes.
    
    Args:
        columns: List of column names or single column name
        
    Returns:
        Sanitized column names
    """
    if isinstance(columns, str):
        return sanitize_string_for_filename(columns).replace(' ', '_')
    
    return [sanitize_string_for_filename(col).replace(' ', '_') for col in columns]


def truncate_string(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate a string to a maximum length.
    
    Args:
        text: String to truncate
        max_length: Maximum length allowed
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated string
    """
    if text is None:
        return ""
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def normalize_whitespace(text: str) -> str:
    """
    Normalize whitespace in a string (collapse multiple spaces to single spaces).
    
    Args:
        text: String to normalize
        
    Returns:
        String with normalized whitespace
    """
    if text is None:
        return ""
    
    import re
    return re.sub(r'\s+', ' ', text.strip())


def slugify(text: str) -> str:
    """
    Convert a string to a URL-friendly slug.
    
    Args:
        text: String to slugify
        
    Returns:
        Slugified string
    """
    if text is None:
        return ""
    
    import re
    # Convert to lowercase
    slug = text.lower()
    # Replace spaces and other characters with hyphens
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    
    return slug


__all__ = [
    "remove_wrapping_quotes_and_trim",
    "remove_quotes",
    "sanitize_string_for_filename",
    "sanitize_filename",
    "sanitize_column_names",
    "truncate_string",
    "normalize_whitespace",
    "slugify",
]
