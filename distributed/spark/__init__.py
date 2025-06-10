"""
Spark utilities package.

This package provides comprehensive PySpark utilities for:
- DataFrame operations and transformations
- JSON processing and flattening
- Geospatial operations with Apache Sedona
- Export utilities with atomic operations
- Data validation and quality checks
"""

import warnings

# Core DataFrame operations - always available
from . import dataframe

# Optional imports with graceful degradation
try:
    from . import json_utils
    _JSON_AVAILABLE = True
except ImportError:
    _JSON_AVAILABLE = False
    warnings.warn("JSON utilities not available")

try:
    from . import geospatial
    _GEOSPATIAL_AVAILABLE = True
except ImportError:
    _GEOSPATIAL_AVAILABLE = False
    warnings.warn("Geospatial utilities not available (requires Apache Sedona)")

try:
    from . import export
    _EXPORT_AVAILABLE = True
except ImportError:
    _EXPORT_AVAILABLE = False
    warnings.warn("Export utilities not available")

try:
    from . import validation
    _VALIDATION_AVAILABLE = True
except ImportError:
    _VALIDATION_AVAILABLE = False
    warnings.warn("Validation utilities not available")

# Import commonly used functions for convenience
from .dataframe import (
    sanitize_column_names,
    get_row_count,
    get_column_info,
    repartition_and_cache,
    create_temp_view,
    sample_dataframe
)

__all__ = [
    # Core modules (always available)
    "dataframe",

    # Core functions
    "sanitize_column_names",
    "get_row_count",
    "get_column_info",
    "repartition_and_cache",
    "create_temp_view",
    "sample_dataframe",
]
