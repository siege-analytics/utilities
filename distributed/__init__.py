"""
Distributed computing utilities package.

This package provides utilities for distributed computing including:
- HDFS operations with proper configuration management
- PySpark DataFrame utilities and operations
- Geospatial operations with Apache Sedona
- Export utilities with atomic operations
- Data validation and quality checks
"""

import warnings

# Core HDFS utilities
try:
    from . import hdfs
    _HDFS_AVAILABLE = True
except ImportError as e:
    _HDFS_AVAILABLE = False
    warnings.warn(f"HDFS utilities not available: {e}")

# Spark utilities
try:
    from . import spark
    _SPARK_AVAILABLE = True
except ImportError as e:
    _SPARK_AVAILABLE = False
    warnings.warn(f"Spark utilities not available: {e}")

# Import convenience functions if available
if _HDFS_AVAILABLE:
    from .hdfs import (
        HDFSConfig,
        HDFSOperations,
        quick_setup,
        setup_for_geocoding,
        check_hdfs_available
    )

if _SPARK_AVAILABLE:
    from .spark import (
        setup_spark_session,
        sanitize_column_names,
        get_row_count,
        export_to_csv
    )

__all__ = [
    # Utility functions (always available)
    "setup_distributed_environment",
    "check_distributed_capabilities",
    "get_recommended_setup",
]

# Add HDFS exports if available
if _HDFS_AVAILABLE:
    __all__.extend([
        "hdfs",
        "HDFSConfig",
        "HDFSOperations",
        "quick_setup",
        "setup_for_geocoding",
        "check_hdfs_available"
    ])

# Add Spark exports if available
if _SPARK_AVAILABLE:
    __all__.extend([
        "spark",
        "setup_spark_session",
        "sanitize_column_names",
        "get_row_count",
        "export_to_csv"
    ])
