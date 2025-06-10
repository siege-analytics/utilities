"""
Utilities Library - A comprehensive toolkit for file operations, distributed computing, and geospatial processing.

This package provides utilities for:
- File operations and management
- Distributed computing (HDFS, Spark)
- Geospatial operations and geocoding
- Core utilities (logging, string manipulation)

Usage:
    from siege_utilities import files, core, distributed, geo

    # Or import specific functions
    from siege_utilities.files import download_file
    from siege_utilities.core.logging import init_logger
    from siege_utilities.geo import geocode_address
"""

import sys
import warnings

# Version info
__version__ = "1.0.0"
__author__ = "Your Name"

# Core utilities - always available
from . import core

# File utilities - minimal dependencies
from . import files

# Optional imports with graceful degradation
try:
    from . import distributed
    _DISTRIBUTED_AVAILABLE = True
except ImportError as e:
    warnings.warn(f"Distributed computing utilities not available: {e}")
    _DISTRIBUTED_AVAILABLE = False

try:
    from . import geo
    _GEO_AVAILABLE = True
except ImportError as e:
    warnings.warn(f"Geospatial utilities not available: {e}")
    _GEO_AVAILABLE = False


def check_dependencies():
    """Check which optional dependencies are available."""
    deps = {
        "core": True,
        "files": True,
        "distributed": _DISTRIBUTED_AVAILABLE,
        "geo": _GEO_AVAILABLE
    }

    print("Utilities Library Dependencies:")
    for module, available in deps.items():
        status = "✓" if available else "✗"
        print(f"  {status} {module}")

    return deps


# Convenience imports for common operations
from .core.logging import init_logger, log_info, log_error, log_warning
from .files.operations import check_file_exists, create_empty_file
from .files.paths import ensure_path_exists

# Optional convenience imports
if _GEO_AVAILABLE:
    from .geo.geocoding import geocode_address

if _DISTRIBUTED_AVAILABLE:
    try:
        from .distributed.spark import setup_spark_session
    except ImportError:
        pass


__all__ = [
    # Version info
    "__version__",
    "__author__",

    # Modules
    "core",
    "files",

    # Core functions
    "init_logger",
    "log_info",
    "log_error",
    "log_warning",
    "check_file_exists",
    "create_empty_file",
    "ensure_path_exists",
    "check_dependencies",
]

# Add optional exports if available
if _GEO_AVAILABLE:
    __all__.extend(["geo", "geocode_address"])

if _DISTRIBUTED_AVAILABLE:
    __all__.extend(["distributed"])
    try:
        __all__.append("setup_spark_session")
    except NameError:
        pass
