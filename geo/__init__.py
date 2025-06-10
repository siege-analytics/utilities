"""
Geospatial utilities package.

This package provides geospatial and geocoding functionality including:
- Address geocoding with multiple providers
- Geocoding result classification
- Coordinate validation and processing
- Integration with PySpark for distributed geocoding
"""

import warnings

# Geocoding utilities
try:
    from . import geocoding
    _GEOCODING_AVAILABLE = True
except ImportError as e:
    _GEOCODING_AVAILABLE = False
    warnings.warn(f"Geocoding utilities not available: {e}")

# Import commonly used functions for convenience
if _GEOCODING_AVAILABLE:
    from .geocoding import (
        GeocodingConfig,
        geocode_address,
        batch_geocode,
        concatenate_address_components,
        NominatimClassifier
    )

__all__ = [
    # Utility functions (always available)
    "quick_geocode",
    "validate_geocoding_result",
    "extract_coordinates",
    "format_coordinates",
    "check_geocoding_capabilities",
]

# Add geocoding exports if available
if _GEOCODING_AVAILABLE:
    __all__.extend([
        "geocoding",
        "GeocodingConfig",
        "geocode_address",
        "batch_geocode",
        "concatenate_address_components",
        "NominatimClassifier"
    ])
