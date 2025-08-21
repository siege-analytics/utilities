"""Geospatial and Census data utilities for Siege Analytics."""

# Import all major functionality from submodules

# Geocoding functionality
from .geocoding import (
    concatenate_addresses,
    use_nominatim_geocoder,
    NominatimGeoClassifier
)

# Spatial data functionality
from .spatial_data import (
    get_census_data,
    get_census_boundaries,
    download_osm_data,
    CensusDirectoryDiscovery,
    SpatialDataSource,
    CensusDataSource,
    GovernmentDataSource,
    OpenStreetMapDataSource
)

# Spatial transformations
from .spatial_transformations import (
    convert_spatial_format,
    SpatialDataTransformer,
    PostGISConnector,
    DuckDBConnector
)

# Census data selection
from .census_data_selector import (
    get_census_data_selector,
    select_census_datasets,
    CensusDataSelector
)

# Census dataset mapping
from .census_dataset_mapper import (
    get_census_dataset_mapper,
    get_best_dataset_for_analysis,
    SurveyType,
    GeographyLevel,
    DataReliability,
    CensusDataset,
    DatasetRelationship,
    CensusDatasetMapper
)

# Define what gets exported when using 'from siege_utilities.geo import *'
__all__ = [
    # Geocoding
    'concatenate_addresses',
    'use_nominatim_geocoder', 
    'NominatimGeoClassifier',
    
    # Spatial data
    'get_census_data',
    'get_census_boundaries',
    'download_osm_data',
    'CensusDirectoryDiscovery',
    'SpatialDataSource',
    'CensusDataSource', 
    'GovernmentDataSource',
    'OpenStreetMapDataSource',
    
    # Spatial transformations
    'convert_spatial_format',
    'SpatialDataTransformer',
    'PostGISConnector',
    'DuckDBConnector',
    
    # Census data selection
    'get_census_data_selector',
    'select_census_datasets',
    'CensusDataSelector',
    
    # Census dataset mapping
    'get_census_dataset_mapper',
    'get_best_dataset_for_analysis',
    'SurveyType',
    'GeographyLevel', 
    'DataReliability',
    'CensusDataset',
    'DatasetRelationship',
    'CensusDatasetMapper'
]
