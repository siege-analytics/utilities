"""
Fixed Census Data Downloader

Provides reliable functions for downloading Census boundary files and thematic data
with proper error handling, SSL support, and fallback mechanisms.
"""

import os
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from urllib.parse import urljoin
import time
from datetime import datetime

# Optional dependencies with fallbacks
try:
    import requests
except ImportError:
    requests = None

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import geopandas as gpd
except ImportError:
    gpd = None

# Import utilities
from utilities.logging_utils import log_info, log_error, log_warning
from utilities.file_utilities.remote_files import download_file
from utilities.file_utilities.paths import ensure_path_exists

# Get logger
log = logging.getLogger(__name__)

# Census TIGER/Line base URLs and patterns by year
CENSUS_TIGER_BASE = "https://www2.census.gov/geo/tiger"
CENSUS_API_BASE = "https://api.census.gov/data"

# URL patterns have changed over the years - this maps year ranges to URL patterns
TIGER_URL_PATTERNS = {
    # Modern pattern (2013+): GENZ{year}/shp/cb_{year}_{geo}_{resolution}.zip
    'modern': {
        'years': range(2013, 2025),
        'national_pattern': "{base}/GENZ{year}/shp/cb_{year}_us_{geography}_{resolution}.zip",
        'state_pattern': "{base}/GENZ{year}/shp/cb_{year}_{state_fips}_{geography}_{resolution}.zip"
    },
    # Legacy pattern (2010-2012): GENZ{year}/gz_{year}_{geo}_{resolution}.zip  
    'legacy_2010s': {
        'years': range(2010, 2013),
        'national_pattern': "{base}/GENZ{year}/gz_{year}_us_{geography}_{resolution}.zip",
        'state_pattern': "{base}/GENZ{year}/gz_{year}_{state_fips}_{geography}_{resolution}.zip"
    },
    # Older pattern (2007-2009): TIGER{year}/{geography}/tl_{year}_{fips}_{geography}.zip
    'legacy_2000s': {
        'years': range(2007, 2010),
        'national_pattern': "{base}/TIGER{year}/{geography}/tl_{year}_us_{geography}.zip",
        'state_pattern': "{base}/TIGER{year}/{geography}/tl_{year}_{state_fips}_{geography}.zip"
    },
    # Very old pattern (2000-2006): Different structure entirely
    'legacy_2000': {
        'years': range(2000, 2007),
        'national_pattern': "{base}/TIGER{year}/tgr{geography}.zip",
        'state_pattern': "{base}/TIGER{year}/tgr{state_fips}.zip"
    }
}

# Geography name mappings for different years (some names changed)
GEOGRAPHY_NAME_MAPPINGS = {
    # Modern names -> legacy names for older years
    'bg': {  # block groups
        range(2013, 2025): 'bg',
        range(2010, 2013): 'bg', 
        range(2007, 2010): 'bg',
        range(2000, 2007): 'blkgrp'
    },
    'tract': {
        range(2013, 2025): 'tract',
        range(2010, 2013): 'tract',
        range(2007, 2010): 'tract', 
        range(2000, 2007): 'trt'
    },
    'county': {
        range(2013, 2025): 'county',
        range(2010, 2013): 'county',
        range(2007, 2010): 'county',
        range(2000, 2007): 'co'
    },
    'state': {
        range(2013, 2025): 'state',
        range(2010, 2013): 'state', 
        range(2007, 2010): 'state',
        range(2000, 2007): 'st'
    },
    'place': {
        range(2013, 2025): 'place',
        range(2010, 2013): 'place',
        range(2007, 2010): 'place',
        range(2000, 2007): 'pl'
    },
    'zcta5': {  # ZIP Code Tabulation Areas 
        range(2013, 2025): 'zcta520',  # ZCTA based on 2020 Census
        range(2010, 2013): 'zcta510',  # ZCTA based on 2010 Census
        range(2007, 2010): 'zcta500',  # ZCTA based on 2000 Census
        range(2000, 2007): None        # Not available in very old format
    }
}

# Comprehensive FIPS codes mapping with names and abbreviations
# Includes all 50 states, DC, and territories
FIPS_DATA = {
    '01': {'name': 'Alabama', 'abbrev': 'AL'},
    '02': {'name': 'Alaska', 'abbrev': 'AK'},
    '04': {'name': 'Arizona', 'abbrev': 'AZ'},
    '05': {'name': 'Arkansas', 'abbrev': 'AR'},
    '06': {'name': 'California', 'abbrev': 'CA'},
    '08': {'name': 'Colorado', 'abbrev': 'CO'},
    '09': {'name': 'Connecticut', 'abbrev': 'CT'},
    '10': {'name': 'Delaware', 'abbrev': 'DE'},
    '11': {'name': 'District of Columbia', 'abbrev': 'DC'},
    '12': {'name': 'Florida', 'abbrev': 'FL'},
    '13': {'name': 'Georgia', 'abbrev': 'GA'},
    '15': {'name': 'Hawaii', 'abbrev': 'HI'},
    '16': {'name': 'Idaho', 'abbrev': 'ID'},
    '17': {'name': 'Illinois', 'abbrev': 'IL'},
    '18': {'name': 'Indiana', 'abbrev': 'IN'},
    '19': {'name': 'Iowa', 'abbrev': 'IA'},
    '20': {'name': 'Kansas', 'abbrev': 'KS'},
    '21': {'name': 'Kentucky', 'abbrev': 'KY'},
    '22': {'name': 'Louisiana', 'abbrev': 'LA'},
    '23': {'name': 'Maine', 'abbrev': 'ME'},
    '24': {'name': 'Maryland', 'abbrev': 'MD'},
    '25': {'name': 'Massachusetts', 'abbrev': 'MA'},
    '26': {'name': 'Michigan', 'abbrev': 'MI'},
    '27': {'name': 'Minnesota', 'abbrev': 'MN'},
    '28': {'name': 'Mississippi', 'abbrev': 'MS'},
    '29': {'name': 'Missouri', 'abbrev': 'MO'},
    '30': {'name': 'Montana', 'abbrev': 'MT'},
    '31': {'name': 'Nebraska', 'abbrev': 'NE'},
    '32': {'name': 'Nevada', 'abbrev': 'NV'},
    '33': {'name': 'New Hampshire', 'abbrev': 'NH'},
    '34': {'name': 'New Jersey', 'abbrev': 'NJ'},
    '35': {'name': 'New Mexico', 'abbrev': 'NM'},
    '36': {'name': 'New York', 'abbrev': 'NY'},
    '37': {'name': 'North Carolina', 'abbrev': 'NC'},
    '38': {'name': 'North Dakota', 'abbrev': 'ND'},
    '39': {'name': 'Ohio', 'abbrev': 'OH'},
    '40': {'name': 'Oklahoma', 'abbrev': 'OK'},
    '41': {'name': 'Oregon', 'abbrev': 'OR'},
    '42': {'name': 'Pennsylvania', 'abbrev': 'PA'},
    '44': {'name': 'Rhode Island', 'abbrev': 'RI'},
    '45': {'name': 'South Carolina', 'abbrev': 'SC'},
    '46': {'name': 'South Dakota', 'abbrev': 'SD'},
    '47': {'name': 'Tennessee', 'abbrev': 'TN'},
    '48': {'name': 'Texas', 'abbrev': 'TX'},
    '49': {'name': 'Utah', 'abbrev': 'UT'},
    '50': {'name': 'Vermont', 'abbrev': 'VT'},
    '51': {'name': 'Virginia', 'abbrev': 'VA'},
    '53': {'name': 'Washington', 'abbrev': 'WA'},
    '54': {'name': 'West Virginia', 'abbrev': 'WV'},
    '55': {'name': 'Wisconsin', 'abbrev': 'WI'},
    '56': {'name': 'Wyoming', 'abbrev': 'WY'},
    # US Territories
    '60': {'name': 'American Samoa', 'abbrev': 'AS'},
    '66': {'name': 'Guam', 'abbrev': 'GU'},
    '69': {'name': 'Northern Mariana Islands', 'abbrev': 'MP'},
    '72': {'name': 'Puerto Rico', 'abbrev': 'PR'},
    '78': {'name': 'Virgin Islands', 'abbrev': 'VI'}
}

# Create reverse lookup dictionaries for convenience
STATE_FIPS = {data['name']: fips for fips, data in FIPS_DATA.items()}
ABBREV_FIPS = {data['abbrev']: fips for fips, data in FIPS_DATA.items()}
FIPS_TO_STATE = {fips: data['name'] for fips, data in FIPS_DATA.items()}
FIPS_TO_ABBREV = {fips: data['abbrev'] for fips, data in FIPS_DATA.items()}

# Geography levels that work at national scale vs need state filtering
NATIONAL_GEOGRAPHIES = ['state', 'county', 'zcta5', 'cbsa', 'place']
STATE_GEOGRAPHIES = ['tract', 'bg', 'block', 'cousub', 'concity']


class CensusBoundaryDownloader:
    """
    Fixed Census boundary downloader with robust error handling.
    """
    
    def __init__(self, download_dir: Optional[str] = None):
        self.download_dir = Path(download_dir) if download_dir else Path.cwd() / "census_data"
        ensure_path_exists(self.download_dir)
        
        # Available years (extended range)
        self.available_years = list(range(2007, 2024))  # 2007-2023
        
        # Cache for URL validation
        self.url_cache = {}
        
        log_info(f"Initialized Census boundary downloader, data dir: {self.download_dir}")
        log_info(f"Supports years: {min(self.available_years)}-{max(self.available_years)}")
    
    def download_boundaries(
        self,
        geography: str = 'county',
        year: int = 2020,
        state: Optional[Union[str, List[str]]] = None,
        resolution: str = '500k'
    ) -> Optional[str]:
        """
        Download Census boundary files (shapefiles).
        
        Args:
            geography: Geographic level ('state', 'county', 'tract', 'bg' for block group)
            year: Census year (2020, 2021, 2022, 2023)
            state: State name, abbreviation, or FIPS code (or list of them)
            resolution: Map resolution ('500k', '5m', '20m' - lower is higher resolution)
            
        Returns:
            Path to downloaded shapefile directory or None if failed
        """
        if not requests:
            log_error("Census downloads require 'requests' library: pip install requests")
            return None
            
        try:
            # Validate and normalize inputs
            geography = self._normalize_geography(geography)
            year = self._validate_year(year)
            states = self._normalize_states(state) if state else []
            
            log_info(f"Downloading {geography} boundaries for {year} (resolution: {resolution})")
            
            # For state-level geographies, download each state separately
            if geography in STATE_GEOGRAPHIES and not states:
                log_error(f"Geography '{geography}' requires state parameter")
                return None
            
            if geography in STATE_GEOGRAPHIES and states:
                return self._download_state_geographies(geography, year, states, resolution)
            else:
                return self._download_national_geography(geography, year, resolution)
                
        except Exception as e:
            log_error(f"Failed to download boundaries: {e}")
            return None
    
    def _normalize_geography(self, geography: str) -> str:
        """Normalize geography names to Census conventions."""
        mapping = {
            'states': 'state',
            'counties': 'county',
            'tracts': 'tract',
            'block_groups': 'bg',
            'block_group': 'bg',
            'blocks': 'block',
            'zip_codes': 'zcta5',
            'zipcodes': 'zcta5',
            'zcta': 'zcta5'
        }
        return mapping.get(geography.lower(), geography.lower())
    
    def _validate_year(self, year: int) -> int:
        """Validate and potentially adjust year."""
        if year in self.available_years:
            return year
        
        # Find closest available year
        closest = min(self.available_years, key=lambda x: abs(x - year))
        log_warning(f"Year {year} not in supported range {min(self.available_years)}-{max(self.available_years)}, using closest: {closest}")
        return closest
    
    def _normalize_states(self, states: Union[str, List[str]]) -> List[str]:
        """Convert state names/abbreviations to FIPS codes."""
        if isinstance(states, str):
            states = [states]
        
        fips_codes = []
        for state in states:
            fips = self._get_state_fips(state)
            if fips:
                fips_codes.append(fips)
            else:
                log_warning(f"Unknown state: {state}")
        
        return fips_codes
    
    def _get_state_fips(self, state: str) -> Optional[str]:
        """Get FIPS code for a state name, abbreviation, or FIPS code."""
        state = state.strip()
        
        # Already a FIPS code - validate it exists
        if state.isdigit() and len(state) <= 2:
            fips_code = state.zfill(2)  # Pad with leading zero if needed
            return fips_code if fips_code in FIPS_DATA else None
        
        # Try exact state name match (case-insensitive)
        for fips, data in FIPS_DATA.items():
            if data['name'].lower() == state.lower():
                return fips
        
        # Try state abbreviation match (case-insensitive)
        state_upper = state.upper()
        if state_upper in ABBREV_FIPS:
            return ABBREV_FIPS[state_upper]
        
        # Try partial name match (for common variations)
        state_lower = state.lower()
        for fips, data in FIPS_DATA.items():
            name_lower = data['name'].lower()
            # Handle common variations
            if (name_lower.startswith(state_lower) or 
                state_lower in name_lower or
                # Special cases
                (state_lower == 'dc' and 'district' in name_lower) or
                (state_lower == 'virgin islands' and 'virgin' in name_lower)):
                return fips
        
        return None
    
    def _download_national_geography(self, geography: str, year: int, resolution: str) -> Optional[str]:
        """Download national-level geography with year-appropriate URL pattern."""
        url_info = self._get_url_pattern_for_year(year)
        if not url_info:
            log_error(f"No URL pattern available for year {year}")
            return None
        
        # Get geography name for this year
        geography_name = self._get_geography_name_for_year(geography, year)
        if not geography_name:
            log_error(f"Geography '{geography}' not available for year {year}")
            return None
        
        # Construct URL using appropriate pattern
        url = url_info['national_pattern'].format(
            base=CENSUS_TIGER_BASE,
            year=year,
            geography=geography_name,
            resolution=resolution
        )
        
        # Try multiple resolutions if the requested one fails
        urls_to_try = [url]
        if resolution != '500k':
            # Also try 500k as fallback
            fallback_url = url_info['national_pattern'].format(
                base=CENSUS_TIGER_BASE,
                year=year, 
                geography=geography_name,
                resolution='500k'
            )
            urls_to_try.append(fallback_url)
        
        for attempt_url in urls_to_try:
            log_info(f"Trying URL: {attempt_url}")
            result = self._download_and_extract(attempt_url, f"{geography}_{year}_{resolution}")
            if result:
                return result
            
        return None
    
    def _download_state_geographies(self, geography: str, year: int, states: List[str], resolution: str) -> Optional[str]:
        """Download state-level geographies (tracts, block groups, etc.)."""
        downloaded_files = []
        
        for state_fips in states:
            state_info = FIPS_DATA.get(state_fips, {})
            state_name = state_info.get('name', state_fips)
            state_abbrev = state_info.get('abbrev', state_fips)
            log_info(f"Downloading {geography} for {state_name} ({state_abbrev}, {state_fips})")
            
            # Get URL pattern and geography name for this year
            url_info = self._get_url_pattern_for_year(year)
            if not url_info:
                log_error(f"No URL pattern available for year {year}")
                continue
                
            geography_name = self._get_geography_name_for_year(geography, year)
            if not geography_name:
                log_warning(f"Geography '{geography}' not available for year {year}")
                continue
            
            # Construct state-specific URL
            url = url_info['state_pattern'].format(
                base=CENSUS_TIGER_BASE,
                year=year,
                state_fips=state_fips,
                geography=geography_name,
                resolution=resolution
            )
            
            output_dir = f"{geography}_{year}_{state_fips}_{resolution}"
            downloaded = self._download_and_extract(url, output_dir)
            
            if downloaded:
                downloaded_files.append(downloaded)
            else:
                log_warning(f"Failed to download {geography} for {state_name}")
        
        if downloaded_files:
            # Return the directory containing all state files
            return str(self.download_dir / f"{geography}_{year}_{resolution}_multi_state")
        
        return None
    
    def _download_and_extract(self, url: str, output_dirname: str) -> Optional[str]:
        """Download and extract a Census shapefile."""
        try:
            output_dir = self.download_dir / output_dirname
            
            # Check if already exists
            if output_dir.exists() and any(output_dir.glob("*.shp")):
                log_info(f"Shapefile already exists: {output_dir}")
                return str(output_dir)
            
            ensure_path_exists(output_dir)
            
            # Download zip file to temporary location
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
                temp_path = temp_file.name
            
            log_info(f"Downloading from: {url}")
            success = download_file(url, temp_path)
            
            if not success:
                log_error(f"Failed to download: {url}")
                return None
            
            # Extract zip file
            log_info(f"Extracting to: {output_dir}")
            with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            
            # Clean up temp file
            os.unlink(temp_path)
            
            # Verify shapefile was extracted
            shp_files = list(output_dir.glob("*.shp"))
            if shp_files:
                log_info(f"Successfully downloaded and extracted shapefile: {shp_files[0]}")
                return str(output_dir)
            else:
                log_error(f"No shapefile found in extracted data: {output_dir}")
                return None
                
        except Exception as e:
            log_error(f"Error downloading/extracting {url}: {e}")
            return None
    
    def load_boundaries(self, shapefile_path: str):
        """Load downloaded boundaries into a GeoDataFrame if geopandas is available."""
        if not gpd:
            log_error("Loading boundaries requires geopandas: pip install geopandas")
            return None
        
        try:
            # Find shapefile in directory
            path = Path(shapefile_path)
            if path.is_dir():
                shp_files = list(path.glob("*.shp"))
                if not shp_files:
                    log_error(f"No shapefile found in: {path}")
                    return None
                shapefile_path = str(shp_files[0])
            
            log_info(f"Loading shapefile: {shapefile_path}")
            gdf = gpd.read_file(shapefile_path)
            
            log_info(f"Loaded {len(gdf)} features with columns: {list(gdf.columns)}")
            return gdf
            
        except Exception as e:
            log_error(f"Failed to load shapefile: {e}")
            return None
    
    def get_available_geographies(self) -> Dict[str, str]:
        """Get list of available geography types."""
        return {
            'state': 'State boundaries',
            'county': 'County boundaries', 
            'tract': 'Census tract boundaries (requires state)',
            'bg': 'Block group boundaries (requires state)',
            'block': 'Block boundaries (requires state)',
            'zcta5': 'ZIP Code Tabulation Areas',
            'place': 'Incorporated places',
            'cbsa': 'Metropolitan/Micropolitan Statistical Areas'
        }


class CensusDataAPI:
    """
    Simple Census API client for thematic data with fallback mechanisms.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = CENSUS_API_BASE
        
        if not api_key:
            log_warning("No Census API key provided - some datasets may be limited")
    
    def get_population_data(
        self,
        geography: str = 'county',
        state: Optional[str] = None,
        variables: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get basic population data from Census API.
        
        Args:
            geography: Geographic level ('state', 'county', 'tract')
            state: State FIPS code or name (required for county/tract)
            variables: List of variable codes (uses defaults if None)
            
        Returns:
            Dictionary with data or None if failed
        """
        if not requests:
            log_error("Census API requires 'requests' library")
            return None
        
        # Default population variables
        if not variables:
            variables = ['B01003_001E']  # Total population
        
        try:
            # Construct API URL
            year = 2021  # Most recent ACS 5-year
            url = f"{self.base_url}/{year}/acs/acs5"
            
            params = {
                'get': ','.join(['NAME'] + variables),
                'for': geography + ':*'
            }
            
            if state and geography in ['county', 'tract']:
                state_fips = self._get_state_fips(state)
                if state_fips:
                    params['in'] = f'state:{state_fips}'
            
            if self.api_key:
                params['key'] = self.api_key
            
            log_info(f"Requesting Census data: {geography}")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                log_info(f"Retrieved {len(data)-1} records from Census API")
                return {'data': data, 'variables': variables}
            else:
                log_error(f"Census API error: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            log_error(f"Failed to get Census data: {e}")
            return None
    
    def _get_state_fips(self, state: str) -> Optional[str]:
        """Get FIPS code for state (reuse from boundary downloader)."""
        downloader = CensusBoundaryDownloader()
        return downloader._get_state_fips(state)


# Convenience functions
def download_census_boundaries(
    geography: str = 'county',
    year: int = 2020,
    state: Optional[Union[str, List[str]]] = None,
    download_dir: Optional[str] = None
) -> Optional[str]:
    """
    Download Census boundary files.
    
    Args:
        geography: Geographic level ('state', 'county', 'tract', 'bg')
        year: Census year 
        state: State name, abbreviation, or FIPS (required for tract/bg)
        download_dir: Directory to save files (uses current dir if None)
        
    Returns:
        Path to downloaded shapefile directory or None if failed
    """
    downloader = CensusBoundaryDownloader(download_dir)
    return downloader.download_boundaries(geography, year, state)


def get_census_data(
    geography: str = 'county',
    state: Optional[str] = None,
    api_key: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Get Census thematic data.
    
    Args:
        geography: Geographic level
        state: State for filtering
        api_key: Census API key (optional)
        
    Returns:
        Dictionary with Census data or None if failed
    """
    api = CensusDataAPI(api_key)
    return api.get_population_data(geography, state)


def get_state_fips(state_identifier: str) -> Optional[str]:
    """
    Get FIPS code for a state by name, abbreviation, or validate existing FIPS.
    
    Args:
        state_identifier: State name ('California'), abbreviation ('CA'), or FIPS ('06')
        
    Returns:
        Two-digit FIPS code or None if not found
    """
    downloader = CensusBoundaryDownloader()
    return downloader._get_state_fips(state_identifier)


def get_state_info(state_identifier: str) -> Optional[Dict[str, str]]:
    """
    Get comprehensive state information.
    
    Args:
        state_identifier: State name, abbreviation, or FIPS code
        
    Returns:
        Dictionary with 'fips', 'name', 'abbrev' or None if not found
    """
    downloader = CensusBoundaryDownloader()
    return downloader.get_state_info(state_identifier)


def list_all_states_and_territories() -> List[Dict[str, str]]:
    """
    Get complete list of all US states and territories.
    
    Returns:
        List of dictionaries with FIPS, name, abbreviation, and type
    """
    downloader = CensusBoundaryDownloader()
    return downloader.list_states_and_territories()


def get_states_by_type(state_type: str = 'State') -> List[Dict[str, str]]:
    """
    Get states/territories filtered by type.
    
    Args:
        state_type: 'State', 'Territory', or 'District'
        
    Returns:
        Filtered list of states/territories
    """
    all_states = list_all_states_and_territories()
    return [s for s in all_states if s['type'] == state_type]


    def get_available_state_fips(self) -> Dict[str, Dict[str, str]]:
        """Get comprehensive mapping of FIPS codes to state/territory info."""
        return FIPS_DATA.copy()
    
    def get_state_info(self, identifier: str) -> Optional[Dict[str, str]]:
        """Get state/territory information by FIPS, name, or abbreviation."""
        fips = self._get_state_fips(identifier)
        if fips and fips in FIPS_DATA:
            return {
                'fips': fips,
                'name': FIPS_DATA[fips]['name'],
                'abbrev': FIPS_DATA[fips]['abbrev']
            }
        return None
    
    def list_states_and_territories(self) -> List[Dict[str, str]]:
        """Get list of all states and territories with their info."""
        return [
            {
                'fips': fips,
                'name': data['name'],
                'abbrev': data['abbrev'],
                'type': 'Territory' if fips in ['60', '66', '69', '72', '78'] else 
                       'District' if fips == '11' else 'State'
            }
            for fips, data in FIPS_DATA.items()
        ]

    def _get_url_pattern_for_year(self, year: int) -> Optional[Dict[str, Any]]:
        """Get the appropriate URL pattern for a given year."""
        for pattern_name, pattern_info in TIGER_URL_PATTERNS.items():
            if year in pattern_info['years']:
                return pattern_info
        
        log_warning(f"No URL pattern found for year {year}, using modern pattern")
        return TIGER_URL_PATTERNS['modern']
    
    def _get_geography_name_for_year(self, geography: str, year: int) -> Optional[str]:
        """Get the appropriate geography name for a given year."""
        if geography not in GEOGRAPHY_NAME_MAPPINGS:
            # Geography name hasn't changed, use as-is
            return geography
        
        mappings = GEOGRAPHY_NAME_MAPPINGS[geography]
        for year_range, geo_name in mappings.items():
            if year in year_range:
                return geo_name
        
        # Default to the input geography name
        return geography
    
    def get_available_years_for_geography(self, geography: str) -> List[int]:
        """Get list of years available for a specific geography."""
        available = []
        
        for year in self.available_years:
            geo_name = self._get_geography_name_for_year(geography, year)
            if geo_name:  # None means not available
                available.append(year)
        
        return available
    
    def validate_year_geography_combination(self, geography: str, year: int) -> bool:
        """Check if a geography is available for a specific year."""
        if year not in self.available_years:
            return False
        
        geo_name = self._get_geography_name_for_year(geography, year)
        return geo_name is not None
    
    def get_recommended_year_for_geography(self, geography: str, preferred_year: Optional[int] = None) -> int:
        """Get the best available year for a geography."""
        available_years = self.get_available_years_for_geography(geography)
        
        if not available_years:
            # Fallback to most recent
            return max(self.available_years)
        
        if preferred_year and preferred_year in available_years:
            return preferred_year
        
        # Return most recent available year
        return max(available_years)
    
    def list_url_patterns_by_year(self) -> Dict[str, List[int]]:
        """Get mapping of URL patterns to the years they support."""
        return {
            pattern_name: list(pattern_info['years'])
            for pattern_name, pattern_info in TIGER_URL_PATTERNS.items()
        }