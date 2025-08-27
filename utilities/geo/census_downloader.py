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

# Census TIGER/Line base URLs
CENSUS_TIGER_BASE = "https://www2.census.gov/geo/tiger"
CENSUS_API_BASE = "https://api.census.gov/data"

# State FIPS codes mapping
STATE_FIPS = {
    'Alabama': '01', 'Alaska': '02', 'Arizona': '04', 'Arkansas': '05',
    'California': '06', 'Colorado': '08', 'Connecticut': '09', 'Delaware': '10',
    'District of Columbia': '11', 'Florida': '12', 'Georgia': '13', 'Hawaii': '15',
    'Idaho': '16', 'Illinois': '17', 'Indiana': '18', 'Iowa': '19',
    'Kansas': '20', 'Kentucky': '21', 'Louisiana': '22', 'Maine': '23',
    'Maryland': '24', 'Massachusetts': '25', 'Michigan': '26', 'Minnesota': '27',
    'Mississippi': '28', 'Missouri': '29', 'Montana': '30', 'Nebraska': '31',
    'Nevada': '32', 'New Hampshire': '33', 'New Jersey': '34', 'New Mexico': '35',
    'New York': '36', 'North Carolina': '37', 'North Dakota': '38', 'Ohio': '39',
    'Oklahoma': '40', 'Oregon': '41', 'Pennsylvania': '42', 'Rhode Island': '44',
    'South Carolina': '45', 'South Dakota': '46', 'Tennessee': '47', 'Texas': '48',
    'Utah': '49', 'Vermont': '50', 'Virginia': '51', 'Washington': '53',
    'West Virginia': '54', 'Wisconsin': '55', 'Wyoming': '56'
}

# Reverse mapping
FIPS_TO_STATE = {v: k for k, v in STATE_FIPS.items()}

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
        
        # Available years (commonly working years)
        self.available_years = [2020, 2021, 2022, 2023]
        
        log_info(f"Initialized Census boundary downloader, data dir: {self.download_dir}")
    
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
        log_warning(f"Year {year} not available, using closest: {closest}")
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
        
        # Already a FIPS code
        if state.isdigit() and len(state) == 2:
            return state if state in FIPS_TO_STATE else None
        
        # State name
        if state.title() in STATE_FIPS:
            return STATE_FIPS[state.title()]
        
        # Try state abbreviations (common ones)
        abbrev_mapping = {
            'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06', 'CO': '08',
            'CT': '09', 'DE': '10', 'DC': '11', 'FL': '12', 'GA': '13', 'HI': '15',
            'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19', 'KS': '20', 'KY': '21',
            'LA': '22', 'ME': '23', 'MD': '24', 'MA': '25', 'MI': '26', 'MN': '27',
            'MS': '28', 'MO': '29', 'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33',
            'NJ': '34', 'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39',
            'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44', 'SC': '45', 'SD': '46',
            'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50', 'VA': '51', 'WA': '53',
            'WV': '54', 'WI': '55', 'WY': '56'
        }
        
        return abbrev_mapping.get(state.upper())
    
    def _download_national_geography(self, geography: str, year: int, resolution: str) -> Optional[str]:
        """Download national-level geography (states, counties, etc.)."""
        # Construct Census TIGER URL
        url = f"{CENSUS_TIGER_BASE}/GENZ{year}/shp/cb_{year}_us_{geography}_{resolution}.zip"
        
        return self._download_and_extract(url, f"{geography}_{year}_{resolution}")
    
    def _download_state_geographies(self, geography: str, year: int, states: List[str], resolution: str) -> Optional[str]:
        """Download state-level geographies (tracts, block groups, etc.)."""
        downloaded_files = []
        
        for state_fips in states:
            state_name = FIPS_TO_STATE.get(state_fips, state_fips)
            log_info(f"Downloading {geography} for {state_name} ({state_fips})")
            
            # Construct state-specific URL
            url = f"{CENSUS_TIGER_BASE}/GENZ{year}/shp/cb_{year}_{state_fips}_{geography}_{resolution}.zip"
            
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
