"""
Fixed Bivariate Choropleth Mapping

Provides functions to create bivariate choropleth maps showing two variables
simultaneously using a 2D color scheme. Includes proper legend generation
and fallback mechanisms for missing dependencies.
"""

import logging
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any
import tempfile
import os

# Optional dependencies with fallbacks
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.colors import ListedColormap
    import matplotlib.colors as mcolors
except ImportError:
    plt = None
    mpatches = None
    ListedColormap = None
    mcolors = None

try:
    import geopandas as gpd
except ImportError:
    gpd = None

try:
    import seaborn as sns
except ImportError:
    sns = None

# Import utilities
from utilities.logging_utils import log_info, log_error, log_warning

# Get logger
log = logging.getLogger(__name__)

# Default color schemes for bivariate maps
DEFAULT_BIVARIATE_COLORS = {
    "blue_red": [
        ['#e8e8e8', '#ace4e4', '#5ac8c8'],
        ['#dfb0d6', '#a5add3', '#5698b9'], 
        ['#be64ac', '#8c62aa', '#3b4994']
    ],
    "green_blue": [
        ['#e8e8e8', '#b5c0da', '#6c83b5'],
        ['#b8d6be', '#90b2b3', '#567994'],
        ['#73ae80', '#5a9178', '#2a5a5b']
    ],
    "orange_purple": [
        ['#e8e8e8', '#e4acac', '#c85a5a'],
        ['#b0d5df', '#ad9ea5', '#985356'],
        ['#64acbe', '#627f8c', '#574249']
    ]
}


class BivariateMapper:
    """
    Fixed bivariate choropleth mapper with robust error handling.
    """
    
    def __init__(self):
        self.figure_size = (12, 8)
        self.dpi = 300
        
        # Check required dependencies
        self.matplotlib_available = plt is not None
        self.geopandas_available = gpd is not None
        self.pandas_available = pd is not None
        
        if not self.matplotlib_available:
            log_warning("matplotlib not available - install with: pip install matplotlib")
        if not self.geopandas_available:
            log_warning("geopandas not available - install with: pip install geopandas")
        if not self.pandas_available:
            log_warning("pandas not available - install with: pip install pandas")
    
    def create_bivariate_choropleth(
        self,
        data: Union[str, Any],  # GeoDataFrame or path to shapefile
        var1_column: str,
        var2_column: str,
        id_column: Optional[str] = None,
        title: str = "Bivariate Choropleth Map",
        var1_label: str = "Variable 1",
        var2_label: str = "Variable 2", 
        color_scheme: str = "blue_red",
        bins: int = 3,
        figsize: Tuple[int, int] = (12, 8),
        save_path: Optional[str] = None,
        show_legend: bool = True
    ) -> Optional[str]:
        """
        Create a bivariate choropleth map.
        
        Args:
            data: GeoDataFrame or path to shapefile
            var1_column: Column name for first variable (x-axis in legend)
            var2_column: Column name for second variable (y-axis in legend) 
            id_column: Column name for geographic identifiers
            title: Map title
            var1_label: Label for first variable
            var2_label: Label for second variable
            color_scheme: Color scheme name ('blue_red', 'green_blue', 'orange_purple')
            bins: Number of bins for each variable (2 or 3)
            figsize: Figure size as (width, height)
            save_path: Path to save the map image
            show_legend: Whether to show the bivariate legend
            
        Returns:
            Path to saved image or None if failed
        """
        if not self._check_dependencies():
            return None
        
        try:
            # Load data
            gdf = self._load_geodata(data)
            if gdf is None:
                return None
            
            # Validate columns exist
            missing_cols = []
            if var1_column not in gdf.columns:
                missing_cols.append(var1_column)
            if var2_column not in gdf.columns:
                missing_cols.append(var2_column)
            
            if missing_cols:
                log_error(f"Missing columns in data: {missing_cols}")
                return None
            
            log_info(f"Creating bivariate choropleth: {var1_label} vs {var2_label}")
            
            # Clean data and remove missing values
            gdf = gdf.dropna(subset=[var1_column, var2_column])
            
            if len(gdf) == 0:
                log_error("No valid data remaining after removing missing values")
                return None
            
            # Create bins for both variables
            gdf = self._create_bivariate_bins(gdf, var1_column, var2_column, bins)
            
            # Get colors
            colors = self._get_color_scheme(color_scheme, bins)
            
            # Create map
            fig, ax = plt.subplots(1, 1, figsize=figsize, facecolor='white')
            
            # Plot each bin combination with its color
            for i in range(bins):
                for j in range(bins):
                    mask = (gdf['var1_bin'] == i) & (gdf['var2_bin'] == j)
                    if mask.any():
                        gdf[mask].plot(
                            ax=ax,
                            color=colors[i][j],
                            edgecolor='white',
                            linewidth=0.3
                        )
            
            # Style the map
            ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
            ax.axis('off')
            
            # Add legend if requested
            if show_legend:
                self._add_bivariate_legend(fig, ax, colors, var1_label, var2_label, bins)
            
            # Save or return
            if save_path:
                output_path = save_path
            else:
                output_path = self._generate_temp_filename("bivariate_choropleth", "png")
            
            plt.tight_layout()
            plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight', facecolor='white')
            plt.close()
            
            log_info(f"Bivariate choropleth saved: {output_path}")
            return output_path
            
        except Exception as e:
            log_error(f"Failed to create bivariate choropleth: {e}")
            if 'fig' in locals():
                plt.close(fig)
            return None
    
    def create_simple_choropleth(
        self,
        data: Union[str, Any],
        value_column: str,
        title: str = "Choropleth Map",
        color_scheme: str = "Blues",
        save_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Create a simple single-variable choropleth map.
        
        Args:
            data: GeoDataFrame or path to shapefile
            value_column: Column name for values to map
            title: Map title
            color_scheme: matplotlib colormap name
            save_path: Path to save the map
            
        Returns:
            Path to saved image or None if failed
        """
        if not self._check_dependencies():
            return None
        
        try:
            # Load data
            gdf = self._load_geodata(data)
            if gdf is None:
                return None
            
            # Validate column exists
            if value_column not in gdf.columns:
                log_error(f"Column '{value_column}' not found in data")
                return None
            
            log_info(f"Creating choropleth map for {value_column}")
            
            # Create map
            fig, ax = plt.subplots(1, 1, figsize=self.figure_size, facecolor='white')
            
            # Plot with colormap
            gdf.plot(
                column=value_column,
                ax=ax,
                cmap=color_scheme,
                edgecolor='white',
                linewidth=0.3,
                legend=True,
                legend_kwds={'shrink': 0.8}
            )
            
            # Style
            ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
            ax.axis('off')
            
            # Save
            if save_path:
                output_path = save_path
            else:
                output_path = self._generate_temp_filename("choropleth", "png")
            
            plt.tight_layout()
            plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight', facecolor='white')
            plt.close()
            
            log_info(f"Choropleth saved: {output_path}")
            return output_path
            
        except Exception as e:
            log_error(f"Failed to create choropleth: {e}")
            if 'fig' in locals():
                plt.close(fig)
            return None
    
    def _check_dependencies(self) -> bool:
        """Check if required dependencies are available."""
        if not self.matplotlib_available:
            log_error("matplotlib required for mapping: pip install matplotlib")
            return False
        if not self.geopandas_available:
            log_error("geopandas required for mapping: pip install geopandas")
            return False
        if not self.pandas_available:
            log_error("pandas required for data handling: pip install pandas")
            return False
        return True
    
    def _load_geodata(self, data: Union[str, Any]) -> Optional[Any]:
        """Load geographic data from file path or return GeoDataFrame."""
        if isinstance(data, str):
            # It's a file path
            path = Path(data)
            if path.is_dir():
                # Look for shapefile in directory
                shp_files = list(path.glob("*.shp"))
                if not shp_files:
                    log_error(f"No shapefile found in directory: {path}")
                    return None
                data = str(shp_files[0])
            
            try:
                log_info(f"Loading geographic data from: {data}")
                return gpd.read_file(data)
            except Exception as e:
                log_error(f"Failed to load geographic data: {e}")
                return None
        else:
            # Assume it's already a GeoDataFrame
            if hasattr(data, 'geometry'):
                return data
            else:
                log_error("Data must be a GeoDataFrame or path to shapefile")
                return None
    
    def _create_bivariate_bins(self, gdf: Any, var1_col: str, var2_col: str, bins: int) -> Any:
        """Create bins for both variables."""
        # Create quantile bins for each variable
        gdf['var1_bin'] = pd.qcut(gdf[var1_col], q=bins, labels=False, duplicates='drop')
        gdf['var2_bin'] = pd.qcut(gdf[var2_col], q=bins, labels=False, duplicates='drop')
        
        # Handle any remaining NaN values from qcut
        gdf = gdf.dropna(subset=['var1_bin', 'var2_bin'])
        
        return gdf
    
    def _get_color_scheme(self, scheme_name: str, bins: int) -> List[List[str]]:
        """Get color scheme matrix."""
        if scheme_name in DEFAULT_BIVARIATE_COLORS:
            colors = DEFAULT_BIVARIATE_COLORS[scheme_name]
            
            # Adjust for different bin sizes
            if bins == 2 and len(colors) == 3:
                # Use subset for 2x2
                return [[colors[0][0], colors[0][2]], 
                        [colors[2][0], colors[2][2]]]
            elif bins == 3:
                return colors
        
        # Fallback to simple scheme
        log_warning(f"Unknown color scheme '{scheme_name}', using default")
        return self._create_simple_color_scheme(bins)
    
    def _create_simple_color_scheme(self, bins: int) -> List[List[str]]:
        """Create a simple color scheme."""
        if bins == 2:
            return [['#f7f7f7', '#cccccc'], 
                    ['#969696', '#525252']]
        else:  # bins == 3
            return [['#f7f7f7', '#cccccc', '#969696'],
                    ['#cccccc', '#969696', '#525252'],
                    ['#969696', '#525252', '#252525']]
    
    def _add_bivariate_legend(self, fig: Any, ax: Any, colors: List[List[str]], 
                             var1_label: str, var2_label: str, bins: int) -> None:
        """Add bivariate legend to the map."""
        try:
            # Create legend in bottom right
            legend_ax = fig.add_axes([0.75, 0.02, 0.2, 0.2])
            
            # Create color grid
            for i in range(bins):
                for j in range(bins):
                    rect = mpatches.Rectangle(
                        (j, bins - i - 1), 1, 1,
                        facecolor=colors[i][j],
                        edgecolor='white',
                        linewidth=1
                    )
                    legend_ax.add_patch(rect)
            
            # Style legend
            legend_ax.set_xlim(0, bins)
            legend_ax.set_ylim(0, bins)
            legend_ax.set_aspect('equal')
            
            # Add labels
            legend_ax.set_xlabel(var1_label, fontsize=10)
            legend_ax.set_ylabel(var2_label, fontsize=10)
            
            # Add arrows and "Higher" labels
            legend_ax.annotate('Higher →', xy=(bins/2, -0.3), ha='center', fontsize=9)
            legend_ax.annotate('Higher ↑', xy=(-0.3, bins/2), ha='center', rotation=90, fontsize=9)
            
            # Remove ticks
            legend_ax.set_xticks([])
            legend_ax.set_yticks([])
            
        except Exception as e:
            log_warning(f"Failed to add bivariate legend: {e}")
    
    def _generate_temp_filename(self, prefix: str, extension: str) -> str:
        """Generate a temporary filename."""
        temp_dir = Path(tempfile.gettempdir())
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S") if pd else "output"
        return str(temp_dir / f"{prefix}_{timestamp}.{extension}")
    
    def get_sample_data(self) -> Optional[Any]:
        """Create sample data for testing."""
        if not self.pandas_available or not self.geopandas_available:
            log_error("Sample data requires pandas and geopandas")
            return None
        
        try:
            from shapely.geometry import Polygon
            
            # Create simple polygon geometries
            polygons = []
            data = []
            
            for i in range(10):
                # Create simple rectangular polygons
                x, y = i % 3, i // 3
                poly = Polygon([(x, y), (x+1, y), (x+1, y+1), (x, y+1)])
                polygons.append(poly)
                
                # Add sample data
                data.append({
                    'id': f'region_{i}',
                    'population': np.random.randint(1000, 50000),
                    'income': np.random.randint(30000, 100000),
                    'geometry': poly
                })
            
            gdf = gpd.GeoDataFrame(data)
            log_info(f"Created sample dataset with {len(gdf)} regions")
            return gdf
            
        except Exception as e:
            log_error(f"Failed to create sample data: {e}")
            return None


# Convenience functions
def create_bivariate_map(
    data: Union[str, Any],
    var1_column: str,
    var2_column: str,
    title: str = "Bivariate Choropleth Map",
    var1_label: str = "Variable 1",
    var2_label: str = "Variable 2",
    save_path: Optional[str] = None
) -> Optional[str]:
    """
    Create a bivariate choropleth map with default settings.
    
    Args:
        data: GeoDataFrame or path to shapefile
        var1_column: First variable column name
        var2_column: Second variable column name 
        title: Map title
        var1_label: First variable label
        var2_label: Second variable label
        save_path: Path to save map
        
    Returns:
        Path to saved map or None if failed
    """
    mapper = BivariateMapper()
    return mapper.create_bivariate_choropleth(
        data, var1_column, var2_column, 
        title=title, var1_label=var1_label, var2_label=var2_label,
        save_path=save_path
    )


def create_simple_map(
    data: Union[str, Any],
    value_column: str,
    title: str = "Choropleth Map",
    save_path: Optional[str] = None
) -> Optional[str]:
    """
    Create a simple single-variable choropleth map.
    
    Args:
        data: GeoDataFrame or path to shapefile
        value_column: Column with values to map
        title: Map title
        save_path: Path to save map
        
    Returns:
        Path to saved map or None if failed
    """
    mapper = BivariateMapper()
    return mapper.create_simple_choropleth(data, value_column, title, save_path=save_path)


def test_bivariate_mapping() -> bool:
    """
    Test bivariate mapping with sample data.
    
    Returns:
        True if test successful, False otherwise
    """
    try:
        mapper = BivariateMapper()
        
        # Create sample data
        sample_data = mapper.get_sample_data()
        if sample_data is None:
            log_error("Failed to create sample data for testing")
            return False
        
        # Test bivariate map
        result = mapper.create_bivariate_choropleth(
            sample_data,
            'population',
            'income',
            title="Test Bivariate Map: Population vs Income",
            var1_label="Population",
            var2_label="Income"
        )
        
        if result:
            log_info(f"Test bivariate map created successfully: {result}")
            return True
        else:
            log_error("Failed to create test bivariate map")
            return False
            
    except Exception as e:
        log_error(f"Test failed: {e}")
        return False
