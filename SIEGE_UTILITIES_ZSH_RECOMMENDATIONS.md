# Siege Utilities ZSH Configuration Recommendations

**For macOS with ZSH using your modular configuration system**

Based on your excellent modular zshrc setup at https://github.com/dheerajchand/zshrc_backups, here are specific recommendations for optimal Siege Utilities library usage:

## 1. Create Siege Utilities Module

Create a new module: `~/.config/zsh/siege-utilities.zsh`

```bash
# =====================================================
# SIEGE UTILITIES CONFIGURATION
# =====================================================

# Check if Siege Utilities is available
if command -v python3 &>/dev/null && python3 -c "import siege_utilities" &>/dev/null 2>&1; then
    export SIEGE_UTILITIES_AVAILABLE=1
    
    # Siege Utilities environment variables
    export SIEGE_UTILITIES_HOME="${UTILITIES:-$HOME/siege_utilities}"
    export SIEGE_DATA_DIR="$HOME/siege_data"
    export SIEGE_CACHE_DIR="$HOME/.siege_cache"
    export SIEGE_CONFIG_DIR="$HOME/.siege_utilities"
    
    # Geospatial data paths
    export CENSUS_DATA_CACHE="$SIEGE_CACHE_DIR/census"
    export SPATIAL_DATA_CACHE="$SIEGE_CACHE_DIR/spatial"
    export BIVARIATE_OUTPUT_DIR="$SIEGE_DATA_DIR/bivariate_maps"
    
    # Create necessary directories
    mkdir -p "$SIEGE_DATA_DIR" "$SIEGE_CACHE_DIR" "$CENSUS_DATA_CACHE" "$SPATIAL_DATA_CACHE" "$BIVARIATE_OUTPUT_DIR"
    
    # Python path for development
    [[ -d "$SIEGE_UTILITIES_HOME" ]] && export PYTHONPATH="$SIEGE_UTILITIES_HOME:$PYTHONPATH"
    
    # Siege Utilities convenience functions
    function siege_status() {
        echo "🚀 Siege Utilities Status"
        echo "========================="
        python3 -c "
        try:
            import siege_utilities as su
            print(f'✅ Library loaded: {len(dir(su))} items')
            print(f'📊 Package info available: {hasattr(su, \"get_package_info\")}')
            
            # Test bivariate choropleth
            from siege_utilities.reporting.chart_generator import ChartGenerator
            chart_gen = ChartGenerator()
            print('✅ Bivariate choropleth: Available')
            
            # Test logging
            su.log_info('Siege Utilities status check complete')
            print('✅ Logging system: Functional')
            
        except ImportError as e:
            print(f'❌ Import error: {e}')
        except Exception as e:
            print(f'⚠️  Warning: {e}')
        "
    }
    
    function siege_test() {
        echo "🧪 Running Siege Utilities Tests"
        echo "================================="
        
        # Run bivariate choropleth test
        if [[ -f "$SIEGE_UTILITIES_HOME/siege_utilities/reporting/test_bivariate_choropleth.py" ]]; then
            echo "📊 Testing bivariate choropleth functionality..."
            python3 "$SIEGE_UTILITIES_HOME/siege_utilities/reporting/test_bivariate_choropleth.py"
        fi
        
        # Run library import test
        echo "📦 Testing library import..."
        python3 -c "import siege_utilities; print(f'✅ Successfully loaded {len(dir(siege_utilities))} items')"
    }
    
    function siege_bivariate() {
        echo "🗺️ Creating sample bivariate choropleth..."
        python3 -c "
        try:
            from siege_utilities.reporting.chart_generator import ChartGenerator
            import pandas as pd
            import numpy as np
            
            print('✅ Bivariate choropleth functionality available')
            print('💡 Use ChartGenerator.create_bivariate_choropleth() with your GeoDataFrame')
            print('📊 Features: 2D color schemes, quantile binning, integrated legends')
            
        except Exception as e:
            print(f'❌ Error: {e}')
        "
    }
    
    function siege_clean_cache() {
        echo "🧹 Cleaning Siege Utilities cache..."
        [[ -d "$SIEGE_CACHE_DIR" ]] && rm -rf "$SIEGE_CACHE_DIR"/* && echo "✅ Cache cleaned"
        mkdir -p "$CENSUS_DATA_CACHE" "$SPATIAL_DATA_CACHE"
    }
    
    function siege_env() {
        echo "🌍 Siege Utilities Environment"
        echo "=============================="
        echo "SIEGE_UTILITIES_HOME: $SIEGE_UTILITIES_HOME"
        echo "SIEGE_DATA_DIR: $SIEGE_DATA_DIR" 
        echo "SIEGE_CACHE_DIR: $SIEGE_CACHE_DIR"
        echo "CENSUS_DATA_CACHE: $CENSUS_DATA_CACHE"
        echo "BIVARIATE_OUTPUT_DIR: $BIVARIATE_OUTPUT_DIR"
        echo ""
        echo "🔧 Available commands:"
        echo "   siege_status     - Check library status"
        echo "   siege_test       - Run functionality tests"
        echo "   siege_bivariate  - Test bivariate choropleth"
        echo "   siege_clean_cache - Clean data cache"
        echo "   siege_notebook   - Launch Jupyter for Siege work"
    }
    
    function siege_notebook() {
        local notebook_dir="${1:-$SIEGE_DATA_DIR}"
        echo "📓 Starting Jupyter notebook in: $notebook_dir"
        cd "$notebook_dir" && jupyter notebook
    }
    
    # Aliases for convenience
    alias siege='siege_env'
    alias siege-status='siege_status'
    alias siege-test='siege_test'
    alias siege-clean='siege_clean_cache'
    
else
    export SIEGE_UTILITIES_AVAILABLE=0
    
    function siege_install() {
        echo "📦 Installing Siege Utilities Dependencies"
        echo "========================================="
        echo "Installing core geospatial stack..."
        
        # Check if conda/mamba available
        if command -v mamba &>/dev/null; then
            echo "Using mamba for geospatial installation..."
            mamba install -c conda-forge geopandas shapely fiona pyproj contextily matplotlib seaborn -y
        elif command -v conda &>/dev/null; then
            echo "Using conda for geospatial installation..."
            conda install -c conda-forge geopandas shapely fiona pyproj contextily matplotlib seaborn -y
        else
            echo "Using pip for installation..."
            pip install geopandas>=1.1.1 shapely>=2.1.1 fiona>=1.10.1 pyproj>=3.7.2
            pip install matplotlib seaborn contextily pandas numpy
        fi
        
        echo "✅ Installation complete. Restart shell and run 'siege_status'"
    }
    
    alias siege='echo "❌ Siege Utilities not available. Run siege_install to set up."'
fi

# =====================================================
# GEOSPATIAL DEVELOPMENT ALIASES
# =====================================================

# Quick development aliases
alias geopandas-test='python3 -c "import geopandas; print(f\"GeoPandas {geopandas.__version__} available\")"'
alias matplotlib-test='python3 -c "import matplotlib; print(f\"Matplotlib {matplotlib.__version__} available\")"'
alias spatial-deps='python3 -c "import geopandas, shapely, fiona, pyproj, contextily; print(\"✅ All spatial dependencies available\")"'

# Data science workflow aliases
alias jupyter-siege='cd "${SIEGE_DATA_DIR:-$HOME/siege_data}" && jupyter lab'
alias python-siege='cd "${SIEGE_UTILITIES_HOME:-$HOME/siege_utilities}" && python3'

# Quick census data commands
function quick_census_test() {
    python3 -c "
    try:
        from siege_utilities.geo import get_census_data_selector
        selector = get_census_data_selector()
        print('✅ Census data utilities available')
    except Exception as e:
        print(f'❌ Census utilities error: {e}')
    "
}

# =====================================================
# MACOS SPECIFIC OPTIMIZATIONS
# =====================================================

# macOS specific environment for geospatial libraries
if [[ "$OSTYPE" == "darwin"* ]]; then
    # Homebrew GDAL/PROJ paths
    export GDAL_DATA="$(brew --prefix)/share/gdal"
    export PROJ_LIB="$(brew --prefix)/share/proj"
    
    # Ensure spatial libraries can find dependencies
    export DYLD_LIBRARY_PATH="$(brew --prefix)/lib:$DYLD_LIBRARY_PATH"
    
    # Matplotlib backend for macOS
    export MPLBACKEND="MacOSX"
fi

# Performance settings for large datasets
export OMP_NUM_THREADS=4  # Optimize for typical Mac CPU cores
export GDAL_CACHEMAX=512  # 512MB cache for GDAL operations

# =====================================================
# INTEGRATION WITH YOUR EXISTING MODULES
# =====================================================

# Add siege utilities help to your existing help system
function siege_help() {
    echo "🚀 Siege Utilities ZSH Integration"
    echo "=================================="
    echo ""
    echo "📊 Library Status:"
    [[ $SIEGE_UTILITIES_AVAILABLE -eq 1 ]] && echo "   ✅ Siege Utilities: Available" || echo "   ❌ Siege Utilities: Not installed"
    echo ""
    echo "🔧 Commands:"
    echo "   siege / siege_env       - Show environment and available commands"
    echo "   siege_status            - Check library and functionality status"
    echo "   siege_test              - Run comprehensive functionality tests"
    echo "   siege_bivariate         - Test bivariate choropleth functionality"
    echo "   siege_clean_cache       - Clean data cache directories"
    echo "   siege_notebook          - Launch Jupyter in siege data directory"
    [[ $SIEGE_UTILITIES_AVAILABLE -eq 0 ]] && echo "   siege_install           - Install required dependencies"
    echo ""
    echo "🗺️ Geospatial:"
    echo "   geopandas-test          - Test GeoPandas installation"
    echo "   matplotlib-test         - Test Matplotlib installation"
    echo "   spatial-deps            - Test all spatial dependencies"
    echo "   quick_census_test       - Test Census data utilities"
    echo ""
    echo "🚀 Development:"
    echo "   jupyter-siege           - Launch Jupyter Lab in data directory"
    echo "   python-siege            - Launch Python in utilities directory"
}

# Export key functions for use in other modules
export -f siege_status siege_test siege_bivariate siege_clean_cache siege_env siege_help
