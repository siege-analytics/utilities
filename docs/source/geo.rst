Geographic Utilities
====================

The geographic utilities package provides comprehensive tools for working with geographic data, including enhanced Census utilities, intelligent data selection, spatial analysis capabilities, and fully restored bivariate choropleth mapping functionality.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   geocoding
   api/siege_utilities/geo/spatial_data
   api/siege_utilities/geo/census_dataset_mapper
   api/siege_utilities/geo/census_data_selector

Overview
--------

The geographic utilities package offers a complete solution for geographic data analysis:

* **Enhanced Census Utilities**: Dynamic discovery and download of Census TIGER/Line boundaries
* **Intelligent Data Selection**: Automatic recommendation of the best Census datasets for your analysis needs
* **Spatial Data Processing**: Comprehensive tools for working with geographic boundaries and spatial data
* **Bivariate Choropleth Mapping**: Fully restored two-dimensional mapping with proper color schemes and legends
* **Geocoding Services**: Address geocoding and reverse geocoding capabilities
* **Data Integration**: Seamless integration with external data sources and analytics platforms
* **Import Chain Fixes**: All import issues resolved, ensuring reliable module loading

Key Features
-----------

Census Data Intelligence
~~~~~~~~~~~~~~~~~~~~~~~

The new Census Data Intelligence system makes Census data human-comprehensible by:

* **Automatic Dataset Selection**: Intelligently recommends the best Census datasets based on your analysis type, geography level, and time requirements
* **Relationship Mapping**: Maps relationships between different Census surveys (Decennial, ACS 1-year/5-year, Economic Census, Population Estimates)
* **Quality Guidance**: Provides methodology notes, quality checks, and reporting considerations
* **Pitfall Prevention**: Helps avoid common mistakes like using incompatible datasets or ignoring margins of error

**Example Usage**:

.. code-block:: python

   from siege_utilities.geo import select_census_datasets
   
   # Get recommendations for demographic analysis at tract level
   recommendations = select_census_datasets(
       analysis_type="demographics",
       geography_level="tract",
       variables=["population", "income", "education"]
   )
   
   # System automatically recommends ACS 5-Year Estimates (2020)
   # because it provides stable, detailed data at tract level
   primary_dataset = recommendations["primary_recommendation"]["dataset"]
   print(f"Use {primary_dataset} for your analysis")

Enhanced Census Utilities
~~~~~~~~~~~~~~~~~~~~~~~~

* **Dynamic Discovery**: Automatically discovers available Census years and boundary types
* **SSL Fallback**: Robust handling of network issues with automatic fallback mechanisms
* **Comprehensive State Information**: Complete FIPS codes, names, and abbreviations for all states
* **Multiple Geography Levels**: Support for counties, tracts, block groups, and more
* **Parameter Validation**: Robust validation of input parameters with helpful error messages

Spatial Data Processing
~~~~~~~~~~~~~~~~~~~~~~~

* **Format Conversion**: Convert between GeoJSON, Shapefile, and other spatial formats
* **Coordinate System Transformation**: Transform data between different coordinate reference systems
* **Database Integration**: Connect to PostGIS and other spatial databases
* **Optional DuckDB Support**: Lightweight spatial operations with optional DuckDB integration

Geocoding Services
~~~~~~~~~~~~~~~~~

* **Address Geocoding**: Convert addresses to geographic coordinates
* **Reverse Geocoding**: Convert coordinates to addresses
* **Batch Processing**: Process multiple addresses efficiently
* **Multiple Providers**: Support for various geocoding services

Installation
-----------

Install the geographic utilities with full support:

.. code-block:: bash

   # Core geospatial dependencies (now properly resolved)
   pip install geopandas>=1.1.1 shapely>=2.1.1 fiona>=1.10.1
   pip install matplotlib seaborn contextily
   
   # For bivariate choropleth functionality
   pip install pyproj>=3.7.2
   
   # Optional enhancements
   pip install folium>=0.20.0  # interactive maps
   pip install reportlab>=4.4.3  # PDF reports

**Note**: All import chain failures have been resolved. The package now loads
reliably without circular dependency issues.

Quick Start
----------

1. **Create Bivariate Choropleth Maps** (Restored Functionality):

   .. code-block:: python

      from siege_utilities.reporting.chart_generator import ChartGenerator
      import geopandas as gpd
      
      # Initialize chart generator
      chart_gen = ChartGenerator()
      
      # Load geographic data with your variables
      gdf = gpd.read_file('your_data.shp')  # or Census boundaries
      
      # Create bivariate choropleth with proper 2D color scheme
      fig = chart_gen.create_bivariate_choropleth(
          geodata=gdf,
          variable1='population',
          variable2='income',
          variable1_name='Population',
          variable2_name='Income',
          title="Population vs Income Analysis",
          output_path='bivariate_map.png'
      )

2. **Get Census Data Intelligence**:

   .. code-block:: python

      from siege_utilities.geo import get_census_data_selector
      
      # Now imports without circular dependency issues
      selector = get_census_data_selector()
      
      # Get dataset recommendations
      recommendations = selector.select_datasets_for_analysis(
          "demographics", "tract"
      )

3. **Use Intelligent Data Selection**:

   .. code-block:: python

      from siege_utilities.geo import quick_census_selection
      
      # Quick selection for business analysis  
      result = quick_census_selection("business", "county")
      print(f"Use {result['recommendations']['primary_recommendation']['dataset']}")

Analysis Types Supported
-----------------------

The intelligent data selection system recognizes these analysis types:

* **demographics** - Population, age, race, ethnicity, income, education
* **housing** - Housing units, value, rent, tenure, vacancy
* **business** - Business counts, employment, industry, payroll
* **transportation** - Commute time, transportation mode, vehicle availability
* **education** - Education level, school enrollment, field of study
* **health** - Health insurance, disability status, veteran status
* **poverty** - Poverty status, public assistance, income

Geography Levels Supported
-------------------------

* **nation** - Country-level data
* **state** - State-level data
* **county** - County-level data
* **tract** - Census tract (neighborhood-level)
* **block_group** - Block group (sub-neighborhood)
* **block** - Census block (smallest unit)
* **place** - City/town data
* **zip_code** - ZIP code areas
* **cbsa** - Metropolitan areas

Census Survey Types
-------------------

* **Decennial Census** - Complete count every 10 years (highest reliability)
* **ACS 5-Year Estimates** - 5-year rolling average (stable, detailed data)
* **ACS 1-Year Estimates** - Single year estimates (recent, large areas only)
* **Economic Census** - Business establishment counts every 5 years
* **Population Estimates** - Annual estimates between decennial censuses

Data Quality and Reliability
---------------------------

* **HIGH** - Decennial Census, Economic Census (100% counts)
* **MEDIUM** - ACS 5-year estimates (sample-based with margins of error)
* **LOW** - ACS 1-year estimates (higher margins of error)
* **ESTIMATED** - Population estimates (modeled from administrative records)

Best Practices
-------------

* Always check margins of error for ACS estimates
* Use consistent survey types for comparisons
* Consider geography limitations when selecting data
* Validate data against known benchmarks
* Document your data sources and methodology
* Use the intelligent selector to avoid common pitfalls

Examples
--------

See the `examples/ <../examples.html>`_ directory for working examples:

* **census_intelligence_demo.py** - Complete demonstration of the Census Data Intelligence system
* **enhanced_features_demo.py** - Examples of enhanced Census utilities

For detailed API documentation, see :doc:`api/siege_utilities/geo/spatial_data`.

Recent Fixes and Improvements
----------------------------

**Import Chain Resolution**

All import issues have been resolved:

* Fixed ``siege_utilities.geo.__init__.py`` to only import existing functions
* Corrected class name references (``SpatialTransformer`` → ``SpatialDataTransformer``)
* Removed non-existent imports (``Geocoder`` class, ``get_geocoder`` function)
* Used string type hints to avoid import-time errors with PySpark

**Bivariate Choropleth Restoration**

The bivariate choropleth functionality has been completely restored:

* **Proper Implementation**: Now matches reference implementation from https://github.com/mikhailsirenko/bivariate-choropleth
* **Two-dimensional Color Schemes**: Quantile-based binning with proper color mixing
* **Integrated Legends**: Color matrix showing variable ranges and combinations
* **Geographic Data Support**: Works with GeoDataFrames and shapefiles
* **Basemap Integration**: Optional background maps for context
* **Professional Output**: High-quality PNG generation

**Logging Standardization**

All geographic modules now use the integrated logging system:

* Consistent logging patterns across all modules
* Proper error handling and user feedback
* Integration with the main package logging framework

**Testing and Reliability**

The package now loads reliably:

* All circular dependencies resolved
* Import errors eliminated
* Cross-module function availability maintained
* Comprehensive test coverage for critical functionality

For the latest examples and usage patterns, see the restored bivariate choropleth
examples in ``siege_utilities/reporting/examples/``.
