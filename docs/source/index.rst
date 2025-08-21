Welcome to Siege Utilities documentation!
========================================

Siege Utilities is a comprehensive Python utilities package with **enhanced auto-discovery** that automatically imports and makes all functions mutually available across modules.

.. toctree::
   :maxdepth: 2
   :caption: Getting Started:

   getting_started
   architecture_diagram
   autodiscovery

.. toctree::
   :maxdepth: 2
   :caption: Core Utilities:

   core_utilities
   string_utilities
   logging_utilities

.. toctree::
   :maxdepth: 2
   :caption: Distributed Computing:

   distributed_computing
   hdfs_operations
   spark_utilities

.. toctree::
   :maxdepth: 2
   :caption: File Operations:

   file_operations
   file_hashing
   remote_operations
   shell_operations

.. toctree::
   :maxdepth: 2
   :caption: Geographic & Analytics:

   geo
   analytics
   mapping_and_reporting

.. toctree::
   :maxdepth: 2
   :caption: Development & Testing:

   testing_guide
   api/index

.. note::

   **Recently Restored**: Bivariate Choropleth Functionality
   
   - 🗺️ **Bivariate Mapping**: Fully restored bivariate choropleth with proper 2D color schemes
   - 🎨 **Two-dimensional Colors**: Quantile-based binning with color matrix legends  
   - 📊 **Professional Output**: High-quality PNG generation with basemap support
   - 🔧 **Import Fixes**: Resolved all import chain failures across the library
   - 📝 **Logging Integration**: Standardized logging across all modules
   
   **Also Available**: Client and Connection Configuration Management
   
   - 👥 **Client Profiles**: Manage client information, contact details, and design artifacts
   - 🔌 **Connection Persistence**: Notebook, Spark, and database connection management
   - 🔗 **Project Association**: Link clients with projects for better organization
   - 🧪 **Comprehensive Testing**: Full test suite with coverage reporting

   See :doc:`testing_guide` for complete testing information.
   
   The bivariate choropleth system now works as intended, matching the
   reference implementation at https://github.com/mikhailsirenko/bivariate-choropleth
   with full geographic data support and professional visualization output.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`