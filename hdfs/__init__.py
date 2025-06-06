"""
hdfs package initialization.
This package contains distributed computing and HDFS-related utility functions.
"""

import os
import importlib
import inspect

# List to track exposed names
__all__ = []

# Get the directory of this package
package_dir = os.path.dirname(__file__)

# Import all modules directly in this package
for filename in os.listdir(package_dir):
    if filename.endswith(".py") and filename != "__init__.py":
        module_name = filename[:-3]  # Remove .py
        full_module_name = f"{__name__}.{module_name}"

        try:
            # Import the module
            module = importlib.import_module(full_module_name)

            # Expose all public functions and classes from the module
            for name, obj in inspect.getmembers(module):
                if (
                    inspect.isfunction(obj) or inspect.isclass(obj)
                ) and not name.startswith("_"):
                    globals()[name] = obj
                    __all__.append(name)

        except Exception as e:
            print(f"Warning: Error importing {module_name}: {e}")


# Package-level convenience function
def quick_setup(data_path: str, app_name: str = "SparkApp", **kwargs):
    """Ultra-quick setup for simple use cases"""
    config = create_hdfs_config(data_path=data_path, app_name=app_name, **kwargs)
    return setup_distributed_environment(config)


# Add to __all__
__all__.append("quick_setup")

# Print summary like your file_utilities pattern
print(f"hdfs package: Imported {len(__all__)} functions and classes")
