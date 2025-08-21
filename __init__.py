"""
utilities package initialization.
This file enables `from utilities import *` to import all functions from all modules.
"""

import os
import importlib
import inspect
import sys

# List to track exposed names
__all__ = []

# Get the directory of this package
package_dir = os.path.dirname(__file__)


# Function to import all public functions from a module
def import_all_from_module(module_path):
    try:
        module = importlib.import_module(module_path)
        imported_names = []

        # Import all public functions and classes from the module
        for name, obj in inspect.getmembers(module):
            if not name.startswith("_"):
                # Import functions
                if inspect.isfunction(obj):
                    globals()[name] = obj
                    imported_names.append(name)
                # Import classes that are defined in this module (not imported from elsewhere)
                elif inspect.isclass(obj) and hasattr(obj, '__module__') and obj.__module__ == module.__name__:
                    globals()[name] = obj
                    imported_names.append(name)

        return imported_names
    except ImportError as e:
        if "No module named" in str(e):
            # Use basic print for package initialization (before logging is available)
            print(f"Warning: Optional dependency missing for {module_path}: {e}")
        else:
            print(f"Warning: Import error from {module_path}: {e}")
        return []
    except Exception as e:
        # Use basic print for package initialization (before logging is available)
        print(f"Warning: Error importing from {module_path}: {e}")
        return []


# Step 1: Import from all direct .py modules in this package
for filename in os.listdir(package_dir):
    if filename.endswith(".py") and filename != "__init__.py":
        module_name = filename[:-3]  # Remove .py extension
        module_path = f"{__name__}.{module_name}"
        new_names = import_all_from_module(module_path)
        __all__.extend(new_names)

# Step 2: Import from all subpackages
for item in os.listdir(package_dir):
    item_path = os.path.join(package_dir, item)
    init_path = os.path.join(item_path, "__init__.py")

    # Check if it's a directory and has an __init__.py file (making it a subpackage)
    if (
        os.path.isdir(item_path)
        and os.path.exists(init_path)
        and not item.startswith("__")
    ):
        subpackage_path = f"{__name__}.{item}"

        # First, import the subpackage itself
        subpackage = importlib.import_module(subpackage_path)
        globals()[item] = subpackage
        __all__.append(item)

        # Then, find all modules in the subpackage
        for subfile in os.listdir(item_path):
            if subfile.endswith(".py") and subfile != "__init__.py":
                submodule_name = subfile[:-3]
                submodule_path = f"{subpackage_path}.{submodule_name}"
                new_names = import_all_from_module(submodule_path)
                __all__.extend(new_names)

# Package initialization complete - use print here as logging may not be set up yet
print(f"utilities package: Imported {len(__all__)} functions and subpackages")
