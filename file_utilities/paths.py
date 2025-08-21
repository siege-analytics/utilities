# Python stdlib

import io
import hashlib
from pyexpat.errors import messages

# ============================================================================
# STANDARDIZED LOGGING IMPORT
# ============================================================================
try:
    from utilities.logging_utils import setup_module_logging
    setup_module_logging(globals(), __name__)
except ImportError:
    # Handle case where package has different name
    import sys
    package_name = __name__.split('.')[0]
    try:
        logging_module = sys.modules[f"{package_name}.logging_utils"]
        setup_func = getattr(logging_module, 'setup_module_logging', None)
        if setup_func:
            setup_func(globals(), __name__)
        else:
            # Individual function import fallback
            log_info = getattr(logging_module, 'log_info', lambda x: print(f"INFO: {x}"))
            log_debug = getattr(logging_module, 'log_debug', lambda x: print(f"DEBUG: {x}"))
            log_warning = getattr(logging_module, 'log_warning', lambda x: print(f"WARNING: {x}"))
            log_error = getattr(logging_module, 'log_error', lambda x: print(f"ERROR: {x}"))
            log_critical = getattr(logging_module, 'log_critical', lambda x: print(f"CRITICAL: {x}"))
    except:
        # Fallback functions
        def log_info(msg): print(f"INFO: {msg}")
        def log_debug(msg): print(f"DEBUG: {msg}")
        def log_warning(msg): print(f"WARNING: {msg}")
        def log_error(msg): print(f"ERROR: {msg}")
        def log_critical(msg): print(f"CRITICAL: {msg}")

# Make IPython import optional
try:
    from IPython.utils.capture import capture_output
except ImportError:
    capture_output = None
import pathlib
import requests
import subprocess

from pkg_resources import working_set
from tqdm import tqdm

import zipfile

# Logging

import logging

# Handle dynamic package imports
try:
    from utilities.logging_utils import *
    from utilities.file_utilities.file_attributes import *
except ImportError:
    # Handle case where package has different name
    import sys
    package_name = __name__.split('.')[0]
    try:
        logging_module = sys.modules[f"{package_name}.logging_utils"]
        for attr in ['log_info', 'log_debug', 'log_warning', 'log_error', 'log_critical']:
            globals()[attr] = getattr(logging_module, attr, lambda x: print(f"{attr.upper().split('_')[1]}: {x}"))
        
        # Import from file_attributes in same package
        from .file_attributes import *
    except Exception as e:
        # Fallback functions
        def log_info(msg): print(f"INFO: {msg}")
        def log_error(msg): print(f"ERROR: {msg}")
        def log_debug(msg): print(f"DEBUG: {msg}")
        def log_warning(msg): print(f"WARNING: {msg}")
        def log_critical(msg): print(f"CRITICAL: {msg}")

# Using standardized logging system


def ensure_path_exists(desired_path: pathlib.Path) -> pathlib.Path:

    try:
        desired_path_object = pathlib.Path(desired_path)
        result = pathlib.Path(desired_path_object).mkdir(parents=True, exist_ok=True)
        message = f"Generated a path at {str(desired_path_object)}: {result}"
        log_info(message=message)
        gitkeep_file = desired_path_object / ".gitkeep"
        delete_existing_file_and_replace_it_with_an_empty_file(gitkeep_file)
        log_info(message=message)

        return desired_path_object

    except Exception as e:
        message = f"Exception while generating local path: {e}"
        log_error(message=message)
        return False


def unzip_file_to_its_own_directory(
    path_to_zipfile: pathlib.Path, new_dir_name=None, new_dir_parent=None
):
    try:
        path_to_zipfile = pathlib.Path(path_to_zipfile)
        frtz = zipfile.ZipFile(path_to_zipfile)
        if new_dir_name is None:
            new_dir_name = path_to_zipfile.stem
        if new_dir_parent is None:
            new_dir_parent = path_to_zipfile.parent

        # ensure that a directory exists for the new files to go in
        target_dir_for_unzipped_files = new_dir_parent / new_dir_name

        pathlib.Path(target_dir_for_unzipped_files).mkdir(parents=True, exist_ok=True)

        frtz.extractall(path=target_dir_for_unzipped_files)
        message = f"Just unzipped: \n {path_to_zipfile} \n To: {target_dir_for_unzipped_files}"
        log_info(message=message)
        return target_dir_for_unzipped_files

    except Exception as e:

        message = f"There was an error: {e}"
        log_error(message=message)
        return False
