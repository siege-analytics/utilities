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
except ImportError:
    # Handle case where package has different name
    import sys
    package_name = __name__.split('.')[0]
    try:
        logging_module = sys.modules[f"{package_name}.logging_utils"]
        for attr in ['log_info', 'log_debug', 'log_warning', 'log_error', 'log_critical']:
            globals()[attr] = getattr(logging_module, attr, lambda x: print(f"{attr.upper().split('_')[1]}: {x}"))
    except:
        # Fallback functions
        def log_info(msg): print(f"INFO: {msg}")
        def log_error(msg): print(f"ERROR: {msg}")
        def log_debug(msg): print(f"DEBUG: {msg}")
        def log_warning(msg): print(f"WARNING: {msg}")
        def log_critical(msg): print(f"CRITICAL: {msg}")

# Using standardized logging system


def download_file(url, local_filename):
    """
    Download a file from a URL to a local file with progress bar

    Args:
        url: The URL to download from
        local_filename: The local path where the file should be saved

    Returns:
        The local filename if successful, False otherwise
    """
    # Using standardized logging system

    try:
        logger.info(f"Attempting to download {url} to {local_filename}")
        with requests.get(url, stream=True, allow_redirects=True) as r:
            if r.ok:
                total_size = int(r.headers.get("content-length", 0))
                logger.info(f"Download started, file size: {total_size} bytes")

                initial_pos = 0
                with open(local_filename, "wb") as f:
                    with tqdm(
                        total=total_size,
                        unit_scale=True,
                        desc=local_filename,
                        initial=initial_pos,
                        ascii=True,
                    ) as pbar:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:  # filter out keep-alive new chunks
                                f.write(chunk)
                                pbar.update(len(chunk))
                logger.info(f"Successfully downloaded {url} to {local_filename}")
                return local_filename
            else:
                logger.error(f"Failed to download {url}: HTTP status {r.status_code}")
                return False
    except Exception as e:
        logger.error(f"Exception during download of {url}: {e}")
        return False


def generate_local_path_from_url(
    url: str, directory_path: pathlib.Path, as_string: bool = True
):

    # Returns a pathlib path to a file

    # Parameters:
    #       url             :   string, the remote url for a file
    #       directory_name  :   path, the directory to white the file should be saved
    #       as_string       :   bool, toggles whether object is returned as string or path

    # Returns:
    #    Path object that concatenates the file name to a directory path

    try:
        remote_file_name = url.split("/")[-1]
        directory_path = pathlib.Path(directory_path)

        new_path = directory_path / remote_file_name

        if as_string is True:
            new_path = str(new_path)
        message = f"Successfully generated path {new_path}, as_string={as_string}"
        log_info(message=message)
        return new_path

    except Exception as e:
        message = f"Exception while generating local path: {e}"
        log_error(message=message)
        return False
