# Python stdlib

import io
import hashlib
from pyexpat.errors import messages

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
    from utilities import *
    from utilities.logging_utils import (
        init_logger,
        log_info,
        log_error,
        log_debug,
        log_warning,
        log_critical,
    )
except ImportError:
    # Handle case where package has different name
    import sys
    package_name = __name__.split('.')[0]
    try:
        logging_module = sys.modules[f"{package_name}.logging_utils"]
        init_logger = getattr(logging_module, 'init_logger', lambda x: None)
        log_info = getattr(logging_module, 'log_info', lambda x: print(f"INFO: {x}"))
        log_error = getattr(logging_module, 'log_error', lambda x: print(f"ERROR: {x}"))
        log_debug = getattr(logging_module, 'log_debug', lambda x: print(f"DEBUG: {x}"))
        log_warning = getattr(logging_module, 'log_warning', lambda x: print(f"WARNING: {x}"))
        log_critical = getattr(logging_module, 'log_critical', lambda x: print(f"CRITICAL: {x}"))
    except:
        # Fallback functions
        def init_logger(name): return None
        def log_info(msg): print(f"INFO: {msg}")
        def log_error(msg): print(f"ERROR: {msg}")
        def log_debug(msg): print(f"DEBUG: {msg}")
        def log_warning(msg): print(f"WARNING: {msg}")
        def log_critical(msg): print(f"CRITICAL: {msg}")

logging.getLogger(__name__)


def run_subprocess(command_list):
    """
    Run a shell command as a subprocess and handle the output.

    Args:
        command_list: The command to run, as a list or string

    Returns:
        The command output (stdout if successful, stderr if failed)
    """
    # Execute the command
    p = subprocess.Popen(
        command_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )
    stdout, stderr = p.communicate()
    returncode = p.returncode

    if returncode != 0:
        output = stderr.decode("utf-8")
        message = f"Subprocess {command_list} failed with return code {returncode}. "
        message += f"stderr: {output}"
        log_error(message=message)
        return output
    else:
        output = stdout.decode("utf-8")
        message = f"Subprocess {command_list} completed with return code {returncode}. "
        message += f"stdout: {output}"
        log_info(message=message)
        return output


# These next few functions can be done using Python methods or command line tools
# The first two are the Python variants
