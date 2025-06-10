# Python stdlib

import io
import hashlib
from pyexpat.errors import messages

from IPython.utils.capture import capture_output
import pathlib
import requests
import subprocess

from pkg_resources import working_set
from tqdm import tqdm

import zipfile

# Logging

import logging

from siege_utilities.core.logging import (
    init_logger,
    log_info,
    log_error,
    log_debug,
    log_warning,
    log_critical,
)

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


def run_command(command, shell=True, capture_output=True, text=True, **kwargs):
    """
    Run a shell command and return the result.
    
    Args:
        command: Command to run (string or list)
        shell: Whether to run through shell
        capture_output: Whether to capture stdout/stderr
        text: Whether to return text output
        **kwargs: Additional arguments for subprocess
        
    Returns:
        subprocess.CompletedProcess object
    """
    try:
        result = subprocess.run(
            command, 
            shell=shell, 
            capture_output=capture_output, 
            text=text,
            **kwargs
        )
        
        if result.returncode == 0:
            message = f"Command succeeded: {command}"
            log_info(message)
        else:
            message = f"Command failed with code {result.returncode}: {command}"
            log_error(message)
            
        return result
    except Exception as e:
        message = f"Error running command '{command}': {e}"
        log_error(message)
        raise


def count_lines_with_wc(file_path) -> int:
    """
    Count lines in a file using the wc command.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Number of lines in the file
    """
    try:
        file_path = str(file_path)
        result = run_command(f"wc -l '{file_path}'")
        
        if result.returncode == 0:
            # wc output format: "  123 filename"
            count_str = result.stdout.strip().split()[0]
            count = int(count_str)
            message = f"File {file_path} has {count} lines (via wc)"
            log_info(message)
            return count
        else:
            message = f"wc command failed for {file_path}: {result.stderr}"
            log_error(message)
            return 0
    except Exception as e:
        message = f"Error counting lines with wc for {file_path}: {e}"
        log_error(message)
        return 0


def count_lines_with_sed(file_path) -> int:
    """
    Count lines in a file using the sed command.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Number of lines in the file
    """
    try:
        file_path = str(file_path)
        result = run_command(f"sed -n '$=' '{file_path}'")
        
        if result.returncode == 0:
            output = result.stdout.strip()
            if output:
                count = int(output)
            else:
                count = 0  # Empty file
            message = f"File {file_path} has {count} lines (via sed)"
            log_info(message)
            return count
        else:
            message = f"sed command failed for {file_path}: {result.stderr}"
            log_error(message)
            return 0
    except Exception as e:
        message = f"Error counting lines with sed for {file_path}: {e}"
        log_error(message)
        return 0


def check_command_available(command_name) -> bool:
    """
    Check if a command is available in the system PATH.
    
    Args:
        command_name: Name of the command to check
        
    Returns:
        True if command is available, False otherwise
    """
    try:
        result = run_command(f"which {command_name}")
        available = result.returncode == 0
        
        if available:
            message = f"Command '{command_name}' is available at: {result.stdout.strip()}"
            log_info(message)
        else:
            message = f"Command '{command_name}' is not available"
            log_warning(message)
            
        return available
    except Exception as e:
        message = f"Error checking command availability for '{command_name}': {e}"
        log_error(message)
        return False


__all__ = [
    "run_subprocess",
    "run_command",
    "count_lines_with_wc", 
    "count_lines_with_sed",
    "check_command_available",
]


# These next few functions can be done using Python methods or command line tools
# The first two are the Python variants
