"""
Fixed HDFS Operations - Minimal working version
"""
import os
import subprocess
import pathlib
import json
import time
import hashlib
from typing import Optional, Tuple, Dict

# Import logging functions from main package
try:
    from siege_utilities import log_info, log_warning, log_error
except ImportError:
    # Fallback if main package not available yet
    def log_info(message): print(f"INFO: {message}")
    def log_warning(message): print(f"WARNING: {message}")
    def log_error(message): print(f"ERROR: {message}")


def get_quick_file_signature(file_path):
    """
    Get quick file signature based on size and modification time.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File signature string or 'error' if failed
    """
    try:
        stat = pathlib.Path(file_path).stat()
        return f'{stat.st_size}_{stat.st_mtime}'
    except Exception as e:
        log_error(f"Error getting file signature for {file_path}: {e}")
        return 'error'


def check_hdfs_status():
    """Check if HDFS is accessible"""
    try:
        result = subprocess.run(['hdfs', 'dfs', '-ls', '/'], capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except Exception as e:
        log_error(f"Error checking HDFS status: {e}")
        return False
