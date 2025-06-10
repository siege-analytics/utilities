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

from siege_utilities.core.logging import *
from siege_utilities.files.operations import *

logging.getLogger(__name__)


def ensure_path_exists(desired_path: pathlib.Path) -> pathlib.Path:

    try:
        desired_path_object = pathlib.Path(desired_path)
        result = pathlib.Path(desired_path_object).mkdir(parents=True, exist_ok=True)
        message = f"Generated a path at {str(desired_path_object)}: {result}"
        log_info(message=message)
        gitkeep_file = desired_path_object / ".gitkeep"
        create_empty_file(gitkeep_file)
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


def remove_tree(target_path: pathlib.Path) -> bool:
    """
    Remove a directory tree (including all files and subdirectories).
    
    Args:
        target_path: Path to remove
        
    Returns:
        True if successful, False otherwise
    """
    try:
        import shutil
        target_path = pathlib.Path(target_path)
        if target_path.exists():
            if target_path.is_file():
                target_path.unlink()
            else:
                shutil.rmtree(target_path)
            message = f"Successfully removed: {target_path}"
            log_info(message)
        else:
            message = f"Path does not exist, nothing to remove: {target_path}"
            log_info(message)
        return True
    except Exception as e:
        message = f"Error removing tree {target_path}: {e}"
        log_error(message)
        return False


def find_files_by_extension(directory_path: pathlib.Path, extension: str) -> list:
    """
    Find all files with a specific extension in a directory.
    
    Args:
        directory_path: Directory to search in
        extension: File extension to search for (with or without dot)
        
    Returns:
        List of pathlib.Path objects
    """
    try:
        directory_path = pathlib.Path(directory_path)
        if not extension.startswith('.'):
            extension = f'.{extension}'
        
        files = list(directory_path.glob(f'**/*{extension}'))
        message = f"Found {len(files)} files with extension {extension} in {directory_path}"
        log_info(message)
        return files
    except Exception as e:
        message = f"Error finding files by extension in {directory_path}: {e}"
        log_error(message)
        return []


def unzip_file(zip_path: pathlib.Path, extract_to: pathlib.Path = None) -> pathlib.Path:
    """
    Extract a zip file to a directory.
    
    Args:
        zip_path: Path to the zip file
        extract_to: Directory to extract to (defaults to same directory as zip)
        
    Returns:
        Path to the extraction directory
    """
    try:
        import zipfile
        zip_path = pathlib.Path(zip_path)
        
        if extract_to is None:
            extract_to = zip_path.parent / zip_path.stem
        else:
            extract_to = pathlib.Path(extract_to)
        
        extract_to.mkdir(parents=True, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        
        message = f"Successfully extracted {zip_path} to {extract_to}"
        log_info(message)
        return extract_to
    except Exception as e:
        message = f"Error extracting {zip_path}: {e}"
        log_error(message)
        return False


def copy_file(source_path: pathlib.Path, destination_path: pathlib.Path) -> bool:
    """
    Copy a file from source to destination.
    
    Args:
        source_path: Source file path
        destination_path: Destination file path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        import shutil
        source_path = pathlib.Path(source_path)
        destination_path = pathlib.Path(destination_path)
        
        # Ensure destination directory exists
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        
        shutil.copy2(source_path, destination_path)
        message = f"Successfully copied {source_path} to {destination_path}"
        log_info(message)
        return True
    except Exception as e:
        message = f"Error copying {source_path} to {destination_path}: {e}"
        log_error(message)
        return False


def move_file(source_path: pathlib.Path, destination_path: pathlib.Path) -> bool:
    """
    Move a file from source to destination.
    
    Args:
        source_path: Source file path
        destination_path: Destination file path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        import shutil
        source_path = pathlib.Path(source_path)
        destination_path = pathlib.Path(destination_path)
        
        # Ensure destination directory exists
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        
        shutil.move(source_path, destination_path)
        message = f"Successfully moved {source_path} to {destination_path}"
        log_info(message)
        return True
    except Exception as e:
        message = f"Error moving {source_path} to {destination_path}: {e}"
        log_error(message)
        return False


__all__ = [
    "ensure_path_exists",
    "unzip_file_to_its_own_directory",
    "remove_tree",
    "find_files_by_extension",
    "check_for_file_type_in_directory",
    "unzip_file",
    "copy_file",
    "move_file",
]
