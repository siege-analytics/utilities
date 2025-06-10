"""
File utilities package.

This package provides comprehensive file operations including:
- Basic file operations (create, delete, check existence)
- Path and directory management
- Remote file downloading
- File hashing and integrity checking
- Shell command utilities
"""

from . import operations
from . import paths
from . import hashing
from . import shell

# Optional import for remote operations (requires requests)
try:
    from . import remote
    _REMOTE_AVAILABLE = True
except ImportError:
    _REMOTE_AVAILABLE = False

# Import commonly used functions for convenience
from .operations import (
    check_file_exists,
    create_empty_file,
    delete_file,
    count_lines_in_file,
    count_empty_lines,
    write_text_to_file,
    read_text_from_file
)

from .paths import (
    ensure_path_exists,
    remove_tree,
    find_files_by_extension,
    check_for_file_type_in_directory,
    unzip_file,
    copy_file,
    move_file
)

from .hashing import (
    hash_file,
    hash_file_sha256,
    hash_file_md5,
    get_quick_file_signature,
    verify_file_integrity,
    compare_files_by_hash
)

from .shell import (
    run_command,
    count_lines_with_wc,
    count_lines_with_sed,
    check_command_available
)

# Add remote functions if available
if _REMOTE_AVAILABLE:
    from .remote import (
        download_file,
        generate_local_path_from_url,
        check_url_accessible,
        get_remote_file_size
    )

__all__ = [
    # Submodules
    "operations",
    "paths",
    "hashing",
    "shell",

    # Operations functions
    "check_file_exists",
    "create_empty_file",
    "delete_file",
    "count_lines_in_file",
    "count_empty_lines",
    "write_text_to_file",
    "read_text_from_file",

    # Path functions
    "ensure_path_exists",
    "remove_tree",
    "find_files_by_extension",
    "check_for_file_type_in_directory",
    "unzip_file",
    "copy_file",
    "move_file",

    # Hashing functions
    "hash_file",
    "hash_file_sha256",
    "hash_file_md5",
    "get_quick_file_signature",
    "verify_file_integrity",
    "compare_files_by_hash",

    # Shell functions
    "run_command",
    "count_lines_with_wc",
    "count_lines_with_sed",
    "check_command_available",
]

# Add remote functions to __all__ if available
if _REMOTE_AVAILABLE:
    __all__.extend([
        "remote",
        "download_file",
        "generate_local_path_from_url",
        "check_url_accessible",
        "get_remote_file_size"
    ])
