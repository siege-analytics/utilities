"""Tests for file_utilities modules."""

import pytest
import os
import tempfile
import hashlib
from unittest.mock import patch, MagicMock

from file_utilities import hash_management, file_attributes, paths, remote_files, shell_utilities


class TestHashManagement:
    """Test cases for hash_management module."""
    
    def test_generate_sha256_hash_for_file(self, temp_file):
        """Test SHA256 hash generation for files."""
        result = hash_management.generate_sha256_hash_for_file(temp_file)
        assert result is not None
        assert len(result) == 64  # SHA256 hash length
        assert isinstance(result, str)
    
    def test_calculate_file_hash(self, temp_file):
        """Test generic file hash calculation."""
        result = hash_management.calculate_file_hash(temp_file)
        assert result is not None
        assert isinstance(result, str)
    
    def test_get_file_hash(self, temp_file):
        """Test getting file hash."""
        result = hash_management.get_file_hash(temp_file)
        assert result is not None
        assert isinstance(result, str)
    
    def test_get_quick_file_signature(self, temp_file):
        """Test quick file signature generation."""
        result = hash_management.get_quick_file_signature(temp_file)
        assert result is not None
        assert isinstance(result, str)
    
    def test_verify_file_integrity(self, temp_file):
        """Test file integrity verification."""
        # Get the actual hash
        actual_hash = hash_management.generate_sha256_hash_for_file(temp_file)
        
        # Verify with correct hash
        result = hash_management.verify_file_integrity(temp_file, actual_hash)
        assert result is True
        
        # Verify with incorrect hash
        wrong_hash = 'wrong_hash_value'
        result = hash_management.verify_file_integrity(temp_file, wrong_hash)
        assert result is False
    
    def test_hash_nonexistent_file(self):
        """Test hash generation for nonexistent file."""
        with pytest.raises(FileNotFoundError):
            hash_management.generate_sha256_hash_for_file('/nonexistent/file.txt')


class TestFileAttributes:
    """Test cases for file_attributes module."""
    
    def test_count_lines_in_file(self, temp_file):
        """Test line counting in files."""
        # We know our temp_file has 3 lines
        result = file_attributes.count_lines_in_file(temp_file)
        assert result == 3
    
    def test_count_total_rows_in_file_pythonically(self, temp_file):
        """Test total row counting using Python."""
        result = file_attributes.count_total_rows_in_file_pythonically(temp_file)
        assert result == 3
    
    def test_count_empty_rows_in_file_pythonically(self, temp_file):
        """Test empty row counting using Python."""
        result = file_attributes.count_empty_rows_in_file_pythonically(temp_file)
        assert result == 0  # Our test file has no empty lines
    
    def test_count_duplicate_rows_in_file_using_awk(self, temp_file):
        """Test duplicate row counting using awk."""
        # This test may fail if awk is not available
        try:
            result = file_attributes.count_duplicate_rows_in_file_using_awk(temp_file)
            assert isinstance(result, int)
            assert result >= 0
        except (FileNotFoundError, subprocess.SubprocessError):
            pytest.skip("awk not available")
    
    def test_check_if_file_exists_at_path(self, temp_file):
        """Test file existence checking."""
        assert file_attributes.check_if_file_exists_at_path(temp_file) is True
        assert file_attributes.check_if_file_exists_at_path('/nonexistent/file.txt') is False
    
    def test_check_for_file_type_in_directory(self, temp_dir):
        """Test checking for file types in directory."""
        # Create a test file in the directory
        test_file = os.path.join(temp_dir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test')
        
        result = file_attributes.check_for_file_type_in_directory(temp_dir, '*.txt')
        assert isinstance(result, (list, bool))


class TestPaths:
    """Test cases for paths module."""
    
    def test_ensure_path_exists(self, temp_dir):
        """Test path creation."""
        new_path = os.path.join(temp_dir, 'new_directory')
        paths.ensure_path_exists(new_path)
        assert os.path.exists(new_path)
        assert os.path.isdir(new_path)
    
    def test_init_path(self, temp_dir):
        """Test path initialization."""
        new_path = os.path.join(temp_dir, 'init_test')
        paths.init_path(new_path)
        assert os.path.exists(new_path)
    
    def test_unzip_file_to_its_own_directory(self, temp_dir):
        """Test unzipping functionality."""
        # Create a simple zip file for testing
        import zipfile
        zip_path = os.path.join(temp_dir, 'test.zip')
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('test.txt', 'test content')
        
        result = paths.unzip_file_to_its_own_directory(zip_path)
        assert os.path.exists(result)
        assert os.path.isdir(result)
        assert os.path.exists(os.path.join(result, 'test.txt'))


class TestRemoteFiles:
    """Test cases for remote_files module."""
    
    @patch('requests.get')
    def test_download_file_success(self, mock_get, temp_dir):
        """Test successful file download."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'test file content'
        mock_get.return_value = mock_response
        
        output_path = os.path.join(temp_dir, 'downloaded.txt')
        result = remote_files.download_file('http://example.com/test.txt', output_path)
        
        assert result is True
        assert os.path.exists(output_path)
        with open(output_path, 'rb') as f:
            assert f.read() == b'test file content'
    
    @patch('requests.get')
    def test_download_file_failure(self, mock_get, temp_dir):
        """Test failed file download."""
        # Mock failed response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        output_path = os.path.join(temp_dir, 'failed_download.txt')
        result = remote_files.download_file('http://example.com/nonexistent.txt', output_path)
        
        assert result is False
        assert not os.path.exists(output_path)
    
    def test_generate_local_path_from_url(self):
        """Test local path generation from URL."""
        url = 'http://example.com/path/to/file.txt'
        result = remote_files.generate_local_path_from_url(url)
        
        assert isinstance(result, str)
        assert result.endswith('file.txt')


class TestShellUtilities:
    """Test cases for shell_utilities module."""
    
    def test_run_subprocess_success(self):
        """Test successful subprocess execution."""
        # Use a simple command that should work on most systems
        result = shell_utilities.run_subprocess(['echo', 'test'])
        assert 'test' in result
    
    def test_run_subprocess_failure(self):
        """Test failed subprocess execution."""
        # Use a command that should fail
        with pytest.raises(Exception):
            shell_utilities.run_subprocess(['nonexistent_command_xyz'])
