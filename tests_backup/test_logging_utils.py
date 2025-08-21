"""Tests for logging_utils module."""

import pytest
import logging
import tempfile
import os
from io import StringIO
from unittest.mock import patch

import logging_utils


class TestLoggingUtils:
    """Test cases for logging utilities."""
    
    def test_init_logger(self):
        """Test logger initialization."""
        logger = logging_utils.init_logger('test_logger')
        assert logger is not None
        assert logger.name == 'test_logger'
    
    def test_setup_module_logging(self):
        """Test module logging setup."""
        test_globals = {}
        logging_utils.setup_module_logging(test_globals, 'test_module')
        
        # Check that logging functions are added to globals
        expected_functions = ['log_info', 'log_debug', 'log_warning', 'log_error', 'log_critical']
        for func_name in expected_functions:
            assert func_name in test_globals
            assert callable(test_globals[func_name])
    
    def test_log_functions(self):
        """Test individual logging functions."""
        with patch('logging_utils.logging.getLogger') as mock_get_logger:
            mock_logger = mock_get_logger.return_value
            
            # Test each logging function
            logging_utils.log_info('test info')
            mock_logger.info.assert_called_with('test info')
            
            logging_utils.log_warning('test warning')
            mock_logger.warning.assert_called_with('test warning')
            
            logging_utils.log_error('test error')
            mock_logger.error.assert_called_with('test error')
            
            logging_utils.log_critical('test critical')
            mock_logger.critical.assert_called_with('test critical')
            
            logging_utils.log_debug('test debug')
            mock_logger.debug.assert_called_with('test debug')
    
    def test_parse_log_level(self):
        """Test log level parsing."""
        assert logging_utils.parse_log_level('DEBUG') == logging.DEBUG
        assert logging_utils.parse_log_level('INFO') == logging.INFO
        assert logging_utils.parse_log_level('WARNING') == logging.WARNING
        assert logging_utils.parse_log_level('ERROR') == logging.ERROR
        assert logging_utils.parse_log_level('CRITICAL') == logging.CRITICAL
        
        # Test case insensitive
        assert logging_utils.parse_log_level('debug') == logging.DEBUG
        assert logging_utils.parse_log_level('info') == logging.INFO
        
        # Test default fallback
        assert logging_utils.parse_log_level('INVALID') == logging.INFO
        assert logging_utils.parse_log_level('') == logging.INFO
        assert logging_utils.parse_log_level(None) == logging.INFO
    
    def test_file_logging(self, temp_dir):
        """Test file logging functionality."""
        log_file = os.path.join(temp_dir, 'test.log')
        
        # Configure file logging
        logger = logging.getLogger('test_file_logger')
        logger.handlers.clear()
        
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG)
        
        # Log some messages
        logger.info('Test file logging')
        logger.warning('Test warning message')
        
        # Check file was created and contains messages
        assert os.path.exists(log_file)
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Test file logging' in content
            assert 'Test warning message' in content
