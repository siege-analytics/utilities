"""Pytest configuration and fixtures for utilities tests."""

import sys
import os
import tempfile
import pytest
from pathlib import Path

# Add the parent directory to the path so we can import utilities
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def temp_file():
    """Create a temporary file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
        tmp.write("test line 1\ntest line 2\ntest line 3\n")
        tmp_path = tmp.name
    
    yield tmp_path
    
    # Cleanup
    if os.path.exists(tmp_path):
        os.unlink(tmp_path)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return {
        'addresses': [
            '123 Main St, London, UK',
            '456 Oak Ave, Manchester, UK', 
            '789 Pine Rd, Birmingham, UK'
        ],
        'place_ranks': [15, 20, 25, 30],
        'importance_values': [0.5, 0.1, 0.01, 0.001],
        'test_strings': [
            '  "quoted string"  ',
            "  'single quoted'  ",
            '  unquoted string  ',
            '',
            None
        ]
    }
