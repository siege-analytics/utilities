"""Tests for string_manipulation module."""

import pytest
import string_manipulation


class TestStringManipulation:
    """Test cases for string manipulation functions."""
    
    def test_remove_wrapping_quotes_and_trim_double_quotes(self):
        """Test removing double quotes and trimming."""
        input_str = '  "test string"  '
        expected = 'test string'
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_single_quotes(self):
        """Test removing single quotes and trimming."""
        input_str = "  'test string'  "
        expected = 'test string'
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_no_quotes(self):
        """Test trimming without quotes."""
        input_str = '  test string  '
        expected = 'test string'
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_mismatched_quotes(self):
        """Test handling mismatched quotes."""
        input_str = '"test string\''
        expected = '"test string\''
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_empty_string(self):
        """Test handling empty string."""
        input_str = ''
        expected = ''
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_none(self):
        """Test handling None input."""
        input_str = None
        expected = ''
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_newline(self):
        """Test handling newline character."""
        input_str = '\n'
        expected = ''
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_nested_quotes(self):
        """Test handling nested quotes."""
        input_str = '"test "inner" string"'
        expected = 'test "inner" string'
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_single_character(self):
        """Test handling single character strings."""
        input_str = '"a"'
        expected = 'a'
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
        
        input_str = 'a'
        expected = 'a'
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
    
    def test_remove_wrapping_quotes_and_trim_whitespace_only(self):
        """Test handling whitespace-only strings."""
        input_str = '   '
        expected = ''
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
        
        input_str = '"   "'
        expected = ''
        result = string_manipulation.remove_wrapping_quotes_and_trim(input_str)
        assert result == expected
