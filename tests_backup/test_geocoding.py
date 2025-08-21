"""Tests for geocoding module."""

import pytest
import json
from unittest.mock import patch, MagicMock

import geocoding


class TestGeocoding:
    """Test cases for geocoding functions."""
    
    def test_concatenate_addresses_full(self):
        """Test address concatenation with all components."""
        result = geocoding.concatenate_addresses(
            street='123 Main St',
            city='London',
            state_province_area='England',
            postal_code='SW1A 1AA',
            country='UK'
        )
        expected = '123 Main St, London, England, SW1A 1AA, UK'
        assert result == expected
    
    def test_concatenate_addresses_partial(self):
        """Test address concatenation with some components."""
        result = geocoding.concatenate_addresses(
            street='456 Oak Ave',
            city='Manchester',
            country='UK'
        )
        expected = '456 Oak Ave, Manchester, UK'
        assert result == expected
    
    def test_concatenate_addresses_empty(self):
        """Test address concatenation with no components."""
        result = geocoding.concatenate_addresses()
        expected = ''
        assert result == expected
    
    def test_concatenate_addresses_none_values(self):
        """Test address concatenation with None values."""
        result = geocoding.concatenate_addresses(
            street=None,
            city='London',
            state_province_area=None,
            postal_code='SW1A 1AA',
            country=None
        )
        expected = 'London, SW1A 1AA'
        assert result == expected
    
    def test_use_nominatim_geocoder_no_geopy(self):
        """Test geocoding when geopy is not available."""
        # Since geopy is not available in our test environment, this should return None
        result = geocoding.use_nominatim_geocoder('London, UK')
        assert result is None
    
    def test_use_nominatim_geocoder_empty_address(self):
        """Test geocoding with empty address."""
        result = geocoding.use_nominatim_geocoder('')
        assert result is None
        
        result = geocoding.use_nominatim_geocoder(None)
        assert result is None
    
    @patch('geocoding.GEOPY_AVAILABLE', True)
    @patch('geocoding.Nominatim')
    def test_use_nominatim_geocoder_success(self, mock_nominatim):
        """Test successful geocoding."""
        # Mock the geocoder and result
        mock_geocoder = MagicMock()
        mock_nominatim.return_value = mock_geocoder
        
        mock_result = MagicMock()
        mock_result.latitude = 51.5074
        mock_result.longitude = -0.1278
        mock_result.raw = {
            'display_name': 'London, UK',
            'place_id': '12345',
            'licence': 'test licence'
        }
        mock_geocoder.geocode.return_value = mock_result
        
        result = geocoding.use_nominatim_geocoder('London, UK')
        
        # Should return JSON string
        assert result is not None
        result_data = json.loads(result)
        assert result_data['nominatim_lat'] == 51.5074
        assert result_data['nominatim_lng'] == -0.1278
        assert result_data['display_name'] == 'London, UK'
    
    @patch('geocoding.GEOPY_AVAILABLE', True)
    @patch('geocoding.Nominatim')
    def test_use_nominatim_geocoder_no_result(self, mock_nominatim):
        """Test geocoding with no results."""
        mock_geocoder = MagicMock()
        mock_nominatim.return_value = mock_geocoder
        mock_geocoder.geocode.return_value = None
        
        result = geocoding.use_nominatim_geocoder('NonexistentPlace123')
        assert result is None


class TestNominatimGeoClassifier:
    """Test cases for NominatimGeoClassifier class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.classifier = geocoding.NominatimGeoClassifier()
    
    def test_get_place_rank_label_known_ranks(self):
        """Test place rank classification for known ranks."""
        test_cases = [
            (3, 'Continent or ocean'),
            (7, 'Country'),
            (10, 'State or region'),
            (12, 'County or district'),
            (15, 'Municipality or metro'),
            (18, 'City or large town'),
            (20, 'Town or village'),
            (24, 'Suburb or locality'),
            (26, 'Neighborhood or area'),
            (27, 'Street'),
            (29, 'Address or building')
        ]
        
        for rank, expected_label in test_cases:
            result = self.classifier.get_place_rank_label(rank)
            assert result == expected_label
    
    def test_get_place_rank_label_unknown_rank(self):
        """Test place rank classification for unknown ranks."""
        result = self.classifier.get_place_rank_label(999)
        assert result == 'Unknown'
        
        result = self.classifier.get_place_rank_label(None)
        assert result == 'Unknown'
    
    def test_get_importance_label_known_values(self):
        """Test importance classification for known values."""
        test_cases = [
            (0.5, 'Global landmark'),
            (0.1, 'Major city or capital'),
            (0.01, 'City or notable place'),
            (0.001, 'Small town or feature'),
            (0.0, 'Minor/local detail')
        ]
        
        for importance, expected_label in test_cases:
            result = self.classifier.get_importance_label(importance)
            assert result == expected_label
    
    def test_get_importance_label_boundary_values(self):
        """Test importance classification for boundary values."""
        # Test values that should fall into specific categories
        assert self.classifier.get_importance_label(0.6) == 'Global landmark'
        assert self.classifier.get_importance_label(0.15) == 'Major city or capital'
        assert self.classifier.get_importance_label(0.05) == 'City or notable place'
        assert self.classifier.get_importance_label(0.005) == 'Small town or feature'
    
    def test_get_importance_label_unknown_value(self):
        """Test importance classification for unknown values."""
        result = self.classifier.get_importance_label(None)
        assert result == 'Unknown'
    
    def test_get_place_ranks_by_label(self):
        """Test getting place ranks by label."""
        result = self.classifier.get_place_ranks_by_label('Town or village')
        expected = [19, 20, 21, 22]
        assert result == expected
        
        result = self.classifier.get_place_ranks_by_label('Country')
        expected = [5, 6, 7]
        assert result == expected
        
        result = self.classifier.get_place_ranks_by_label('Unknown Label')
        assert result == []
    
    def test_get_importance_threshold_by_label(self):
        """Test getting importance threshold by label."""
        result = self.classifier.get_importance_threshold_by_label('Global landmark')
        assert result == 0.5
        
        result = self.classifier.get_importance_threshold_by_label('City or notable place')
        assert result == 0.01
        
        result = self.classifier.get_importance_threshold_by_label('Unknown Label')
        assert result is None
    
    def test_to_json(self):
        """Test JSON export functionality."""
        json_str = self.classifier.to_json()
        assert isinstance(json_str, str)
        assert len(json_str) > 100  # Should be a substantial JSON string
        
        # Verify it's valid JSON
        data = json.loads(json_str)
        assert 'place_rank_dict' in data
        assert 'importance_dict' in data
    
    def test_register_udfs_no_pyspark(self):
        """Test UDF registration when PySpark is not available."""
        with pytest.raises(ImportError, match="PySpark is required"):
            self.classifier.register_udfs(None)
    
    @patch('geocoding.PYSPARK_AVAILABLE', True)
    @patch('geocoding.udf')
    def test_register_udfs_success(self, mock_udf):
        """Test successful UDF registration."""
        mock_place_rank_udf = MagicMock()
        mock_importance_udf = MagicMock()
        mock_udf.side_effect = [mock_place_rank_udf, mock_importance_udf]
        
        place_rank_udf, importance_udf = self.classifier.register_udfs(MagicMock())
        
        assert place_rank_udf == mock_place_rank_udf
        assert importance_udf == mock_importance_udf
        assert mock_udf.call_count == 2
