"""
Comprehensive test suite for multi_region_config.py

Tests cover configuration loading, validation, FIPS code validation,
error handling, and region management functionality.
"""

import pytest
import json
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import logging

from multi_region_config import (
    RegionConfig, MultiRegionConfigManager
)

class TestRegionConfig:
    """Test suite for RegionConfig dataclass"""
    
    def test_region_config_creation_valid(self):
        """Test creating a valid RegionConfig"""
        config = RegionConfig(
            region_name="Test Region",
            region_code="TEST",
            fips_code="12345",
            region_input_date1=datetime(2009, 1, 1),
            region_input_date2=datetime(2019, 1, 1),
            region_input_amount1=75000.0,
            region_input_amount2=200000.0,
            market_type="Test Market",
            description="Test Description",
            notes="Test Notes"
        )
        
        assert config.region_name == "Test Region"
        assert config.region_code == "TEST"
        assert config.fips_code == "12345"
        assert config.region_input_date1 == datetime(2009, 1, 1)
        assert config.region_input_date2 == datetime(2019, 1, 1)
        assert config.region_input_amount1 == 75000.0
        assert config.region_input_amount2 == 200000.0
        assert config.market_type == "Test Market"
        assert config.description == "Test Description"
        assert config.notes == "Test Notes"
    
    def test_region_config_validation_date_order(self):
        """Test RegionConfig validation for date order"""
        with patch('multi_region_config.logger') as mock_logger:
            # date1 should be older than date2, but we'll reverse them
            config = RegionConfig(
                region_name="Test Region",
                region_code="TEST",
                fips_code="12345",
                region_input_date1=datetime(2019, 1, 1),  # Newer date
                region_input_date2=datetime(2009, 1, 1),  # Older date
                region_input_amount1=75000.0,
                region_input_amount2=200000.0,
                market_type="Test Market",
                description="Test Description",
                notes="Test Notes"
            )
            
            # Should log a warning about date order
            mock_logger.warning.assert_called()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "date1" in warning_msg
            assert "should be older than date2" in warning_msg
    
    def test_region_config_validation_amount_order(self):
        """Test RegionConfig validation for amount order"""
        with patch('multi_region_config.logger') as mock_logger:
            # amount1 should be less than amount2, but we'll reverse them
            config = RegionConfig(
                region_name="Test Region",
                region_code="TEST",
                fips_code="12345",
                region_input_date1=datetime(2009, 1, 1),
                region_input_date2=datetime(2019, 1, 1),
                region_input_amount1=300000.0,  # Higher amount
                region_input_amount2=100000.0,  # Lower amount
                market_type="Test Market",
                description="Test Description",
                notes="Test Notes"
            )
            
            # Should log a warning about amount order
            mock_logger.warning.assert_called()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "amount1" in warning_msg
            assert "should be less than amount2" in warning_msg


class TestMultiRegionConfigManager:
    """Test suite for MultiRegionConfigManager class"""
    
    def setup_method(self):
        """Set up test fixtures with temporary directory structure"""
        self.temp_dir = tempfile.mkdtemp()
        self.regions_dir = Path(self.temp_dir) / "regions"
        self.regions_dir.mkdir()
        
        # Create test region configuration
        self.test_config_data = {
            "region_name": "Test City, VA",
            "region_code": "TEST",
            "fips_code": "51999",
            "region_input_date1": "2009-01-01",
            "region_input_date2": "2019-01-01",
            "region_input_amount1": 75000,
            "region_input_amount2": 200000,
            "market_type": "Rural/Small City",
            "description": "Test region for unit testing",
            "notes": "Used for testing purposes only"
        }
    
    def teardown_method(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_test_region(self, region_key: str, config_data: dict = None, 
                          create_excel_files: bool = True):
        """Helper method to create test region structure"""
        if config_data is None:
            config_data = self.test_config_data.copy()
            config_data["region_code"] = region_key.upper()
        
        region_dir = self.regions_dir / region_key
        region_dir.mkdir()
        
        # Create config.json
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        if create_excel_files:
            # Create test Excel files
            test_data = {
                'Owner 1 Last Name': ['Smith', 'Jones'],
                'Owner 1 First Name': ['John', 'Mary'],
                'Address': ['123 Main St', '456 Oak Ave'],
                'FIPS': [config_data['fips_code']] * 2
            }
            df = pd.DataFrame(test_data)
            
            # Create main region file
            main_file = region_dir / "main_region.xlsx"
            df.to_excel(main_file, index=False)
            
            # Create niche file
            niche_file = region_dir / "liens.xlsx"
            df.to_excel(niche_file, index=False)
        
        return region_dir
    
    def test_init_with_valid_regions_directory(self):
        """Test initialization with valid regions directory"""
        # Create test regions
        self.create_test_region("test_region_1")
        self.create_test_region("test_region_2")
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        assert len(manager.configs) == 2
        assert "test_region_1" in manager.configs
        assert "test_region_2" in manager.configs
    
    def test_init_with_nonexistent_directory(self):
        """Test initialization with non-existent directory"""
        nonexistent_dir = str(Path(self.temp_dir) / "nonexistent")
        
        with pytest.raises(FileNotFoundError):
            MultiRegionConfigManager(nonexistent_dir)
    
    def test_load_region_config_valid(self):
        """Test loading a valid region configuration"""
        region_dir = self.create_test_region("valid_region")
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        config = manager.get_region_config("valid_region")
        
        assert config.region_name == "Test City, VA"
        assert config.region_code == "VALID_REGION"
        assert config.fips_code == "51999"
        assert config.region_input_date1 == datetime(2009, 1, 1)
        assert config.region_input_date2 == datetime(2019, 1, 1)
        assert config.region_input_amount1 == 75000.0
        assert config.region_input_amount2 == 200000.0
    
    def test_load_region_config_missing_file(self):
        """Test loading region config when config.json is missing"""
        region_dir = self.regions_dir / "missing_config"
        region_dir.mkdir()
        # Don't create config.json
        
        with patch('multi_region_config.logger') as mock_logger:
            manager = MultiRegionConfigManager(str(self.regions_dir))
            
            # Should log warning about missing config
            mock_logger.warning.assert_called()
            assert len(manager.configs) == 0
    
    def test_load_region_config_invalid_json(self):
        """Test loading region config with invalid JSON"""
        region_dir = self.regions_dir / "invalid_json"
        region_dir.mkdir()
        
        # Create invalid JSON file
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            f.write("{ invalid json content")
        
        with patch('multi_region_config.logger') as mock_logger:
            manager = MultiRegionConfigManager(str(self.regions_dir))
            
            # Should log error about invalid JSON
            mock_logger.error.assert_called()
            assert len(manager.configs) == 0
    
    def test_load_region_config_missing_required_fields(self):
        """Test loading region config with missing required fields"""
        region_dir = self.regions_dir / "missing_fields"
        region_dir.mkdir()
        
        # Create config with missing required fields
        incomplete_config = {
            "region_name": "Incomplete Region",
            # Missing other required fields
        }
        
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(incomplete_config, f)
        
        with patch('multi_region_config.logger') as mock_logger:
            manager = MultiRegionConfigManager(str(self.regions_dir))
            
            # Should log error about missing fields
            mock_logger.error.assert_called()
            assert len(manager.configs) == 0
    
    def test_load_region_config_invalid_dates(self):
        """Test loading region config with invalid date formats"""
        region_dir = self.regions_dir / "invalid_dates"
        region_dir.mkdir()
        
        invalid_config = self.test_config_data.copy()
        invalid_config["region_input_date1"] = "not-a-date"
        
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(invalid_config, f)
        
        with patch('multi_region_config.logger') as mock_logger:
            manager = MultiRegionConfigManager(str(self.regions_dir))
            
            # Should log error about invalid dates
            mock_logger.error.assert_called()
            assert len(manager.configs) == 0
    
    def test_load_region_config_invalid_amounts(self):
        """Test loading region config with invalid amounts"""
        region_dir = self.regions_dir / "invalid_amounts"
        region_dir.mkdir()
        
        invalid_config = self.test_config_data.copy()
        invalid_config["region_input_amount1"] = "not-a-number"
        
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(invalid_config, f)
        
        with patch('multi_region_config.logger') as mock_logger:
            manager = MultiRegionConfigManager(str(self.regions_dir))
            
            # Should log error about invalid amounts
            mock_logger.error.assert_called()
            assert len(manager.configs) == 0
    
    def test_get_region_config_existing(self):
        """Test getting configuration for existing region"""
        self.create_test_region("existing_region")
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        config = manager.get_region_config("existing_region")
        
        assert config.region_code == "EXISTING_REGION"
    
    def test_get_region_config_nonexistent(self):
        """Test getting configuration for non-existent region"""
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        with pytest.raises(ValueError, match="Region 'nonexistent' not found"):
            manager.get_region_config("nonexistent")
    
    def test_list_regions(self):
        """Test listing all available regions"""
        self.create_test_region("region_a")
        self.create_test_region("region_b")
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        regions = manager.list_regions()
        
        assert len(regions) == 2
        region_keys = [r['key'] for r in regions]
        assert "region_a" in region_keys
        assert "region_b" in region_keys
        
        # Test sorting by name
        assert regions[0]['name'] <= regions[1]['name']
    
    def test_get_region_directory(self):
        """Test getting region directory path"""
        self.create_test_region("test_region")
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        region_dir = manager.get_region_directory("test_region")
        
        assert region_dir == self.regions_dir / "test_region"
        assert region_dir.exists()
    
    def test_get_region_directory_nonexistent(self):
        """Test getting directory for non-existent region"""
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        with pytest.raises(ValueError, match="Region 'nonexistent' not found"):
            manager.get_region_directory("nonexistent")
    
    def test_validate_region_files_valid(self):
        """Test validating region files when all required files exist"""
        self.create_test_region("valid_region", create_excel_files=True)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        validation = manager.validate_region_files("valid_region")
        
        assert validation['has_config'] == True
        assert validation['has_main_file'] == True
        assert validation['has_excel_files'] == True
        assert validation['total_files'] == 2  # main_region.xlsx + liens.xlsx
        assert validation['valid'] == True
    
    def test_validate_region_files_missing_main(self):
        """Test validating region files when main file is missing"""
        region_dir = self.create_test_region("missing_main", create_excel_files=False)
        
        # Create only niche file, no main file
        test_data = pd.DataFrame({'FIPS': ['51999']})
        niche_file = region_dir / "liens.xlsx"
        test_data.to_excel(niche_file, index=False)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        validation = manager.validate_region_files("missing_main")
        
        assert validation['has_config'] == True
        assert validation['has_main_file'] == False
        assert validation['has_excel_files'] == True
        assert validation['valid'] == False
    
    def test_validate_region_files_no_excel_files(self):
        """Test validating region files when no Excel files exist"""
        self.create_test_region("no_excel", create_excel_files=False)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        validation = manager.validate_region_files("no_excel")
        
        assert validation['has_config'] == True
        assert validation['has_main_file'] == False
        assert validation['has_excel_files'] == False
        assert validation['total_files'] == 0
        assert validation['valid'] == False
    
    def test_create_output_directory(self):
        """Test creating output directory structure"""
        self.create_test_region("test_region")
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        with patch('multi_region_config.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2023, 6, 15)
            output_dir = manager.create_output_directory("test_region")
        
        expected_path = Path("output") / "test_region" / "2023_06"
        assert str(output_dir).endswith(str(expected_path))
    
    def test_validate_fips_codes_all_valid(self):
        """Test FIPS code validation when all files have correct FIPS"""
        self.create_test_region("valid_fips")
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        validation = manager.validate_fips_codes("valid_fips")
        
        assert validation['region_fips'] == "51999"
        assert validation['files_checked'] == 2
        assert validation['files_valid'] == 2
        assert validation['all_valid'] == True
        assert len(validation['fips_mismatches']) == 0
        assert len(validation['missing_fips_column']) == 0
    
    def test_validate_fips_codes_mismatch(self):
        """Test FIPS code validation with mismatched FIPS codes"""
        region_dir = self.create_test_region("fips_mismatch", create_excel_files=False)
        
        # Create files with wrong FIPS code
        wrong_fips_data = {
            'Owner 1 Last Name': ['Smith'],
            'Owner 1 First Name': ['John'],
            'Address': ['123 Main St'],
            'FIPS': ['99999']  # Wrong FIPS code
        }
        df = pd.DataFrame(wrong_fips_data)
        
        main_file = region_dir / "main_region.xlsx"
        df.to_excel(main_file, index=False)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        validation = manager.validate_fips_codes("fips_mismatch")
        
        assert validation['all_valid'] == False
        assert len(validation['fips_mismatches']) == 1
        assert validation['fips_mismatches'][0]['file'] == 'main_region.xlsx'
        assert validation['fips_mismatches'][0]['expected'] == '51999'
        assert '99999' in validation['fips_mismatches'][0]['found']
    
    def test_validate_fips_codes_missing_column(self):
        """Test FIPS code validation with missing FIPS column"""
        region_dir = self.create_test_region("missing_fips", create_excel_files=False)
        
        # Create files without FIPS column
        no_fips_data = {
            'Owner 1 Last Name': ['Smith'],
            'Owner 1 First Name': ['John'],
            'Address': ['123 Main St']
            # No FIPS column
        }
        df = pd.DataFrame(no_fips_data)
        
        main_file = region_dir / "main_region.xlsx"
        df.to_excel(main_file, index=False)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        validation = manager.validate_fips_codes("missing_fips")
        
        assert validation['all_valid'] == False
        assert len(validation['missing_fips_column']) == 1
        assert validation['missing_fips_column'][0] == 'main_region.xlsx'
    
    def test_validate_fips_codes_empty_file(self):
        """Test FIPS code validation with empty Excel files"""
        region_dir = self.create_test_region("empty_files", create_excel_files=False)
        
        # Create empty Excel file
        empty_df = pd.DataFrame()
        empty_file = region_dir / "empty.xlsx"
        empty_df.to_excel(empty_file, index=False)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        with patch('multi_region_config.logger') as mock_logger:
            validation = manager.validate_fips_codes("empty_files")
            
            # Should skip empty file and log warning
            mock_logger.warning.assert_called()
    
    def test_validate_fips_codes_corrupted_file(self):
        """Test FIPS code validation with corrupted Excel files"""
        region_dir = self.create_test_region("corrupted_files", create_excel_files=False)
        
        # Create corrupted Excel file (not actually Excel data)
        corrupted_file = region_dir / "corrupted.xlsx"
        with open(corrupted_file, 'w') as f:
            f.write("This is not Excel data")
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        with patch('multi_region_config.logger') as mock_logger:
            validation = manager.validate_fips_codes("corrupted_files")
            
            # Should log error and mark file as invalid
            mock_logger.error.assert_called()
            assert validation['all_valid'] == False
    
    def test_validate_fips_codes_mixed_fips_types(self):
        """Test FIPS code validation with mixed string/numeric FIPS"""
        region_dir = self.create_test_region("mixed_fips", create_excel_files=False)
        
        # Create file with numeric FIPS that should match string FIPS
        mixed_fips_data = {
            'Owner 1 Last Name': ['Smith'],
            'FIPS': [51999]  # Numeric instead of string
        }
        df = pd.DataFrame(mixed_fips_data)
        
        main_file = region_dir / "main_region.xlsx"
        df.to_excel(main_file, index=False)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        validation = manager.validate_fips_codes("mixed_fips")
        
        # Should handle string/numeric conversion and validate correctly
        assert validation['all_valid'] == True


class TestConfigurationEdgeCases:
    """Test suite for edge cases and error handling"""
    
    def setup_method(self):
        """Set up edge case test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.regions_dir = Path(self.temp_dir) / "regions"
        self.regions_dir.mkdir()
    
    def teardown_method(self):
        """Clean up edge case test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_config_with_unicode_characters(self):
        """Test configuration with Unicode characters in names"""
        unicode_config = {
            "region_name": "Región de Prueba, México",
            "region_code": "MX01",
            "fips_code": "99001",
            "region_input_date1": "2009-01-01",
            "region_input_date2": "2019-01-01",
            "region_input_amount1": 75000,
            "region_input_amount2": 200000,
            "market_type": "Mercado Internacional",
            "description": "Configuración de prueba con caracteres Unicode",
            "notes": "Notas con acentos: café, niño, España"
        }
        
        region_dir = self.regions_dir / "unicode_region"
        region_dir.mkdir()
        
        config_file = region_dir / "config.json"
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(unicode_config, f, indent=2, ensure_ascii=False)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        config = manager.get_region_config("unicode_region")
        
        assert config.region_name == "Región de Prueba, México"
        assert "café" in config.notes
    
    def test_config_with_extreme_dates(self):
        """Test configuration with extreme date values"""
        extreme_config = {
            "region_name": "Extreme Dates Region",
            "region_code": "EXT",
            "fips_code": "99002",
            "region_input_date1": "1900-01-01",  # Very old
            "region_input_date2": "2099-12-31",  # Far future
            "region_input_amount1": 1,           # Very low
            "region_input_amount2": 999999999,   # Very high
            "market_type": "Extreme Market",
            "description": "Testing extreme values",
            "notes": ""
        }
        
        region_dir = self.regions_dir / "extreme_region"
        region_dir.mkdir()
        
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(extreme_config, f)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        config = manager.get_region_config("extreme_region")
        
        assert config.region_input_date1 == datetime(1900, 1, 1)
        assert config.region_input_date2 == datetime(2099, 12, 31)
        assert config.region_input_amount1 == 1.0
        assert config.region_input_amount2 == 999999999.0
    
    def test_config_with_optional_fields_missing(self):
        """Test configuration with optional fields missing"""
        minimal_config = {
            "region_name": "Minimal Region",
            "region_code": "MIN",
            "fips_code": "99003",
            "region_input_date1": "2009-01-01",
            "region_input_date2": "2019-01-01",
            "region_input_amount1": 75000,
            "region_input_amount2": 200000
            # Missing optional fields: market_type, description, notes
        }
        
        region_dir = self.regions_dir / "minimal_region"
        region_dir.mkdir()
        
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(minimal_config, f)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        config = manager.get_region_config("minimal_region")
        
        # Should use default values for missing optional fields
        assert config.market_type == "Unknown"
        assert config.description == ""
        assert config.notes == ""
    
    def test_large_number_of_regions(self):
        """Test handling large number of regions"""
        # Create 50 test regions
        for i in range(50):
            region_key = f"region_{i:03d}"
            config_data = {
                "region_name": f"Region {i}",
                "region_code": f"R{i:03d}",
                "fips_code": f"{i:05d}",
                "region_input_date1": "2009-01-01",
                "region_input_date2": "2019-01-01", 
                "region_input_amount1": 75000,
                "region_input_amount2": 200000,
                "market_type": "Test Market",
                "description": f"Test region {i}",
                "notes": ""
            }
            
            region_dir = self.regions_dir / region_key
            region_dir.mkdir()
            
            config_file = region_dir / "config.json"
            with open(config_file, 'w') as f:
                json.dump(config_data, f)
        
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        assert len(manager.configs) == 50
        regions = manager.list_regions()
        assert len(regions) == 50
        
        # Test that all regions are accessible
        for i in range(50):
            region_key = f"region_{i:03d}"
            config = manager.get_region_config(region_key)
            assert config.region_code == f"R{i:03d}"


class TestIntegrationScenarios:
    """Test suite for integration scenarios"""
    
    def setup_method(self):
        """Set up integration test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.regions_dir = Path(self.temp_dir) / "regions"
        self.regions_dir.mkdir()
    
    def teardown_method(self):
        """Clean up integration test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_full_region_setup_and_validation(self):
        """Test complete region setup and validation workflow"""
        # Create realistic region configuration
        roanoke_config = {
            "region_name": "Roanoke City, VA",
            "region_code": "ROA",
            "fips_code": "51770",
            "region_input_date1": "2009-01-01",
            "region_input_date2": "2019-01-01",
            "region_input_amount1": 75000,
            "region_input_amount2": 200000,
            "market_type": "Rural/Small City",
            "description": "Roanoke City market with conservative thresholds",
            "notes": "ABS1: 15+ year old properties, BUY: Recent buyers (5 years)"
        }
        
        region_dir = self.regions_dir / "roanoke_city_va"
        region_dir.mkdir()
        
        # Create config file
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(roanoke_config, f, indent=2)
        
        # Create realistic Excel files
        main_data = {
            'Owner 1 Last Name': ['Smith', 'Jones Trust', 'First Baptist'],
            'Owner 1 First Name': ['John', 'Family', 'Church'],
            'Address': ['123 Main St', '456 Oak Ave', '789 Church St'],
            'Mailing Address': ['123 Main St', '999 Different St', '789 Church St'],
            'Last Sale Date': ['2010-01-15', '2020-06-30', '2005-03-20'],
            'Last Sale Amount': [100000, 50000, 200000],
            'FIPS': ['51770'] * 3
        }
        df_main = pd.DataFrame(main_data)
        
        liens_data = {
            'Owner 1 Last Name': ['Brown'],
            'Owner 1 First Name': ['Robert'],
            'Address': ['321 Pine St'],
            'FIPS': ['51770'],
            'Lien Type': ['Tax Lien'],
            'Lien Amount': [5000]
        }
        df_liens = pd.DataFrame(liens_data)
        
        # Save Excel files
        main_file = region_dir / "main_region.xlsx"
        liens_file = region_dir / "liens.xlsx"
        df_main.to_excel(main_file, index=False)
        df_liens.to_excel(liens_file, index=False)
        
        # Initialize manager and run full validation
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        # Test configuration loading
        assert len(manager.configs) == 1
        config = manager.get_region_config("roanoke_city_va")
        assert config.region_name == "Roanoke City, VA"
        
        # Test file validation
        file_validation = manager.validate_region_files("roanoke_city_va")
        assert file_validation['valid'] == True
        assert file_validation['total_files'] == 2
        
        # Test FIPS validation
        fips_validation = manager.validate_fips_codes("roanoke_city_va")
        assert fips_validation['all_valid'] == True
        assert fips_validation['files_valid'] == 2
        
        # Test output directory creation
        output_dir = manager.create_output_directory("roanoke_city_va")
        assert output_dir.exists()
    
    def test_multi_region_batch_validation(self):
        """Test batch validation of multiple regions"""
        regions_to_create = [
            ("roanoke_city_va", "51770", "Roanoke City, VA"),
            ("virginia_beach_va", "51810", "Virginia Beach, VA"), 
            ("alexandria_va", "51510", "Alexandria, VA")
        ]
        
        for region_key, fips, name in regions_to_create:
            config_data = {
                "region_name": name,
                "region_code": region_key.upper()[:3],
                "fips_code": fips,
                "region_input_date1": "2009-01-01",
                "region_input_date2": "2019-01-01",
                "region_input_amount1": 75000,
                "region_input_amount2": 200000,
                "market_type": "Test Market",
                "description": f"Test region for {name}",
                "notes": ""
            }
            
            region_dir = self.regions_dir / region_key
            region_dir.mkdir()
            
            # Create config
            config_file = region_dir / "config.json"
            with open(config_file, 'w') as f:
                json.dump(config_data, f)
            
            # Create Excel files with correct FIPS
            test_data = {
                'Owner 1 Last Name': ['Test'],
                'Owner 1 First Name': ['Owner'],
                'Address': ['123 Test St'],
                'FIPS': [fips]
            }
            df = pd.DataFrame(test_data)
            
            main_file = region_dir / "main_region.xlsx"
            df.to_excel(main_file, index=False)
        
        # Initialize manager and test batch operations
        manager = MultiRegionConfigManager(str(self.regions_dir))
        
        assert len(manager.configs) == 3
        
        # Test listing all regions
        regions = manager.list_regions()
        assert len(regions) == 3
        region_names = [r['name'] for r in regions]
        assert "Roanoke City, VA" in region_names
        assert "Virginia Beach, VA" in region_names
        assert "Alexandria, VA" in region_names
        
        # Test validation for each region
        for region_key, _, _ in regions_to_create:
            file_validation = manager.validate_region_files(region_key)
            assert file_validation['valid'] == True
            
            fips_validation = manager.validate_fips_codes(region_key)
            assert fips_validation['all_valid'] == True


if __name__ == '__main__':
    # Run tests with pytest
    pytest.main([__file__, '-v'])