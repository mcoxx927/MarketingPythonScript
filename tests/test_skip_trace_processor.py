"""
Comprehensive Unit Tests for Skip Trace Processor

Tests the critical business logic for skip trace data integration including:
- Hybrid matching strategy (APN+FIPS primary, address fallback)
- Golden Address integration and difference detection
- Skip trace flag detection and priority code enhancement
- File operations and multi-region processing

Based on real-world requirements for ~21k records per region across 11 regions.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os

# Import functions from skip_trace_processor
from skip_trace_processor import (
    _normalize_address,
    _normalize_city,
    _create_address_city_key,
    _detect_skip_trace_flags, 
    _match_skip_trace_hybrid,
    process_region_skip_trace,
    find_enhanced_files
)

from multi_region_config import MultiRegionConfigManager, RegionConfig


class TestAddressNormalization:
    """Test address normalization for accurate matching"""
    
    def test_basic_address_normalization(self):
        """Test basic address normalization cases"""
        test_cases = [
            # Basic street type normalization
            ("123 MAIN ST,", "123 MAIN ST", "Remove trailing comma from ST"),
            ("456 ELM AVE,", "456 ELM AVE", "Remove trailing comma from AVE"),
            ("789 OAK RD,", "789 OAK RD", "Remove trailing comma from RD"),
            ("101 PINE DR,", "101 PINE DR", "Remove trailing comma from DR"),
            ("202 MAPLE BLVD,", "202 MAPLE BLVD", "Remove trailing comma from BLVD"),
            
            # Case normalization
            ("123 main street", "123 MAIN STREET", "Convert to uppercase"),
            ("456 Elm Avenue", "456 ELM AVENUE", "Mixed case normalization"),
            
            # Multiple spaces and commas
            ("123  MAIN   ST,  APT  2", "123 MAIN ST APT 2", "Collapse multiple spaces, remove commas"),
            ("456,ELM,AVE", "456 ELM AVE", "Replace commas with spaces"),
            
            # Edge cases
            ("", "", "Empty string"),
            ("   ", "", "Whitespace only"),
            ("123", "123", "Single element"),
        ]
        
        for input_addr, expected, description in test_cases:
            result = _normalize_address(input_addr)
            assert result == expected, f"Failed: {description} - '{input_addr}' -> '{result}' (expected '{expected}')"
    
    def test_address_normalization_edge_cases(self):
        """Test edge cases for address normalization"""
        edge_cases = [
            (None, "", "None input"),
            (np.nan, "", "NaN input"),
            (123, "123", "Numeric input"),
            ("", "", "Empty string"),
            ("   \t\n  ", "", "Whitespace and tabs"),
        ]
        
        for input_val, expected, description in edge_cases:
            result = _normalize_address(input_val)
            assert result == expected, f"Failed: {description}"
    
    def test_real_world_addresses(self):
        """Test with real-world address variations"""
        real_world_cases = [
            # Common variations that should normalize to same result
            ("123 MAIN ST", "123 MAIN ST", "Standard format"),
            ("123 MAIN ST,", "123 MAIN ST", "With trailing comma"),
            ("123  MAIN   ST", "123 MAIN ST", "Multiple spaces"),
            ("123 main st", "123 MAIN ST", "Lowercase"),
            
            # Complex addresses
            ("1234 ROANOKE VALLEY RD, APT B", "1234 ROANOKE VALLEY RD APT B", "Complex address with apartment"),
            ("987 COLONIAL AVE, UNIT 12", "987 COLONIAL AVE UNIT 12", "Address with unit"),
        ]
        
        for input_addr, expected, description in real_world_cases:
            result = _normalize_address(input_addr)
            assert result == expected, f"Failed: {description}"


class TestCityNormalization:
    """Test city normalization for address+city matching"""
    
    def test_basic_city_normalization(self):
        """Test basic city name normalization"""
        test_cases = [
            # Basic normalization
            ("roanoke", "ROANOKE", "Uppercase conversion"),
            ("Vinton", "VINTON", "Mixed case normalization"),
            ("SALEM", "SALEM", "Already uppercase"),
            
            # Remove periods and extra spaces
            ("St. Petersburg", "ST PETERSBURG", "Remove periods"),
            ("Mt.  Pleasant", "MT PLEASANT", "Remove periods and collapse spaces"),
            ("  Roanoke  ", "ROANOKE", "Trim whitespace"),
            
            # Edge cases
            ("", "", "Empty string"),
            (None, "", "None input"),
            (123, "123", "Numeric input")
        ]
        
        for input_city, expected, description in test_cases:
            result = _normalize_city(input_city)
            assert result == expected, f"Failed: {description} - '{input_city}' -> '{result}' (expected '{expected}')"
    
    def test_create_address_city_key(self):
        """Test address+city key creation for matching"""
        test_cases = [
            # Normal address+city combinations
            ("123 Main St", "Roanoke", "123 MAIN ST|ROANOKE", "Normal address+city"),
            ("456 ELM AVE,", "vinton", "456 ELM AVE|VINTON", "With comma and mixed case city"),
            ("789  OAK  RD", "  SALEM  ", "789 OAK RD|SALEM", "Multiple spaces in both"),
            
            # Address only (fallback)
            ("123 Main St", "", "123 MAIN ST", "Empty city falls back to address only"),
            ("456 ELM AVE", None, "456 ELM AVE", "None city falls back to address only"),
            
            # Edge cases
            ("", "Roanoke", "", "Empty address returns empty"),
            ("", "", "", "Both empty returns empty"),
            (None, None, "", "Both None returns empty")
        ]
        
        for address, city, expected, description in test_cases:
            result = _create_address_city_key(address, city)
            assert result == expected, f"Failed: {description} - '{address}' + '{city}' -> '{result}' (expected '{expected}')"


class TestSkipTraceFlags:
    """Test skip trace flag detection and processing"""
    
    def test_flag_detection_true_values(self):
        """Test detection of actual data format"""
        from datetime import datetime
        true_test_cases = [
            # Owner Is Deceased - numeric format (Excel TRUE/FALSE -> 1.0/0.0)
            {"Owner Is Deceased": 1.0, "expected": ["STDeceased"]},
            {"Owner Is Deceased": "True", "expected": ["STDeceased"]},  # Fallback string format
            
            # Date-based distress indicators - actual datetime objects
            {"Owner Bankruptcy": datetime(2024, 1, 15), "expected": ["STBankruptcy"]},
            {"Lien": datetime(2023, 5, 10), "expected": ["STLien"]},
            {"Judgment": datetime(2022, 12, 1), "expected": ["STJudgment"]},
            
            # Multiple flags - mixed format
            {
                "Owner Bankruptcy": datetime(2024, 3, 1), 
                "Lien": datetime(2023, 8, 15), 
                "Owner Is Deceased": 1.0,
                "expected": ["STDeceased", "STBankruptcy", "STLien"]  # STDeceased first due to processing order
            },
            
            # All possible flags
            {
                "Owner Bankruptcy": datetime(2024, 1, 1),
                "Owner Foreclosure": datetime(2023, 6, 1), 
                "Lien": datetime(2022, 12, 1),
                "Judgment": datetime(2021, 5, 1),
                "Quitclaim": datetime(2020, 8, 1),
                "Owner Is Deceased": 1.0,
                "expected": ["STDeceased", "STBankruptcy", "STForeclosure", "STLien", "STJudgment", "STQuitclaim"]
            }
        ]
        
        for test_data in true_test_cases:
            expected = test_data.pop("expected")
            row = pd.Series(test_data)
            result = _detect_skip_trace_flags(row)
            
            # Sort both lists for comparison since order doesn't matter
            result_sorted = sorted(result)
            expected_sorted = sorted(expected)
            
            assert result_sorted == expected_sorted, f"Failed flag detection: {test_data} -> {result} (expected {expected})"
    
    def test_flag_detection_false_values(self):
        """Test that false/empty values don't trigger flags"""
        false_test_cases = [
            # Owner Is Deceased - 0.0 means not deceased
            {"Owner Is Deceased": 0.0, "expected": []},
            {"Owner Is Deceased": "False", "expected": []},
            {"Owner Is Deceased": None, "expected": []},
            {"Owner Is Deceased": np.nan, "expected": []},
            
            # Date-based - "No Data" and NaN should not trigger flags
            {"Owner Bankruptcy": "No Data", "expected": []},
            {"Owner Bankruptcy": None, "expected": []},
            {"Owner Bankruptcy": np.nan, "expected": []},
            {"Lien": "No Data", "expected": []},
            {"Judgment": "", "expected": []},
            
            # Mixed true/false - only deceased should trigger
            {
                "Owner Is Deceased": 1.0,
                "Owner Bankruptcy": "No Data", 
                "Lien": None,
                "expected": ["STDeceased"]
            },
        ]
        
        for test_data in false_test_cases:
            expected = test_data.pop("expected")
            row = pd.Series(test_data)
            result = _detect_skip_trace_flags(row)
            assert result == expected, f"Failed flag detection: {test_data} -> {result} (expected {expected})"
    
    def test_missing_columns(self):
        """Test behavior when skip trace columns are missing"""
        # Row with no skip trace columns
        row = pd.Series({"Other Column": "value"})
        result = _detect_skip_trace_flags(row)
        assert result == [], "Should return empty list when no skip trace columns present"
        
        # Row with only some skip trace columns
        row = pd.Series({"Owner Bankruptcy": datetime(2023, 1, 15), "Some Other Column": "value"})
        result = _detect_skip_trace_flags(row)
        assert result == ["STBankruptcy"], "Should detect available columns only"


class TestHybridMatching:
    """Test the hybrid matching strategy (APN+FIPS primary, address fallback)"""
    
    def setup_method(self):
        """Setup test data for matching tests"""
        # Create sample enhanced data (main region file)
        self.enhanced_df = pd.DataFrame({
            'APN': ['12345', '67890', '11111', '22222'],
            'Address': ['123 MAIN ST', '456 ELM AVE', '789 OAK RD', '101 PINE DR'],
            'Mailing Address': ['123 MAIN ST', '456 ELM AVE', '789 OAK RD', '101 PINE DR'],
            'PriorityCode': ['ABS1', 'BUY2', 'OIN1', 'DEFAULT'],
            'PriorityName': ['ABS1 - High Priority', 'BUY2 - Buyer', 'OIN1 - Owner', 'Default']
        })
        
        # Create sample skip trace data
        self.skip_trace_df = pd.DataFrame({
            'Property APN': ['12345', '99999', '11111'],  # 12345 and 11111 match, 99999 doesn't
            'Property FIPS': ['051770', '051770', '051770'],  # All same FIPS
            'Property Address': ['123 MAIN ST', '999 NEW ST', '789 OAK RD'],
            'Golden Address': ['123 MAIN ST UNIT A', '999 NEW ST', '789 OAK RD'],  # Golden differs for first record
            'Owner Bankruptcy': [datetime(2023, 1, 15), 'No Data', 'No Data'],
            'Lien': ['No Data', datetime(2023, 2, 15), 'No Data'],
            'Judgment': ['No Data', 'No Data', datetime(2023, 3, 15)]
        })
        
        self.region_fips = '051770'
    
    def test_apn_fips_matching(self):
        """Test primary APN+FIPS matching strategy"""
        result_df = _match_skip_trace_hybrid(
            self.enhanced_df.copy(), 
            self.skip_trace_df.copy(), 
            self.region_fips
        )
        
        # Check APN matches (records 0 and 2 should match)
        assert pd.notna(result_df.loc[0, 'Golden_Address']), "First record should have Golden Address from APN match"
        assert result_df.loc[0, 'Golden_Address'] == '123 MAIN ST UNIT A', "Golden Address should match skip trace data"
        assert result_df.loc[0, 'Golden_Address_Differs'] == True, "Golden Address should differ from original"
        assert result_df.loc[0, 'ST_Flags'] == 'STBankruptcy', "Should have bankruptcy flag"
        
        assert pd.notna(result_df.loc[2, 'Golden_Address']), "Third record should have Golden Address from APN match"
        assert result_df.loc[2, 'ST_Flags'] == 'STJudgment', "Should have judgment flag"
        
        # Check non-matches (records 1 and 3 should not match)
        assert pd.isna(result_df.loc[1, 'Golden_Address']) or result_df.loc[1, 'Golden_Address'] == '', "Second record should not match"
        assert pd.isna(result_df.loc[3, 'Golden_Address']) or result_df.loc[3, 'Golden_Address'] == '', "Fourth record should not match"
    
    def test_address_fallback_matching(self):
        """Test address-based fallback matching when APN doesn't match"""
        # Remove APN column to force address matching
        enhanced_no_apn = self.enhanced_df.drop(columns=['APN']).copy()
        skip_trace_no_apn = self.skip_trace_df.drop(columns=['Property APN']).copy()
        
        result_df = _match_skip_trace_hybrid(
            enhanced_no_apn, 
            skip_trace_no_apn, 
            self.region_fips
        )
        
        # Should match on address normalization
        assert pd.notna(result_df.loc[0, 'Golden_Address']), "Should match on address"
        assert result_df.loc[0, 'ST_Flags'] == 'STBankruptcy', "Should have bankruptcy flag from address match"
    
    def test_fips_filtering(self):
        """Test that skip trace data is properly filtered by FIPS code"""
        # Add records with different FIPS codes
        skip_trace_mixed_fips = self.skip_trace_df.copy()
        skip_trace_mixed_fips.loc[len(skip_trace_mixed_fips)] = {
            'Property APN': '12345', 
            'Property FIPS': '999999',  # Different FIPS
            'Property Address': '123 MAIN ST',
            'Golden Address': 'WRONG GOLDEN ADDRESS',
            'Owner Bankruptcy': datetime(2023, 1, 15),
            'Lien': 'No Data',
            'Judgment': 'No Data'
        }
        
        result_df = _match_skip_trace_hybrid(
            self.enhanced_df.copy(), 
            skip_trace_mixed_fips, 
            self.region_fips
        )
        
        # Should only match records with correct FIPS
        assert result_df.loc[0, 'Golden_Address'] != 'WRONG GOLDEN ADDRESS', "Should not match record with wrong FIPS"
        assert result_df.loc[0, 'Golden_Address'] == '123 MAIN ST UNIT A', "Should match correct FIPS record"
    
    def test_golden_address_differs_logic(self):
        """Test Golden_Address_Differs flag logic"""
        test_cases = [
            # Golden same as original -> False
            ('123 MAIN ST', '123 MAIN ST', False),
            # Golden different -> True  
            ('123 MAIN ST', '123 MAIN ST UNIT A', True),
            # Case differences -> True (because strings are different)
            ('123 main st', '123 MAIN ST', True),
            # Whitespace differences -> True
            ('123 MAIN ST', '123  MAIN  ST', True),
        ]
        
        for original, golden, expected_differs in test_cases:
            enhanced_test = pd.DataFrame({
                'APN': ['12345'],
                'Address': ['123 MAIN ST'],
                'Mailing Address': [original],
                'PriorityCode': ['ABS1'],
                'PriorityName': ['Test']
            })
            
            skip_trace_test = pd.DataFrame({
                'Property APN': ['12345'],
                'Property FIPS': [self.region_fips],
                'Property Address': ['123 MAIN ST'],
                'Golden Address': [golden],
                'Owner Bankruptcy': ['False'],
                'Lien': ['False'],
                'Judgment': ['False']
            })
            
            result_df = _match_skip_trace_hybrid(enhanced_test, skip_trace_test, self.region_fips)
            
            actual_differs = result_df.loc[0, 'Golden_Address_Differs']
            assert actual_differs == expected_differs, f"Golden Address differs test failed: '{original}' vs '{golden}' -> {actual_differs} (expected {expected_differs})"
    
    def test_priority_code_enhancement(self):
        """Test that priority codes are properly enhanced with skip trace flags"""
        result_df = _match_skip_trace_hybrid(
            self.enhanced_df.copy(), 
            self.skip_trace_df.copy(), 
            self.region_fips
        )
        
        # Check priority code updates
        assert result_df.loc[0, 'PriorityCode'] == 'STBankruptcy-ABS1', "Priority code should be enhanced with ST flag"
        assert 'STBankruptcy Enhanced' in result_df.loc[0, 'PriorityName'], "Priority name should be enhanced"
        
        # Check record with no flags doesn't change
        original_priority = self.enhanced_df.loc[1, 'PriorityCode']
        assert result_df.loc[1, 'PriorityCode'] == original_priority, "Priority code should not change without flags"
    
    def test_empty_skip_trace_data(self):
        """Test behavior with empty skip trace data"""
        empty_skip_trace = pd.DataFrame(columns=['Property FIPS', 'Property Address', 'Golden Address'])
        
        result_df = _match_skip_trace_hybrid(
            self.enhanced_df.copy(), 
            empty_skip_trace, 
            self.region_fips
        )
        
        # Should add empty skip trace columns
        assert 'Golden_Address' in result_df.columns, "Should add Golden_Address column"
        assert 'Golden_Address_Differs' in result_df.columns, "Should add Golden_Address_Differs column"
        assert 'ST_Flags' in result_df.columns, "Should add ST_Flags column"
        
        # All values should be empty/false
        assert result_df['Golden_Address'].isna().all(), "All Golden_Address should be null"
        assert (result_df['Golden_Address_Differs'] == False).all(), "All Golden_Address_Differs should be False"
        assert (result_df['ST_Flags'] == '').all(), "All ST_Flags should be empty"
    
    def test_no_fips_match_skip_trace(self):
        """Test behavior when skip trace has no records for this region's FIPS"""
        # Skip trace with different FIPS
        wrong_fips_skip_trace = self.skip_trace_df.copy()
        wrong_fips_skip_trace['Property FIPS'] = '999999'  # Different FIPS
        
        result_df = _match_skip_trace_hybrid(
            self.enhanced_df.copy(), 
            wrong_fips_skip_trace, 
            self.region_fips
        )
        
        # Should add columns but no matches
        assert 'Golden_Address' in result_df.columns
        assert result_df['Golden_Address'].isna().all(), "No Golden Addresses should be populated"
        assert (result_df['Golden_Address_Differs'] == False).all(), "No differs flags should be True"
        assert (result_df['ST_Flags'] == '').all(), "No ST flags should be set"


class TestFileOperations:
    """Test file discovery and processing operations"""
    
    def test_find_enhanced_files(self):
        """Test enhanced file discovery functionality"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock config manager
            config_manager = Mock()
            
            # Create test directory structure
            region_key = 'test_region'
            output_dir = Path(temp_dir) / 'output' / region_key
            month1_dir = output_dir / '2024_01'
            month2_dir = output_dir / '2024_02'
            month1_dir.mkdir(parents=True)
            month2_dir.mkdir(parents=True)
            
            # Create test files with different timestamps
            file1 = month1_dir / 'test_main_region_enhanced_20240115.xlsx'
            file2 = month1_dir / 'test_main_region_enhanced_20240120.xlsx'
            file3 = month2_dir / 'test_main_region_enhanced_20240205.xlsx'
            file4 = month2_dir / 'other_file.xlsx'  # Should not match pattern
            
            # Create files (empty is fine for test)
            for file in [file1, file2, file3, file4]:
                file.touch()
            
            # Make file3 newest by modifying timestamp
            import time
            time.sleep(0.1)  # Ensure different timestamps
            file3.touch()
            
            # Test file discovery directly with actual path
            with patch('skip_trace_processor.Path') as mock_path_class:
                def path_side_effect(path_str):
                    if path_str == "output":
                        return Path(temp_dir) / "output"
                    return Path(path_str)
                
                mock_path_class.side_effect = path_side_effect
                
                found_files = find_enhanced_files(region_key, config_manager)
                
                # Should find files matching pattern, sorted by newest first
                assert len(found_files) == 3, f"Should find 3 enhanced files, found {len(found_files)}"
                
                # Check file names are in the results
                found_names = [f.name for f in found_files]
                assert 'test_main_region_enhanced_20240205.xlsx' in found_names
                assert 'test_main_region_enhanced_20240120.xlsx' in found_names
                assert 'test_main_region_enhanced_20240115.xlsx' in found_names
                assert 'other_file.xlsx' not in found_names
    
    def test_find_enhanced_files_no_directory(self):
        """Test behavior when output directory doesn't exist"""
        config_manager = Mock()
        
        # Use a region that definitely doesn't exist in temp directory
        found_files = find_enhanced_files('definitely_nonexistent_region_12345', config_manager)
        
        assert found_files == [], "Should return empty list for non-existent directory"


class TestRegionProcessing:
    """Test region-level processing workflow"""
    
    def setup_method(self):
        """Setup mock configuration and test data"""
        self.mock_config = RegionConfig(
            region_name="Test Region",
            region_code="TST",
            fips_code="123456",
            region_input_date1=datetime(2020, 1, 1),
            region_input_date2=datetime(2024, 1, 1),
            region_input_amount1=50000,
            region_input_amount2=300000,
            market_type="Test Market",
            description="Test Description",
            notes="Test Notes"
        )
        
        self.mock_config_manager = Mock(spec=MultiRegionConfigManager)
        self.mock_config_manager.get_region_config.return_value = self.mock_config
    
    def test_region_processing_success(self):
        """Test successful region processing workflow"""
        # Use the end-to-end test pattern which already works
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test enhanced file
            enhanced_file = Path(temp_dir) / "test_enhanced.xlsx"
            enhanced_data = pd.DataFrame({
                'APN': ['12345'],
                'Address': ['123 MAIN ST'],
                'Mailing Address': ['123 MAIN ST'],
                'PriorityCode': ['ABS1'],
                'PriorityName': ['Test Priority']
            })
            enhanced_data.to_excel(enhanced_file, index=False)
            
            # Create test skip trace file
            skip_trace_file = Path(temp_dir) / "test_skip_trace.xlsx"
            skip_trace_data = pd.DataFrame({
                'APN': ['12345'],
                'FIPS': ['123456'],
                'Address': ['123 MAIN ST'],
                'Golden Address': ['123 MAIN ST UNIT A'],
                'Owner Bankruptcy': [datetime(2023, 1, 15)],
                'Lien': ['No Data'],
                'Judgment': ['No Data']
            })
            skip_trace_data.to_excel(skip_trace_file, index=False)
            
            # Run processing
            result = process_region_skip_trace(
                'test_region',
                str(enhanced_file),
                str(skip_trace_file),
                self.mock_config_manager
            )
            
            # Check success result
            assert result['success'] == True, "Processing should succeed"
            assert result['region_name'] == "Test Region"
            assert result['total_records'] == 1
            assert result['golden_address_count'] == 1
            assert result['golden_differs_count'] == 1
            assert result['st_flags_count'] == 1
    
    def test_region_processing_missing_enhanced_file(self):
        """Test handling of missing enhanced file"""
        result = process_region_skip_trace(
            'test_region',
            'definitely_missing_file_12345.xlsx',
            'test_skip_trace.xlsx',
            self.mock_config_manager
        )
        
        assert result['success'] == False, "Should fail when enhanced file missing"
        assert 'not found' in result['error'].lower(), "Error should mention file not found"
    
    def test_region_processing_missing_skip_trace_columns(self):
        """Test handling of skip trace file with missing required columns"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test enhanced file
            enhanced_file = Path(temp_dir) / "test_enhanced.xlsx"
            enhanced_data = pd.DataFrame({'Address': ['123 MAIN ST']})
            enhanced_data.to_excel(enhanced_file, index=False)
            
            # Create skip trace file missing required columns
            skip_trace_file = Path(temp_dir) / "bad_skip_trace.xlsx"
            skip_trace_data = pd.DataFrame({'Some Other Column': ['value']})
            skip_trace_data.to_excel(skip_trace_file, index=False)
            
            result = process_region_skip_trace(
                'test_region',
                str(enhanced_file),
                str(skip_trace_file),
                self.mock_config_manager
            )
            
            assert result['success'] == False, "Should fail when required columns missing"
            assert 'missing required columns' in result['error'].lower(), "Error should mention missing columns"


class TestPerformanceAndMemory:
    """Test performance with realistic dataset sizes"""
    
    def test_large_dataset_performance(self):
        """Test performance with realistic dataset size (~21k records)"""
        # Create large test dataset
        size = 21000  # Realistic size for one region
        
        enhanced_df = pd.DataFrame({
            'APN': [f'APN_{i:05d}' for i in range(size)],
            'Address': [f'{100 + i} MAIN ST' for i in range(size)],
            'Mailing Address': [f'{100 + i} MAIN ST' for i in range(size)],
            'PriorityCode': ['ABS1'] * size,
            'PriorityName': ['Test Priority'] * size
        })
        
        # Skip trace data with 10% match rate (realistic)
        st_size = 2100  # 10% of enhanced records
        skip_trace_df = pd.DataFrame({
            'APN': [f'APN_{i:05d}' for i in range(0, st_size)],  # First 10% match
            'FIPS': ['123456'] * st_size,
            'Address': [f'{100 + i} MAIN ST' for i in range(st_size)],
            'Golden Address': [f'{100 + i} MAIN ST UNIT A' for i in range(st_size)],
            'Owner Bankruptcy': [datetime(2023, 1, 15)] * st_size,
            'Lien': ['No Data'] * st_size,
            'Judgment': ['No Data'] * st_size
        })
        
        # Time the matching process
        start_time = datetime.now()
        
        result_df = _match_skip_trace_hybrid(enhanced_df, skip_trace_df, '123456')
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        # Performance assertions
        assert processing_time < 30.0, f"Large dataset processing took too long: {processing_time:.2f} seconds"
        assert len(result_df) == size, "Should return all input records"
        
        # Check match results
        golden_count = result_df['Golden_Address'].notna().sum()
        st_flags_count = (result_df['ST_Flags'] != '').sum()
        
        assert golden_count == st_size, f"Should have {st_size} Golden Addresses, got {golden_count}"
        assert st_flags_count == st_size, f"Should have {st_size} ST flags, got {st_flags_count}"
        
        print(f"Performance test: Processed {size:,} records with {st_size:,} matches in {processing_time:.2f} seconds")
    
    def test_memory_efficiency(self):
        """Test memory efficiency with large datasets"""
        # This test ensures we're not creating excessive temporary objects
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Create moderately large dataset
        size = 10000
        enhanced_df = pd.DataFrame({
            'APN': [f'APN_{i:05d}' for i in range(size)],
            'Address': [f'{100 + i} MAIN ST' for i in range(size)],
            'Mailing Address': [f'{100 + i} MAIN ST' for i in range(size)],
            'PriorityCode': ['ABS1'] * size,
            'PriorityName': ['Test Priority'] * size
        })
        
        skip_trace_df = pd.DataFrame({
            'APN': [f'APN_{i:05d}' for i in range(1000)],
            'FIPS': ['123456'] * 1000,
            'Address': [f'{100 + i} MAIN ST' for i in range(1000)],
            'Golden Address': [f'{100 + i} MAIN ST UNIT A' for i in range(1000)],
            'Owner Bankruptcy': [datetime(2023, 1, 15)] * 1000,
            'Lien': ['No Data'] * 1000,
            'Judgment': ['No Data'] * 1000
        })
        
        # Process data
        result_df = _match_skip_trace_hybrid(enhanced_df, skip_trace_df, '123456')
        
        final_memory = process.memory_info().rss
        memory_increase = (final_memory - initial_memory) / 1024 / 1024  # MB
        
        # Memory should not increase excessively (allowing for reasonable overhead)
        assert memory_increase < 500, f"Memory usage increased by {memory_increase:.1f}MB - too much for dataset size"
        
        # Verify results are correct
        assert len(result_df) == size, "Should return all records"
        
        print(f"Memory test: Processed {size:,} records, memory increase: {memory_increase:.1f}MB")


class TestErrorHandling:
    """Test error handling and edge cases"""
    
    def test_corrupt_excel_file_handling(self):
        """Test handling of corrupt or unreadable Excel files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a fake "Excel" file that's actually text
            fake_excel = Path(temp_dir) / "corrupt.xlsx"
            fake_excel.write_text("This is not an Excel file")
            
            mock_config = Mock()
            mock_config.region_name = "Test Region"
            mock_config.region_code = "TST"
            mock_config.fips_code = "123456"
            
            mock_config_manager = Mock()
            mock_config_manager.get_region_config.return_value = mock_config
            
            # This should handle the Excel reading error gracefully
            result = process_region_skip_trace(
                'test_region',
                str(fake_excel),
                str(fake_excel),
                mock_config_manager
            )
            
            assert result['success'] == False, "Should fail gracefully with corrupt file"
            assert 'error' in result, "Should return error information"
    
    def test_invalid_fips_codes(self):
        """Test handling of invalid or mismatched FIPS codes"""
        enhanced_df = pd.DataFrame({
            'Address': ['123 MAIN ST'],
            'Mailing Address': ['123 MAIN ST'],
            'PriorityCode': ['ABS1'],
            'PriorityName': ['Test']
        })
        
        # Skip trace with different FIPS
        skip_trace_df = pd.DataFrame({
            'FIPS': ['999999'],  # Wrong FIPS
            'Address': ['123 MAIN ST'],
            'Golden Address': ['123 MAIN ST'],
            'Owner Bankruptcy': [datetime(2023, 1, 15)],
            'Lien': ['No Data'],
            'Judgment': ['No Data']
        })
        
        result_df = _match_skip_trace_hybrid(enhanced_df, skip_trace_df, '123456')
        
        # Should handle gracefully and return no matches
        assert len(result_df) == len(enhanced_df), "Should return all original records"
        assert result_df['Golden_Address'].isna().all(), "No matches should be made with wrong FIPS"
    
    def test_malformed_data_handling(self):
        """Test handling of malformed data in skip trace files"""
        enhanced_df = pd.DataFrame({
            'Address': ['123 MAIN ST'],
            'Mailing Address': ['123 MAIN ST'],
            'PriorityCode': ['ABS1'],
            'PriorityName': ['Test']
        })
        
        # Skip trace with malformed data
        skip_trace_df = pd.DataFrame({
            'Property FIPS': [None],  # Null FIPS
            'Property Address': [None],  # Null address
            'Golden Address': [''],  # Empty golden address
            'Owner Bankruptcy': ['Maybe'],  # Invalid boolean
            'Lien': [123],  # Numeric instead of text
            'Judgment': [None]  # Null judgment
        })
        
        # Should handle gracefully without crashing
        result_df = _match_skip_trace_hybrid(enhanced_df, skip_trace_df, '123456')
        
        assert len(result_df) == len(enhanced_df), "Should return all original records"
        # Should not crash and should handle malformed data gracefully


class TestCommandLineInterface:
    """Test command line interface and argument handling"""
    
    def test_argument_parsing_single_region(self):
        """Test argument parsing for single region processing"""
        from skip_trace_processor import main
        
        test_args = [
            'skip_trace_processor.py',
            '--region', 'test_region',
            '--skip-trace-file', 'test_skip_trace.xlsx'
        ]
        
        with patch('sys.argv', test_args):
            with patch('skip_trace_processor.MultiRegionConfigManager') as mock_config_manager:
                with patch('skip_trace_processor.find_enhanced_files') as mock_find_files:
                    with patch('skip_trace_processor.process_region_skip_trace') as mock_process:
                        
                        # Setup mocks
                        mock_find_files.return_value = [Path('test_enhanced.xlsx')]
                        mock_process.return_value = {'success': True}
                        
                        # Should not raise any exceptions
                        try:
                            main()
                            assert True, "Main should execute without errors"
                        except SystemExit as e:
                            # exit(0) is success
                            assert e.code == 0 or e.code is None, f"Should exit successfully, got code {e.code}"
                        except Exception as e:
                            pytest.fail(f"Unexpected exception: {e}")
    
    def test_argument_parsing_all_regions(self):
        """Test argument parsing for all regions processing"""
        from skip_trace_processor import main
        
        test_args = [
            'skip_trace_processor.py',
            '--all-regions',
            '--skip-trace-file', 'test_skip_trace.xlsx'
        ]
        
        with patch('sys.argv', test_args):
            with patch('skip_trace_processor.MultiRegionConfigManager') as mock_config_manager:
                with patch('skip_trace_processor.find_enhanced_files') as mock_find_files:
                    with patch('skip_trace_processor.process_region_skip_trace') as mock_process:
                        
                        # Setup mocks
                        mock_config_manager.return_value.configs = {'region1': Mock(), 'region2': Mock()}
                        mock_find_files.return_value = [Path('test_enhanced.xlsx')]
                        mock_process.return_value = {'success': True}
                        
                        try:
                            main()
                            assert True, "Main should execute without errors"
                        except SystemExit as e:
                            assert e.code == 0 or e.code is None, f"Should exit successfully, got code {e.code}"
                        except Exception as e:
                            pytest.fail(f"Unexpected exception: {e}")
    
    def test_missing_required_arguments(self):
        """Test handling of missing required arguments"""
        from skip_trace_processor import main
        import argparse
        
        # Test missing skip-trace-file
        test_args = [
            'skip_trace_processor.py',
            '--region', 'test_region'
            # Missing --skip-trace-file
        ]
        
        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()  # Should exit due to missing required argument
    
    def test_mutually_exclusive_arguments(self):
        """Test that --region and --all-regions are mutually exclusive"""
        from skip_trace_processor import main
        
        test_args = [
            'skip_trace_processor.py',
            '--region', 'test_region',
            '--all-regions',  # These are mutually exclusive
            '--skip-trace-file', 'test_skip_trace.xlsx'
        ]
        
        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()  # Should exit due to mutually exclusive arguments
    
    def test_auto_file_discovery(self):
        """Test automatic enhanced file discovery when not specified"""
        from skip_trace_processor import main
        
        test_args = [
            'skip_trace_processor.py',
            '--region', 'test_region',
            '--skip-trace-file', 'test_skip_trace.xlsx'
            # No --enhanced-file specified
        ]
        
        with patch('sys.argv', test_args):
            with patch('skip_trace_processor.MultiRegionConfigManager') as mock_config_manager:
                with patch('skip_trace_processor.find_enhanced_files') as mock_find_files:
                    with patch('skip_trace_processor.process_region_skip_trace') as mock_process:
                        
                        # Setup mocks
                        mock_find_files.return_value = [Path('auto_found_enhanced.xlsx')]
                        mock_process.return_value = {'success': True}
                        
                        try:
                            main()
                            
                            # Verify find_enhanced_files was called for auto-discovery
                            mock_find_files.assert_called_once()
                            # Verify process was called with auto-discovered file
                            call_args = mock_process.call_args
                            assert 'auto_found_enhanced.xlsx' in call_args[0][1]
                            
                        except SystemExit as e:
                            assert e.code == 0 or e.code is None
    
    def test_no_enhanced_files_found(self):
        """Test handling when no enhanced files are found"""
        from skip_trace_processor import main
        
        test_args = [
            'skip_trace_processor.py',
            '--region', 'test_region',
            '--skip-trace-file', 'test_skip_trace.xlsx'
        ]
        
        with patch('sys.argv', test_args):
            with patch('skip_trace_processor.MultiRegionConfigManager'):
                with patch('skip_trace_processor.find_enhanced_files') as mock_find_files:
                    with patch('builtins.print') as mock_print:
                        
                        # Setup mocks - no files found
                        mock_find_files.return_value = []
                        
                        with pytest.raises(SystemExit) as exc_info:
                            main()
                        
                        # Should exit with error code
                        assert exc_info.value.code == 1
                        
                        # Should print error message
                        mock_print.assert_any_call("ERROR: No enhanced files found for region test_region")
    
    def test_processing_failure_exit_code(self):
        """Test that processing failures result in proper exit codes"""
        from skip_trace_processor import main
        
        test_args = [
            'skip_trace_processor.py',
            '--region', 'test_region',
            '--skip-trace-file', 'test_skip_trace.xlsx'
        ]
        
        with patch('sys.argv', test_args):
            with patch('skip_trace_processor.MultiRegionConfigManager'):
                with patch('skip_trace_processor.find_enhanced_files') as mock_find_files:
                    with patch('skip_trace_processor.process_region_skip_trace') as mock_process:
                        
                        # Setup mocks
                        mock_find_files.return_value = [Path('test_enhanced.xlsx')]
                        mock_process.return_value = {'success': False, 'error': 'Test error'}
                        
                        with pytest.raises(SystemExit) as exc_info:
                            main()
                        
                        # Should exit with error code 1
                        assert exc_info.value.code == 1
    
    def test_batch_processing_summary(self):
        """Test batch processing summary output"""
        from skip_trace_processor import main
        
        test_args = [
            'skip_trace_processor.py',
            '--all-regions',
            '--skip-trace-file', 'test_skip_trace.xlsx'
        ]
        
        with patch('sys.argv', test_args):
            with patch('skip_trace_processor.MultiRegionConfigManager') as mock_config_manager:
                with patch('skip_trace_processor.find_enhanced_files') as mock_find_files:
                    with patch('skip_trace_processor.process_region_skip_trace') as mock_process:
                        with patch('builtins.print') as mock_print:
                            
                            # Setup mocks
                            mock_config_manager.return_value.configs = {
                                'region1': Mock(), 
                                'region2': Mock(),
                                'region3': Mock()
                            }
                            mock_find_files.side_effect = [
                                [Path('region1_enhanced.xlsx')],  # region1 has files
                                [Path('region2_enhanced.xlsx')],  # region2 has files  
                                []  # region3 has no files
                            ]
                            mock_process.side_effect = [
                                {'success': True, 'total_records': 1000, 'golden_address_count': 100, 'st_flags_count': 50},  # region1 success
                                {'success': False, 'error': 'Test error'}  # region2 failure
                            ]
                            
                            try:
                                main()
                            except SystemExit:
                                pass
                            
                            # Check that summary was printed
                            print_calls = [call.args[0] for call in mock_print.call_args_list if call.args]
                            summary_found = any('[SUMMARY]' in str(call) for call in print_calls)
                            assert summary_found, "Should print batch processing summary"
                            
                            # Check success/failure counts in output
                            success_found = any('Successfully processed: 1' in str(call) for call in print_calls)
                            failed_found = any('Failed: 2' in str(call) for call in print_calls)  # region2 failed, region3 no files
                            assert success_found, "Should report successful regions"
                            assert failed_found, "Should report failed regions"


class TestIntegrationScenarios:
    """Test complete integration scenarios"""
    
    def test_end_to_end_single_region_workflow(self):
        """Test complete end-to-end workflow for single region"""
        from datetime import datetime
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test enhanced file
            enhanced_file = Path(temp_dir) / "test_enhanced.xlsx"
            enhanced_data = pd.DataFrame({
                'APN': ['12345', '67890'],
                'Address': ['123 MAIN ST', '456 ELM AVE'],
                'Mailing Address': ['123 MAIN ST', '456 ELM AVE'],
                'PriorityCode': ['ABS1', 'BUY2'],
                'PriorityName': ['ABS1 Priority', 'BUY2 Priority']
            })
            enhanced_data.to_excel(enhanced_file, index=False)
            
            # Create test skip trace file
            skip_trace_file = Path(temp_dir) / "test_skip_trace.xlsx"
            skip_trace_data = pd.DataFrame({
                'Property APN': ['12345'],
                'Property FIPS': ['123456'],
                'Property Address': ['123 MAIN ST'],
                'Golden Address': ['123 MAIN ST UNIT A'],
                'Owner Bankruptcy': [datetime(2024, 1, 15)],  # Date indicates bankruptcy
                'Lien': ['No Data'],  # No lien
                'Judgment': ['No Data'],  # No judgment
                'Owner Foreclosure': ['No Data'],  # No foreclosure
                'Quitclaim': [None],  # No quitclaim
                'Owner Is Deceased': [0.0]  # Not deceased
            })
            skip_trace_data.to_excel(skip_trace_file, index=False)
            
            # Create mock config
            mock_config = Mock()
            mock_config.region_name = "Test Region"
            mock_config.region_code = "TST"
            mock_config.fips_code = "123456"
            
            mock_config_manager = Mock()
            mock_config_manager.get_region_config.return_value = mock_config
            
            # Run processing
            result = process_region_skip_trace(
                'test_region',
                str(enhanced_file),
                str(skip_trace_file),
                mock_config_manager
            )
            
            # Verify success
            assert result['success'] == True, f"Processing failed: {result.get('error', 'Unknown error')}"
            assert result['total_records'] == 2
            assert result['golden_address_count'] == 1
            assert result['golden_differs_count'] == 1
            assert result['st_flags_count'] == 1
            
            # Verify file was updated
            updated_data = pd.read_excel(enhanced_file)
            
            # Check new columns were added
            assert 'Golden_Address' in updated_data.columns
            assert 'Golden_Address_Differs' in updated_data.columns
            assert 'ST_Flags' in updated_data.columns
            
            # Check first record was updated
            assert updated_data.loc[0, 'Golden_Address'] == '123 MAIN ST UNIT A'
            assert updated_data.loc[0, 'Golden_Address_Differs'] == True
            assert updated_data.loc[0, 'ST_Flags'] == 'STBankruptcy'  # Should detect bankruptcy date
            assert updated_data.loc[0, 'PriorityCode'] == 'STBankruptcy-ABS1'
            
            # Check second record was not updated (no match)
            assert pd.isna(updated_data.loc[1, 'Golden_Address']) or updated_data.loc[1, 'Golden_Address'] == ''
            assert updated_data.loc[1, 'Golden_Address_Differs'] == False
            assert pd.isna(updated_data.loc[1, 'ST_Flags']) or updated_data.loc[1, 'ST_Flags'] == ''
            assert updated_data.loc[1, 'PriorityCode'] == 'BUY2'  # Unchanged
    
    def test_multiple_flag_combination(self):
        """Test handling of multiple skip trace flags on same record"""
        enhanced_data = pd.DataFrame({
            'APN': ['12345'],
            'Address': ['123 MAIN ST'],
            'Mailing Address': ['123 MAIN ST'],
            'PriorityCode': ['ABS1'],
            'PriorityName': ['ABS1 Priority']
        })
        
        skip_trace_data = pd.DataFrame({
            'Property APN': ['12345'],
            'Property FIPS': ['123456'],
            'Property Address': ['123 MAIN ST'],
            'Golden Address': ['123 MAIN ST'],
            'Owner Bankruptcy': [datetime(2023, 1, 15)],
            'Lien': [datetime(2023, 2, 15)],
            'Judgment': [datetime(2023, 3, 15)],
            'Owner Foreclosure': ['False'],
            'Quitclaim': ['False'],
            'Owner Is Deceased': ['False']
        })
        
        result_df = _match_skip_trace_hybrid(enhanced_data, skip_trace_data, '123456')
        
        # Should have multiple flags
        st_flags = result_df.loc[0, 'ST_Flags']
        flag_list = st_flags.split(',')
        
        assert 'STBankruptcy' in flag_list
        assert 'STLien' in flag_list
        assert 'STJudgment' in flag_list
        assert len(flag_list) == 3, f"Should have 3 flags, got {len(flag_list)}: {flag_list}"
        
        # Priority code should be enhanced with all flags
        priority_code = result_df.loc[0, 'PriorityCode']
        assert 'STBankruptcy' in priority_code
        assert 'STLien' in priority_code
        assert 'STJudgment' in priority_code
        assert priority_code.endswith('-ABS1'), f"Should end with original priority: {priority_code}"


if __name__ == "__main__":
    # Run tests directly
    import sys
    pytest.main([__file__, "-v"] + sys.argv[1:])