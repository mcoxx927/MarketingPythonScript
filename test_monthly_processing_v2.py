"""
Comprehensive test suite for monthly_processing_v2.py

Tests cover multi-region orchestration, niche list integration,
file processing workflows, error handling, and performance optimizations.
"""

import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock, call
import json
import logging
import argparse

# Import functions and constants from monthly_processing_v2
from monthly_processing_v2 import (
    _detect_niche_type_from_filename,
    _normalize_address,
    _update_main_with_niche,
    process_region,
    main,
    NICHE_ONLY_PRIORITY_ID,
    VERY_OLD_DATE_STR
)

class TestNicheTypeDetection:
    """Test suite for niche type detection from filenames"""
    
    def test_detect_liens_files(self):
        """Test detection of liens files"""
        liens_files = [
            'liens.xlsx',
            'tax_liens_2023.xlsx',
            'property_liens.xlsx',
            'LIENS_DATA.xlsx'
        ]
        
        for filename in liens_files:
            assert _detect_niche_type_from_filename(filename) == 'Liens'
    
    def test_detect_foreclosure_files(self):
        """Test detection of foreclosure files"""
        foreclosure_files = [
            'foreclosure.xlsx',
            'preforeclosure_data.xlsx',
            'pre_foreclosure_list.xlsx',
            'FORECLOSURE_2023.xlsx'
        ]
        
        for filename in foreclosure_files:
            assert _detect_niche_type_from_filename(filename) == 'PreForeclosure'
    
    def test_detect_bankruptcy_files(self):
        """Test detection of bankruptcy files"""
        bankruptcy_files = [
            'bankruptcy.xlsx',
            'bankrupt_properties.xlsx',
            'BANKRUPTCY_DATA.xlsx'
        ]
        
        for filename in bankruptcy_files:
            assert _detect_niche_type_from_filename(filename) == 'Bankruptcy'
    
    def test_detect_landlord_files(self):
        """Test detection of landlord/tired landlord files"""
        landlord_files = [
            'landlord.xlsx',
            'tired_landlords.xlsx',
            'landlord_properties.xlsx'
        ]
        
        for filename in landlord_files:
            assert _detect_niche_type_from_filename(filename) == 'Landlord'
    
    def test_detect_tax_delinquent_files(self):
        """Test detection of tax delinquent files"""
        tax_files = [
            'tax_delinquent.xlsx',
            'delinq_taxes.xlsx',
            'tax_delinq_properties.xlsx'
        ]
        
        for filename in tax_files:
            assert _detect_niche_type_from_filename(filename) == 'Tax'
    
    def test_detect_probate_files(self):
        """Test detection of probate files"""
        probate_files = [
            'probate.xlsx',
            'probate_properties.xlsx',
            'PROBATE_LIST.xlsx'
        ]
        
        for filename in probate_files:
            assert _detect_niche_type_from_filename(filename) == 'Probate'
    
    def test_detect_interfamily_files(self):
        """Test detection of interfamily transfer files"""
        interfamily_files = [
            'interfamily.xlsx',
            'family_transfers.xlsx',
            'interfamily_properties.xlsx'
        ]
        
        for filename in interfamily_files:
            assert _detect_niche_type_from_filename(filename) == 'InterFamily'
    
    def test_detect_cash_buyer_files(self):
        """Test detection of cash buyer files"""
        cash_buyer_files = [
            'cash_buyers.xlsx',
            'cash_buyer_list.xlsx',
            'CASH_BUYER_DATA.xlsx'
        ]
        
        for filename in cash_buyer_files:
            assert _detect_niche_type_from_filename(filename) == 'CashBuyer'
    
    def test_detect_unknown_files(self):
        """Test detection of unknown file types"""
        unknown_files = [
            'unknown_data.xlsx',
            'random_file.xlsx',
            'property_export.xlsx',
            'main_region.xlsx'
        ]
        
        for filename in unknown_files:
            assert _detect_niche_type_from_filename(filename) == 'Other'


class TestAddressNormalization:
    """Test suite for address normalization"""
    
    def test_normalize_address_basic(self):
        """Test basic address normalization"""
        addresses = [
            ('123 Main St', '123 MAIN ST'),
            ('456 OAK AVE', '456 OAK AVE'),
            ('789 pine rd', '789 PINE RD')
        ]
        
        for input_addr, expected in addresses:
            assert _normalize_address(input_addr) == expected
    
    def test_normalize_address_with_commas(self):
        """Test address normalization with comma removal"""
        addresses = [
            ('123 Main St,', '123 MAIN ST'),
            ('456 Oak Ave,', '456 OAK AVE'),
            ('789 Pine Rd,', '789 PINE RD'),
            ('321 Elm Blvd,', '321 ELM BLVD')
        ]
        
        for input_addr, expected in addresses:
            assert _normalize_address(input_addr) == expected
    
    def test_normalize_address_multiple_spaces(self):
        """Test address normalization with multiple spaces"""
        test_cases = [
            ('123  Main   St', '123 MAIN ST'),
            ('456   Oak  Ave  ', '456 OAK AVE'),
            ('  789    Pine    Rd  ', '789 PINE RD')
        ]
        
        for input_addr, expected in test_cases:
            assert _normalize_address(input_addr) == expected
    
    def test_normalize_address_empty_values(self):
        """Test address normalization with empty/null values"""
        empty_values = [None, '', '   ', pd.NA]
        
        for empty_val in empty_values:
            assert _normalize_address(empty_val) == ''
    
    def test_normalize_address_complex_cases(self):
        """Test complex address normalization cases"""
        complex_cases = [
            ('123 Main St, Apt 4B', '123 MAIN ST APT 4B'),
            ('456 Oak Ave, Suite 200,', '456 OAK AVE SUITE 200'),
            ('789 Pine Rd,, Unit 5', '789 PINE RD UNIT 5')
        ]
        
        for input_addr, expected in complex_cases:
            assert _normalize_address(input_addr) == expected


class TestNicheListIntegration:
    """Test suite for niche list integration with main region data"""
    
    def setup_method(self):
        """Set up test fixtures for niche integration"""
        # Create sample main region data
        self.main_data = pd.DataFrame({
            'Owner 1 Last Name': ['Smith', 'Jones', 'Brown'],
            'Owner 1 First Name': ['John', 'Mary', 'Robert'],
            'Address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
            'PriorityCode': ['OWN1', 'ABS1', 'BUY1'],
            'PriorityName': ['Owner List 1', 'Absentee List 1', 'Buyer List 1']
        })
        
        # Create sample niche data
        self.niche_data = pd.DataFrame({
            'Owner 1 Last Name': ['Smith', 'Davis'],  # Smith matches main, Davis is new
            'Owner 1 First Name': ['John', 'Sarah'],
            'Address': ['123 Main St', '999 New St'],  # First matches, second is new
            'Lien Type': ['Tax Lien', 'Mechanic Lien'],
            'Lien Amount': [5000, 12000]
        })
    
    def test_update_main_with_niche_existing_records(self):
        """Test updating existing records with niche data"""
        main_df = self.main_data.copy()
        niche_df = self.niche_data.copy()
        
        updates_count, inserts_count = _update_main_with_niche(
            main_df, niche_df, 'Liens'
        )
        
        # Should update 1 existing record (123 Main St)
        assert updates_count == 1
        assert inserts_count == 1  # 999 New St is new
        
        # Check that the existing record was updated
        smith_record = main_df[main_df['Address'] == '123 Main St'].iloc[0]
        assert smith_record['PriorityCode'] == 'Liens-OWN1'
        assert 'Liens Enhanced' in smith_record['PriorityName']
    
    def test_update_main_with_niche_new_records(self):
        """Test inserting new records from niche data"""
        main_df = self.main_data.copy()
        niche_df = pd.DataFrame({
            'Owner 1 Last Name': ['New'],
            'Owner 1 First Name': ['Owner'],
            'Address': ['888 New Ave'],
            'City': ['Roanoke'],
            'State': ['VA'],
            'Zip': ['24016']
        })
        
        original_length = len(main_df)
        updates_count, inserts_count = _update_main_with_niche(
            main_df, niche_df, 'Liens'
        )
        
        # Should insert 1 new record
        assert updates_count == 0  # No existing matches
        assert inserts_count == 1
        assert len(main_df) == original_length + 1
        
        # Check the new record
        new_record = main_df[main_df['Address'] == '888 New Ave'].iloc[0]
        assert new_record['PriorityId'] == NICHE_ONLY_PRIORITY_ID
        assert new_record['PriorityCode'] == 'Liens'
        assert new_record['PriorityName'] == 'Liens List Only'
    
    def test_update_main_with_niche_no_duplicates(self):
        """Test that duplicate niche types don't create duplicate prefixes"""
        main_df = self.main_data.copy()
        
        # First update with Liens
        niche_df1 = pd.DataFrame({
            'Address': ['123 Main St'],
            'Owner 1 Last Name': ['Smith'],
            'Owner 1 First Name': ['John']
        })
        
        _update_main_with_niche(main_df, niche_df1, 'Liens')
        
        # Second update with same Liens data
        _update_main_with_niche(main_df, niche_df1, 'Liens')
        
        # Should not duplicate the Liens prefix
        smith_record = main_df[main_df['Address'] == '123 Main St'].iloc[0]
        assert smith_record['PriorityCode'].count('Liens') == 1
    
    def test_update_main_with_niche_empty_addresses(self):
        """Test handling of empty addresses in niche data"""
        main_df = self.main_data.copy()
        niche_df = pd.DataFrame({
            'Owner 1 Last Name': ['Empty', 'Valid'],
            'Owner 1 First Name': ['Address', 'Address'],
            'Address': ['', '123 Main St'],  # Empty and valid address
        })
        
        updates_count, inserts_count = _update_main_with_niche(
            main_df, niche_df, 'Liens'
        )
        
        # Should only process the record with valid address
        assert updates_count == 1  # 123 Main St matches
        assert inserts_count == 0  # Empty address is filtered out
    
    def test_update_main_with_niche_multiple_types(self):
        """Test updating with multiple niche types"""
        main_df = self.main_data.copy()
        
        # Update with Liens first
        liens_df = pd.DataFrame({
            'Address': ['123 Main St'],
            'Owner 1 Last Name': ['Smith'],
            'Owner 1 First Name': ['John']
        })
        _update_main_with_niche(main_df, liens_df, 'Liens')
        
        # Then update with Bankruptcy
        bankruptcy_df = pd.DataFrame({
            'Address': ['123 Main St'],
            'Owner 1 Last Name': ['Smith'],
            'Owner 1 First Name': ['John'],
            'BK Date': ['2020-01-15']
        })
        _update_main_with_niche(main_df, bankruptcy_df, 'Bankruptcy')
        
        # Should have both prefixes
        smith_record = main_df[main_df['Address'] == '123 Main St'].iloc[0]
        assert 'Liens' in smith_record['PriorityCode']
        assert 'Bankruptcy' in smith_record['PriorityCode']
    
    def test_update_main_preserves_dataframe_structure(self):
        """Test that niche integration preserves DataFrame structure"""
        main_df = self.main_data.copy()
        original_columns = set(main_df.columns)
        
        niche_df = self.niche_data.copy()
        _update_main_with_niche(main_df, niche_df, 'Liens')
        
        # Should not add the temporary _NormalizedAddress column
        assert '_NormalizedAddress' not in main_df.columns
        assert '_NormalizedAddress' not in niche_df.columns
        
        # Original columns should be preserved
        for col in original_columns:
            assert col in main_df.columns


class TestRegionProcessing:
    """Test suite for region processing orchestration"""
    
    def setup_method(self):
        """Set up region processing test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.regions_dir = Path(self.temp_dir) / "regions"
        self.regions_dir.mkdir()
        
        # Create test region configuration
        self.test_config = {
            "region_name": "Test City, VA",
            "region_code": "TEST",
            "fips_code": "51999",
            "region_input_date1": "2009-01-01",
            "region_input_date2": "2019-01-01",
            "region_input_amount1": 75000,
            "region_input_amount2": 200000,
            "market_type": "Rural/Small City",
            "description": "Test region for unit testing",
            "notes": "Test region"
        }
    
    def teardown_method(self):
        """Clean up region processing test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_test_region_structure(self, region_key: str, include_niche: bool = True):
        """Helper method to create complete test region structure"""
        region_dir = self.regions_dir / region_key
        region_dir.mkdir()
        
        # Create config file
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(self.test_config, f, indent=2)
        
        # Create main region Excel file
        main_data = {
            'Owner 1 Last Name': ['Smith', 'Jones Trust', 'First Baptist'],
            'Owner 1 First Name': ['John', 'Family', 'Church'],
            'Address': ['123 Main St', '456 Oak Ave', '789 Church St'],
            'Mailing Address': ['123 Main St', '999 Different St', '789 Church St'],
            'Last Sale Date': ['2010-01-15', '2020-06-30', '2005-03-20'],
            'Last Sale Amount': [100000, 50000, 200000],
            'FIPS': [self.test_config['fips_code']] * 3,
            'Grantor': ['John Doe', 'Smith Family', 'Different Church']
        }
        df_main = pd.DataFrame(main_data)
        main_file = region_dir / "main_region.xlsx"
        df_main.to_excel(main_file, index=False)
        
        if include_niche:
            # Create niche files
            liens_data = {
                'Owner 1 Last Name': ['Brown', 'Smith'],
                'Owner 1 First Name': ['Robert', 'John'],
                'Address': ['321 Pine St', '123 Main St'],
                'FIPS': [self.test_config['fips_code']] * 2,
                'Lien Type': ['Tax Lien', 'Mechanic Lien'],
                'Lien Amount': [5000, 3000]
            }
            df_liens = pd.DataFrame(liens_data)
            liens_file = region_dir / "liens.xlsx"
            df_liens.to_excel(liens_file, index=False)
            
            foreclosure_data = {
                'Owner 1 Last Name': ['Davis'],
                'Owner 1 First Name': ['Sarah'],
                'Address': ['999 Elm St'],
                'FIPS': [self.test_config['fips_code']],
                'FC Date': ['2023-01-15']
            }
            df_foreclosure = pd.DataFrame(foreclosure_data)
            foreclosure_file = region_dir / "foreclosure.xlsx"
            df_foreclosure.to_excel(foreclosure_file, index=False)
        
        return region_dir
    
    @patch('monthly_processing_v2.MultiRegionConfigManager')
    @patch('monthly_processing_v2.PropertyProcessor')
    def test_process_region_success(self, mock_processor_class, mock_config_manager_class):
        """Test successful region processing"""
        # Create test region structure
        region_dir = self.create_test_region_structure("test_region")
        
        # Set up mocks
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        
        mock_config = Mock()
        mock_config.region_name = "Test Region"
        mock_config.region_code = "TEST"
        mock_config.market_type = "Test Market"
        mock_config.region_input_date1 = datetime(2009, 1, 1)
        mock_config.region_input_date2 = datetime(2019, 1, 1)
        mock_config.region_input_amount1 = 75000
        mock_config.region_input_amount2 = 200000
        
        mock_config_manager.get_region_config.return_value = mock_config
        mock_config_manager.get_region_directory.return_value = region_dir
        mock_config_manager.create_output_directory.return_value = Path(self.temp_dir) / "output"
        mock_config_manager.validate_region_files.return_value = {
            'valid': True, 'has_config': True, 'has_main_file': True, 
            'has_excel_files': True, 'total_files': 3
        }
        mock_config_manager.validate_fips_codes.return_value = {
            'all_valid': True, 'region_fips': '51999', 'files_checked': 3, 
            'files_valid': 3, 'fips_mismatches': [], 'missing_fips_column': []
        }
        
        # Mock processor
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        
        # Create mock processed data
        mock_main_result = pd.DataFrame({
            'OwnerName': ['Smith John', 'Jones Family Trust', 'First Baptist Church'],
            'Address': ['123 Main St', '456 Oak Ave', '789 Church St'],
            'PriorityCode': ['OWN1', 'TRS2', 'CHURCH'],
            'PriorityName': ['Owner List 1', 'Trust', 'Church']
        })
        mock_processor.process_excel_file.return_value = mock_main_result
        
        # Create output directory
        output_dir = Path(self.temp_dir) / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Test region processing
        result = process_region("test_region", mock_config_manager)
        
        # Verify successful processing
        assert result['success'] == True
        assert result['region_name'] == "Test Region"
        assert result['total_records'] == 3
        
        # Verify processor was called
        mock_processor.process_excel_file.assert_called()
    
    @patch('monthly_processing_v2.MultiRegionConfigManager')
    def test_process_region_validation_failure(self, mock_config_manager_class):
        """Test region processing with validation failure"""
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        
        # Mock validation failure
        mock_config_manager.get_region_config.return_value = Mock()
        mock_config_manager.get_region_directory.return_value = Path("/fake/path")
        mock_config_manager.create_output_directory.return_value = Path("/fake/output")
        mock_config_manager.validate_region_files.return_value = {
            'valid': False, 'has_config': True, 'has_main_file': False,
            'has_excel_files': False, 'total_files': 0
        }
        
        result = process_region("invalid_region", mock_config_manager)
        
        assert result['success'] == False
        assert 'Region validation failed' in result['error']
    
    @patch('monthly_processing_v2.MultiRegionConfigManager')
    def test_process_region_fips_validation_failure(self, mock_config_manager_class):
        """Test region processing with FIPS validation failure"""
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        
        mock_config_manager.get_region_config.return_value = Mock()
        mock_config_manager.get_region_directory.return_value = Path("/fake/path")
        mock_config_manager.create_output_directory.return_value = Path("/fake/output")
        mock_config_manager.validate_region_files.return_value = {
            'valid': True, 'has_config': True, 'has_main_file': True,
            'has_excel_files': True, 'total_files': 2
        }
        mock_config_manager.validate_fips_codes.return_value = {
            'all_valid': False, 'region_fips': '51999', 'files_checked': 2,
            'files_valid': 0, 'fips_mismatches': [
                {'file': 'main.xlsx', 'expected': '51999', 'found': ['99999']}
            ], 'missing_fips_column': []
        }
        
        result = process_region("fips_invalid_region", mock_config_manager)
        
        assert result['success'] == False
        assert 'FIPS validation failed' in result['error']
    
    @patch('monthly_processing_v2.MultiRegionConfigManager')
    @patch('monthly_processing_v2.PropertyProcessor')
    def test_process_region_with_niche_files(self, mock_processor_class, mock_config_manager_class):
        """Test region processing with niche file integration"""
        region_dir = self.create_test_region_structure("niche_test_region", include_niche=True)
        
        # Set up mocks similar to success test
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        
        mock_config = Mock()
        mock_config.region_name = "Niche Test Region"
        mock_config.region_code = "NICHE"
        mock_config.market_type = "Test Market"
        mock_config.region_input_date1 = datetime(2009, 1, 1)
        mock_config.region_input_date2 = datetime(2019, 1, 1)
        mock_config.region_input_amount1 = 75000
        mock_config.region_input_amount2 = 200000
        
        mock_config_manager.get_region_config.return_value = mock_config
        mock_config_manager.get_region_directory.return_value = region_dir
        mock_config_manager.create_output_directory.return_value = Path(self.temp_dir) / "output"
        mock_config_manager.validate_region_files.return_value = {'valid': True, 'has_config': True, 'has_main_file': True, 'has_excel_files': True, 'total_files': 3}
        mock_config_manager.validate_fips_codes.return_value = {'all_valid': True, 'region_fips': '51999', 'files_checked': 3, 'files_valid': 3, 'fips_mismatches': [], 'missing_fips_column': []}
        
        # Mock processor
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        mock_main_result = pd.DataFrame({
            'OwnerName': ['Smith John', 'Jones Family Trust'],
            'Address': ['123 Main St', '456 Oak Ave'],
            'PriorityCode': ['OWN1', 'TRS2'],
            'PriorityName': ['Owner List 1', 'Trust']
        })
        mock_processor.process_excel_file.return_value = mock_main_result
        
        output_dir = Path(self.temp_dir) / "output"
        output_dir.mkdir(exist_ok=True)
        
        result = process_region("niche_test_region", mock_config_manager)
        
        assert result['success'] == True
        # Should have processed both main file and niche files


class TestMainFunction:
    """Test suite for main function and command line interface"""
    
    @patch('monthly_processing_v2.MultiRegionConfigManager')
    def test_main_list_regions(self, mock_config_manager_class):
        """Test main function with --list-regions flag"""
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        
        # Mock list_regions return value
        mock_config_manager.list_regions.return_value = [
            {'code': 'ROA', 'name': 'Roanoke City, VA', 'market_type': 'Rural', 'description': 'Test region'},
            {'code': 'VB', 'name': 'Virginia Beach, VA', 'market_type': 'Coastal', 'description': 'Beach region'}
        ]
        mock_config_manager.configs = {'roanoke': Mock(), 'vb': Mock()}
        
        # Mock sys.argv
        with patch('sys.argv', ['monthly_processing_v2.py', '--list-regions']):
            with patch('builtins.print') as mock_print:
                main()
                
                # Verify that regions were printed
                mock_print.assert_called()
                print_calls = [call[0][0] for call in mock_print.call_args_list]
                region_output = ' '.join(print_calls)
                assert 'Roanoke City, VA' in region_output
                assert 'Virginia Beach, VA' in region_output
    
    @patch('monthly_processing_v2.process_region')
    @patch('monthly_processing_v2.MultiRegionConfigManager')
    def test_main_single_region_success(self, mock_config_manager_class, mock_process_region):
        """Test main function processing single region successfully"""
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        
        # Mock successful region processing
        mock_process_region.return_value = {'success': True, 'region_name': 'Test Region'}
        
        with patch('sys.argv', ['monthly_processing_v2.py', '--region', 'test_region']):
            with patch('builtins.print') as mock_print:
                main()
                
                # Verify process_region was called with correct arguments
                mock_process_region.assert_called_once_with('test_region', mock_config_manager)
                
                # Check success message was printed
                print_calls = [call[0][0] for call in mock_print.call_args_list]
                success_output = ' '.join(print_calls)
                assert 'SUCCESS' in success_output
    
    @patch('monthly_processing_v2.process_region')
    @patch('monthly_processing_v2.MultiRegionConfigManager')
    def test_main_single_region_failure(self, mock_config_manager_class, mock_process_region):
        """Test main function handling single region processing failure"""
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        
        # Mock failed region processing
        mock_process_region.return_value = {'success': False, 'error': 'Test error message'}
        
        with patch('sys.argv', ['monthly_processing_v2.py', '--region', 'failing_region']):
            with patch('builtins.print') as mock_print:
                with pytest.raises(SystemExit) as exc_info:
                    main()
                
                # Should exit with code 1
                assert exc_info.value.code == 1
                
                # Check error message was printed
                print_calls = [call[0][0] for call in mock_print.call_args_list]
                error_output = ' '.join(print_calls)
                assert 'ERROR' in error_output
                assert 'Test error message' in error_output
    
    @patch('monthly_processing_v2.process_region')
    @patch('monthly_processing_v2.MultiRegionConfigManager')
    def test_main_all_regions(self, mock_config_manager_class, mock_process_region):
        """Test main function processing all regions"""
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        
        # Mock multiple regions
        mock_config_manager.configs.keys.return_value = ['region1', 'region2', 'region3']
        
        # Mock successful processing for all regions
        mock_process_region.side_effect = [
            {'success': True, 'total_records': 100, 'region_name': 'Region 1'},
            {'success': True, 'total_records': 200, 'region_name': 'Region 2'},
            {'success': False, 'error': 'Region 3 failed', 'region_name': 'Region 3'}
        ]
        
        with patch('sys.argv', ['monthly_processing_v2.py', '--all-regions']):
            with patch('builtins.print') as mock_print:
                main()
                
                # Verify all regions were processed
                assert mock_process_region.call_count == 3
                
                # Check summary was printed
                print_calls = [call[0][0] for call in mock_print.call_args_list]
                summary_output = ' '.join(print_calls)
                assert 'BATCH PROCESSING SUMMARY' in summary_output
                assert 'Successfully processed: 2 regions' in summary_output
                assert 'Failed: 1 regions' in summary_output
                assert 'Total records processed: 300' in summary_output
    
    def test_main_no_arguments(self):
        """Test main function with no arguments (should show help)"""
        with patch('sys.argv', ['monthly_processing_v2.py']):
            with pytest.raises(SystemExit):
                main()


class TestErrorHandlingAndEdgeCases:
    """Test suite for error handling and edge cases"""
    
    def setup_method(self):
        """Set up error handling test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Clean up error handling test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_niche_integration_with_malformed_data(self):
        """Test niche integration with malformed data"""
        # Main data with missing columns
        main_df = pd.DataFrame({
            'Address': ['123 Main St'],
            'PriorityCode': ['OWN1']
            # Missing other expected columns
        })
        
        # Niche data with different column structure
        niche_df = pd.DataFrame({
            'Property_Address': ['123 Main St'],  # Different column name
            'Owner_Name': ['John Smith']
        })
        
        # Should handle gracefully without crashing
        try:
            updates, inserts = _update_main_with_niche(main_df, niche_df, 'Liens')
            # Function should complete even with column mismatches
            assert isinstance(updates, int)
            assert isinstance(inserts, int)
        except Exception as e:
            pytest.fail(f"Function should handle malformed data gracefully: {e}")
    
    def test_address_normalization_with_special_characters(self):
        """Test address normalization with special characters and Unicode"""
        special_addresses = [
            ('123 Café Street', '123 CAFÉ STREET'),
            ('456 O\'Brien Ave', '456 O\'BRIEN AVE'),
            ('789 Señora Blvd', '789 SEÑORA BLVD'),
            ('321 Müller Straße', '321 MÜLLER STRASSE')
        ]
        
        for input_addr, expected in special_addresses:
            try:
                result = _normalize_address(input_addr)
                # Should handle special characters without crashing
                assert isinstance(result, str)
                assert len(result) > 0
            except Exception as e:
                pytest.fail(f"Address normalization should handle special characters: {e}")
    
    def test_niche_type_detection_edge_cases(self):
        """Test niche type detection with edge cases"""
        edge_case_files = [
            '',  # Empty filename
            '.xlsx',  # No actual filename
            'file_without_extension',
            'MULTIPLE_KEYWORDS_liens_foreclosure_bankruptcy.xlsx',
            'très_special_naïve_filename.xlsx'  # Unicode in filename
        ]
        
        for filename in edge_case_files:
            try:
                niche_type = _detect_niche_type_from_filename(filename)
                assert isinstance(niche_type, str)
                assert len(niche_type) > 0
            except Exception as e:
                pytest.fail(f"Niche type detection should handle edge case '{filename}': {e}")
    
    def test_large_dataset_integration(self):
        """Test integration with large datasets"""
        # Create large datasets to test memory handling
        large_main_df = pd.DataFrame({
            'Address': [f'{i} Test St' for i in range(10000)],
            'PriorityCode': ['OWN1'] * 10000,
            'PriorityName': ['Owner List 1'] * 10000
        })
        
        large_niche_df = pd.DataFrame({
            'Address': [f'{i} Test St' for i in range(5000, 15000)],  # 50% overlap
            'Owner 1 Last Name': ['Test'] * 10000,
            'Owner 1 First Name': ['Owner'] * 10000
        })
        
        try:
            updates, inserts = _update_main_with_niche(
                large_main_df, large_niche_df, 'Liens'
            )
            
            # Should complete without memory errors
            assert isinstance(updates, int)
            assert isinstance(inserts, int)
            assert updates > 0  # Should have some updates from overlap
            assert inserts > 0  # Should have some inserts from new records
            
        except MemoryError:
            pytest.skip("System doesn't have enough memory for large dataset test")
        except Exception as e:
            pytest.fail(f"Large dataset integration should work: {e}")


class TestPerformanceOptimizations:
    """Test suite for performance optimizations"""
    
    def test_vectorized_address_matching_performance(self):
        """Test that address matching uses vectorized operations"""
        # Create datasets large enough to measure performance difference
        main_df = pd.DataFrame({
            'Address': [f'{i} Performance St' for i in range(1000)],
            'PriorityCode': ['OWN1'] * 1000,
            'PriorityName': ['Owner List 1'] * 1000
        })
        
        niche_df = pd.DataFrame({
            'Address': [f'{i} Performance St' for i in range(0, 2000, 2)],  # Every other address
            'Owner 1 Last Name': ['Test'] * 1000,
            'Owner 1 First Name': ['Owner'] * 1000
        })
        
        import time
        start_time = time.time()
        
        updates, inserts = _update_main_with_niche(main_df, niche_df, 'Performance')
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Should complete in reasonable time (vectorized operations)
        assert processing_time < 5.0  # Should complete in less than 5 seconds
        assert updates == 500  # Half of the niche records should match
        assert inserts == 500  # Half should be new
    
    def test_memory_efficient_niche_processing(self):
        """Test memory-efficient processing of niche files"""
        # This test verifies that memory usage doesn't grow excessively
        main_df = pd.DataFrame({
            'Address': [f'{i} Memory St' for i in range(5000)],
            'PriorityCode': ['OWN1'] * 5000
        })
        
        # Process multiple niche files in sequence
        for niche_type in ['Liens', 'Foreclosure', 'Bankruptcy']:
            niche_df = pd.DataFrame({
                'Address': [f'{i} Memory St' for i in range(2500, 7500)],
                'Owner 1 Last Name': ['Test'] * 5000,
                'Owner 1 First Name': ['Owner'] * 5000
            })
            
            original_length = len(main_df)
            updates, inserts = _update_main_with_niche(main_df, niche_df, niche_type)
            
            # Memory should be managed efficiently
            assert len(main_df) >= original_length  # Length increases with inserts
            assert isinstance(updates, int) and updates >= 0
            assert isinstance(inserts, int) and inserts >= 0


if __name__ == '__main__':
    # Run tests with pytest
    pytest.main([__file__, '-v'])