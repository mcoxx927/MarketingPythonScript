"""
Shared test fixtures and utilities for the real estate processing test suite.

This module provides common test data, mock objects, and utility functions
used across multiple test files.
"""

import pandas as pd
import tempfile
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import shutil

class TestDataFactory:
    """Factory class for generating test data"""
    
    @staticmethod
    def create_sample_property_data(num_records: int = 100, include_special_cases: bool = True) -> pd.DataFrame:
        """
        Create sample property data for testing.
        
        Args:
            num_records: Number of records to generate
            include_special_cases: Whether to include special test cases
            
        Returns:
            DataFrame with sample property data
        """
        base_data = {
            'Owner 1 Last Name': ['Smith', 'Jones', 'Brown', 'Davis', 'Wilson'] * (num_records // 5 + 1),
            'Owner 1 First Name': ['John', 'Mary', 'Robert', 'Sarah', 'Michael'] * (num_records // 5 + 1),
            'Address': [f'{100 + i} Test St' for i in range(num_records)],
            'Mailing Address': [f'{100 + i} Test St' if i % 3 == 0 else f'{200 + i} Different Ave' for i in range(num_records)],
            'Last Sale Date': [
                (datetime.now() - timedelta(days=365 * (i % 20))).strftime('%Y-%m-%d') 
                for i in range(num_records)
            ],
            'Last Sale Amount': [50000 + (i * 1000) % 200000 for i in range(num_records)],
            'FIPS': ['51770'] * num_records,
            'City': ['Test City'] * num_records,
            'State': ['VA'] * num_records,
            'Zip': ['24016'] * num_records,
            'Last Cash Buyer': ['true' if i % 4 == 0 else 'false' for i in range(num_records)]
        }
        
        df = pd.DataFrame({key: values[:num_records] for key, values in base_data.items()})
        
        if include_special_cases:
            # Add special test cases
            special_cases = pd.DataFrame({
                'Owner 1 Last Name': ['Smith Family Trust', 'First Baptist', 'ABC Properties LLC', '', 'O\'Brien'],
                'Owner 1 First Name': ['Revocable', 'Church', 'Inc', '', 'Seán'],
                'Address': ['500 Trust Ave', '600 Church St', '700 Business Blvd', '800 Empty Rd', '900 Unicode St'],
                'Mailing Address': ['500 Trust Ave', '600 Church St', '999 Different St', '', '900 Unicode St'],
                'Last Sale Date': ['2010-01-15', '', '1900-01-01', '2099-12-31', 'invalid-date'],
                'Last Sale Amount': [75000, None, -5000, 50000000, 'not-a-number'],
                'FIPS': ['51770', '51770', '51770', '51770', '51770'],
                'City': ['Test City'] * 5,
                'State': ['VA'] * 5,
                'Zip': ['24016'] * 5,
                'Last Cash Buyer': ['false', 'true', 'false', '', '1']
            })
            
            df = pd.concat([df, special_cases], ignore_index=True)
        
        return df
    
    @staticmethod
    def create_niche_data(niche_type: str = 'liens', num_records: int = 50, 
                         overlap_with_main: float = 0.3) -> pd.DataFrame:
        """
        Create sample niche list data for testing.
        
        Args:
            niche_type: Type of niche list (liens, foreclosure, etc.)
            num_records: Number of records to generate
            overlap_with_main: Percentage of records that should overlap with main data
            
        Returns:
            DataFrame with sample niche data
        """
        # Generate base addresses - some overlap with main data
        overlap_count = int(num_records * overlap_with_main)
        overlap_addresses = [f'{100 + i} Test St' for i in range(overlap_count)]
        new_addresses = [f'{1000 + i} Niche Ave' for i in range(num_records - overlap_count)]
        addresses = overlap_addresses + new_addresses
        
        base_data = {
            'Owner 1 Last Name': ['Niche', 'List', 'Owner', 'Test', 'Data'] * (num_records // 5 + 1),
            'Owner 1 First Name': ['Person', 'Entity', 'Individual', 'Name', 'Owner'] * (num_records // 5 + 1),
            'Address': addresses,
            'City': ['Test City'] * num_records,
            'State': ['VA'] * num_records,
            'Zip': ['24016'] * num_records,
            'FIPS': ['51770'] * num_records
        }
        
        # Add niche-specific columns
        if niche_type.lower() == 'liens':
            base_data.update({
                'Lien Type': ['Tax Lien', 'Mechanic Lien', 'Judgment Lien'] * (num_records // 3 + 1),
                'Lien Amount': [5000, 12000, 8000] * (num_records // 3 + 1),
                'Lien Date': [(datetime.now() - timedelta(days=30 * i)).strftime('%Y-%m-%d') for i in range(num_records)]
            })
        elif niche_type.lower() == 'foreclosure':
            base_data.update({
                'FC Date': [(datetime.now() - timedelta(days=60 * i)).strftime('%Y-%m-%d') for i in range(num_records)],
                'FC Status': ['Pre-Foreclosure', 'Notice Filed', 'Auction Scheduled'] * (num_records // 3 + 1)
            })
        elif niche_type.lower() == 'bankruptcy':
            base_data.update({
                'BK Date': [(datetime.now() - timedelta(days=90 * i)).strftime('%Y-%m-%d') for i in range(num_records)],
                'BK Chapter': ['Chapter 7', 'Chapter 13', 'Chapter 11'] * (num_records // 3 + 1)
            })
        
        return pd.DataFrame({key: values[:num_records] for key, values in base_data.items()})
    
    @staticmethod
    def create_region_config(region_key: str = 'test_region', 
                           region_name: str = 'Test Region, VA',
                           fips_code: str = '51999') -> Dict:
        """
        Create a test region configuration.
        
        Args:
            region_key: Region identifier
            region_name: Human-readable region name
            fips_code: FIPS code for the region
            
        Returns:
            Dictionary with region configuration
        """
        return {
            "region_name": region_name,
            "region_code": region_key.upper()[:4],
            "fips_code": fips_code,
            "region_input_date1": "2009-01-01",  # ABS1 cutoff
            "region_input_date2": "2019-01-01",  # BUY cutoff
            "region_input_amount1": 75000,       # Low threshold
            "region_input_amount2": 200000,      # High threshold
            "market_type": "Rural/Small City",
            "description": f"Test configuration for {region_name}",
            "notes": "Generated for unit testing purposes"
        }


class TestRegionBuilder:
    """Builder class for creating complete test region structures"""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize builder with optional base directory"""
        if base_dir is None:
            self.base_dir = Path(tempfile.mkdtemp())
        else:
            self.base_dir = base_dir
        
        self.regions_dir = self.base_dir / "regions"
        self.regions_dir.mkdir(exist_ok=True)
        
        self.created_regions = []
    
    def create_region(self, region_key: str, config_overrides: Optional[Dict] = None,
                     include_main_file: bool = True, include_niche_files: List[str] = None,
                     main_records: int = 100, niche_records: int = 50) -> Path:
        """
        Create a complete test region with configuration and data files.
        
        Args:
            region_key: Unique identifier for the region
            config_overrides: Optional config values to override defaults
            include_main_file: Whether to create main region Excel file
            include_niche_files: List of niche file types to create
            main_records: Number of records in main file
            niche_records: Number of records in each niche file
            
        Returns:
            Path to the created region directory
        """
        region_dir = self.regions_dir / region_key
        region_dir.mkdir(exist_ok=True)
        
        # Create configuration file
        config = TestDataFactory.create_region_config(region_key)
        if config_overrides:
            config.update(config_overrides)
        
        config_file = region_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        # Create main region file
        if include_main_file:
            main_data = TestDataFactory.create_sample_property_data(
                num_records=main_records, include_special_cases=True
            )
            # Ensure FIPS matches config
            main_data['FIPS'] = config['fips_code']
            
            main_file = region_dir / "main_region.xlsx"
            main_data.to_excel(main_file, index=False)
        
        # Create niche files
        if include_niche_files:
            for niche_type in include_niche_files:
                niche_data = TestDataFactory.create_niche_data(
                    niche_type=niche_type, 
                    num_records=niche_records,
                    overlap_with_main=0.3
                )
                # Ensure FIPS matches config
                niche_data['FIPS'] = config['fips_code']
                
                niche_file = region_dir / f"{niche_type}.xlsx"
                niche_data.to_excel(niche_file, index=False)
        
        self.created_regions.append(region_key)
        return region_dir
    
    def create_invalid_region(self, region_key: str, issue_type: str) -> Path:
        """
        Create a region with specific validation issues for testing error handling.
        
        Args:
            region_key: Region identifier
            issue_type: Type of issue to create ('missing_config', 'invalid_json', 
                       'missing_fields', 'wrong_fips', 'no_files')
            
        Returns:
            Path to the created region directory
        """
        region_dir = self.regions_dir / region_key
        region_dir.mkdir(exist_ok=True)
        
        if issue_type == 'missing_config':
            # Don't create config.json
            pass
            
        elif issue_type == 'invalid_json':
            config_file = region_dir / "config.json"
            with open(config_file, 'w') as f:
                f.write("{ invalid json content")
                
        elif issue_type == 'missing_fields':
            incomplete_config = {"region_name": "Incomplete Region"}
            config_file = region_dir / "config.json"
            with open(config_file, 'w') as f:
                json.dump(incomplete_config, f)
                
        elif issue_type == 'wrong_fips':
            config = TestDataFactory.create_region_config(region_key, fips_code='51999')
            config_file = region_dir / "config.json"
            with open(config_file, 'w') as f:
                json.dump(config, f)
            
            # Create Excel file with wrong FIPS
            wrong_fips_data = TestDataFactory.create_sample_property_data(10)
            wrong_fips_data['FIPS'] = '99999'  # Wrong FIPS
            
            main_file = region_dir / "main_region.xlsx"
            wrong_fips_data.to_excel(main_file, index=False)
            
        elif issue_type == 'no_files':
            # Create valid config but no Excel files
            config = TestDataFactory.create_region_config(region_key)
            config_file = region_dir / "config.json"
            with open(config_file, 'w') as f:
                json.dump(config, f)
        
        return region_dir
    
    def cleanup(self):
        """Clean up all created test regions"""
        if self.base_dir.exists():
            shutil.rmtree(self.base_dir, ignore_errors=True)


class MockConfigManager:
    """Mock configuration manager for testing"""
    
    def __init__(self, regions: List[str] = None):
        self.regions = regions or ['test_region_1', 'test_region_2']
        self.configs = {}
        
        for region in self.regions:
            self.configs[region] = self._create_mock_config(region)
    
    def _create_mock_config(self, region_key: str):
        """Create a mock region config object"""
        from types import SimpleNamespace
        
        config_data = TestDataFactory.create_region_config(region_key)
        config = SimpleNamespace()
        config.region_name = config_data['region_name']
        config.region_code = config_data['region_code']
        config.fips_code = config_data['fips_code']
        config.region_input_date1 = datetime.strptime(config_data['region_input_date1'], '%Y-%m-%d')
        config.region_input_date2 = datetime.strptime(config_data['region_input_date2'], '%Y-%m-%d')
        config.region_input_amount1 = config_data['region_input_amount1']
        config.region_input_amount2 = config_data['region_input_amount2']
        config.market_type = config_data['market_type']
        config.description = config_data['description']
        config.notes = config_data['notes']
        
        return config
    
    def get_region_config(self, region_key: str):
        """Get mock region config"""
        if region_key not in self.configs:
            raise ValueError(f"Region '{region_key}' not found")
        return self.configs[region_key]
    
    def list_regions(self):
        """List mock regions"""
        return [
            {
                'key': key,
                'name': config.region_name,
                'code': config.region_code,
                'market_type': config.market_type,
                'description': config.description
            }
            for key, config in self.configs.items()
        ]
    
    def validate_region_files(self, region_key: str):
        """Mock file validation - always returns valid"""
        return {
            'has_config': True,
            'has_main_file': True,
            'has_excel_files': True,
            'total_files': 3,
            'valid': True
        }
    
    def validate_fips_codes(self, region_key: str):
        """Mock FIPS validation - always returns valid"""
        config = self.get_region_config(region_key)
        return {
            'region_fips': config.fips_code,
            'files_checked': 3,
            'files_valid': 3,
            'all_valid': True,
            'fips_mismatches': [],
            'missing_fips_column': []
        }


class TestAssertions:
    """Custom assertion helpers for testing"""
    
    @staticmethod
    def assert_dataframe_has_columns(df: pd.DataFrame, required_columns: List[str]):
        """Assert that DataFrame has all required columns"""
        missing_columns = [col for col in required_columns if col not in df.columns]
        assert not missing_columns, f"Missing required columns: {missing_columns}"
    
    @staticmethod
    def assert_priority_distribution(df: pd.DataFrame, expected_priorities: Dict[str, int]):
        """Assert that priority distribution matches expectations"""
        actual_priorities = df['PriorityCode'].value_counts().to_dict()
        
        for priority, expected_count in expected_priorities.items():
            actual_count = actual_priorities.get(priority, 0)
            assert actual_count == expected_count, f"Expected {expected_count} {priority} records, got {actual_count}"
    
    @staticmethod
    def assert_address_normalization_quality(original_addresses: List[str], 
                                           normalized_addresses: List[str]):
        """Assert that address normalization maintains data quality"""
        assert len(original_addresses) == len(normalized_addresses)
        
        for orig, norm in zip(original_addresses, normalized_addresses):
            if orig and pd.notna(orig):
                # Normalized should not be empty for non-empty input
                assert norm, f"Normalized address is empty for input: '{orig}'"
                # Should be uppercase
                assert norm.isupper(), f"Normalized address not uppercase: '{norm}'"
                # Should not have multiple spaces
                assert '  ' not in norm, f"Normalized address has multiple spaces: '{norm}'"
    
    @staticmethod
    def assert_processing_performance(processing_time: float, max_time: float, 
                                    records_processed: int):
        """Assert that processing meets performance requirements"""
        assert processing_time <= max_time, f"Processing took {processing_time:.2f}s, expected <= {max_time}s"
        
        if records_processed > 0:
            records_per_second = records_processed / processing_time
            assert records_per_second >= 100, f"Processing rate too slow: {records_per_second:.0f} records/sec"


# Constants for consistent testing
TEST_FIPS_CODES = {
    'roanoke': '51770',
    'virginia_beach': '51810', 
    'alexandria': '51510',
    'test_region': '51999'
}

TEST_REGIONS = [
    'roanoke_city_va',
    'virginia_beach_va', 
    'alexandria_va',
    'test_region_1',
    'test_region_2'
]

SAMPLE_TRUST_NAMES = [
    'Smith Family Revocable Trust',
    'Jones Living Trust',
    'Brown Estate Trust',
    'The Williams Trust Agreement'
]

SAMPLE_CHURCH_NAMES = [
    'First Baptist Church',
    'Holy Trinity Episcopal Church', 
    'St. Mary Catholic Church',
    'Grace Community Church'
]

SAMPLE_BUSINESS_NAMES = [
    'ABC Properties LLC',
    'Smith Construction Inc',
    'Real Estate Holdings Company',
    'Investment Properties Group'
]

SAMPLE_NICHE_TYPES = [
    'liens',
    'foreclosure', 
    'bankruptcy',
    'landlord',
    'probate'
]

if __name__ == '__main__':
    # Demo usage of test fixtures
    print("=== TEST FIXTURES DEMO ===")
    
    # Create sample data
    property_data = TestDataFactory.create_sample_property_data(10)
    print(f"Created property data with {len(property_data)} records")
    print(f"Columns: {list(property_data.columns)}")
    
    # Create niche data
    liens_data = TestDataFactory.create_niche_data('liens', 5)
    print(f"\nCreated liens data with {len(liens_data)} records")
    print(f"Columns: {list(liens_data.columns)}")
    
    # Create test region
    builder = TestRegionBuilder()
    try:
        region_dir = builder.create_region(
            'demo_region',
            include_niche_files=['liens', 'foreclosure']
        )
        print(f"\nCreated test region at: {region_dir}")
        
        files = list(region_dir.glob("*"))
        print(f"Files created: {[f.name for f in files]}")
        
    finally:
        builder.cleanup()
        print("\nCleaned up test region")
    
    print("\n=== FIXTURES READY FOR TESTING ===")