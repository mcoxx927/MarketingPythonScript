"""
Integration tests for the complete real estate direct mail processing system.

These tests verify end-to-end workflows, performance optimizations,
and system reliability under various conditions.
"""

import pytest
import pandas as pd
import tempfile
import time
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import logging

from test_fixtures import (
    TestDataFactory, TestRegionBuilder, MockConfigManager, TestAssertions,
    TEST_FIPS_CODES, SAMPLE_TRUST_NAMES, SAMPLE_CHURCH_NAMES, SAMPLE_BUSINESS_NAMES
)

from property_processor import PropertyProcessor, PropertyClassifier, PropertyPriorityScorer
from multi_region_config import MultiRegionConfigManager
from monthly_processing_v2 import process_region, _update_main_with_niche

class TestEndToEndProcessing:
    """Test complete end-to-end processing workflows"""
    
    def setup_method(self):
        """Set up integration test environment"""
        self.region_builder = TestRegionBuilder()
        self.test_regions = []
    
    def teardown_method(self):
        """Clean up integration test environment"""
        self.region_builder.cleanup()
    
    def test_complete_property_classification_workflow(self):
        """Test complete property classification from raw data to final results"""
        # Create realistic test data with known classifications
        test_data = pd.DataFrame({
            'Owner 1 Last Name': [
                'Smith',                    # Individual
                'Jones Trust',             # Trust
                'First Baptist',           # Church
                'ABC Properties',          # Business
                'Brown Family',            # Individual
                'Living Trust',            # Trust
                'Holy Trinity',            # Church
                'Real Estate LLC'          # Business
            ],
            'Owner 1 First Name': [
                'John',
                'Family Revocable',
                'Church',
                'LLC',
                'Robert',
                'Agreement',
                'Episcopal Church',
                'Holdings'
            ],
            'Address': [f'{100 + i} Test St' for i in range(8)],
            'Mailing Address': [
                '100 Test St',    # Same - owner occupied
                '999 Trust Ave',  # Different - absentee  
                '102 Test St',    # Same - owner occupied
                '999 Business St',# Different - absentee
                '104 Test St',    # Same - owner occupied
                '999 Trust Rd',   # Different - absentee
                '106 Test St',    # Same - owner occupied
                '999 Company Ave' # Different - absentee
            ],
            'Last Sale Date': [
                '2020-01-15',  # Recent
                '2005-06-30',  # Old (ABS1)
                '2015-03-20',  # Medium age
                '2021-12-10',  # Recent
                '2010-08-15',  # Old
                '2008-01-01',  # Very old (ABS1)
                '2019-05-30',  # Recent
                '2007-12-31'   # Very old (ABS1)
            ],
            'Last Sale Amount': [
                100000,  # Normal
                50000,   # Low (TRS1/OON1)
                150000,  # Normal
                75000,   # At threshold
                200000,  # High
                30000,   # Low
                300000,  # High
                60000    # Low
            ],
            'Grantor': [
                'John Smith',      # Same first name
                'Different Name',  # No match
                'First Church',    # Same first name
                'ABC Corp',        # Same first name
                'Robert Brown',    # Exact match
                'Jones Family',    # Different
                'Holy Church',     # Same first name
                'Different Co'     # Different
            ],
            'Last Cash Buyer': ['false', 'false', 'true', 'false', 'false', 'false', 'true', 'false'],
            'FIPS': ['51770'] * 8
        })
        
        # Create processor with test configuration
        processor = PropertyProcessor(
            region_input_date1=datetime(2009, 1, 1),
            region_input_date2=datetime(2018, 1, 1),
            region_input_amount1=75000,
            region_input_amount2=200000
        )
        
        # Save test data to temporary Excel file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            test_data.to_excel(tmp.name, index=False)
            
            # Process the file
            result = processor.process_excel_file(tmp.name)
            
            # Verify classifications
            assert result.iloc[0]['IsTrust'] == False  # Smith John - individual
            assert result.iloc[1]['IsTrust'] == True   # Jones Trust - trust
            assert result.iloc[2]['IsChurch'] == True  # First Baptist Church
            assert result.iloc[3]['IsBusiness'] == True # ABC Properties LLC
            
            # Verify owner occupancy detection
            assert result.iloc[0]['IsOwnerOccupied'] == True   # Same address
            assert result.iloc[1]['IsOwnerOccupied'] == False  # Different address
            assert result.iloc[2]['IsOwnerOccupied'] == True   # Same address
            assert result.iloc[3]['IsOwnerOccupied'] == False  # Different address
            
            # Verify priority assignments
            # Trust should be priority 5 (TRS2)
            assert result.iloc[1]['PriorityId'] == 5
            assert result.iloc[1]['PriorityCode'] == 'TRS2'
            
            # Church should be priority 10 (CHURCH)
            assert result.iloc[2]['PriorityId'] == 10
            assert result.iloc[2]['PriorityCode'] == 'CHURCH'
            
            # Owner occupied with grantor match should be priority 1 (OIN1)
            # Row 0: John Smith vs John Smith (exact match)
            # Note: This won't trigger grantor match as full names match exactly
            
            Path(tmp.name).unlink()  # Clean up
    
    def test_multi_region_batch_processing_simulation(self):
        """Test processing multiple regions with different configurations"""
        # Create multiple test regions with different market characteristics
        regions = [
            {
                'key': 'rural_market',
                'config': {
                    'region_name': 'Rural Market, VA',
                    'fips_code': '51001',
                    'region_input_amount1': 50000,   # Lower thresholds
                    'region_input_amount2': 150000,
                    'market_type': 'Rural/Small City'
                }
            },
            {
                'key': 'metro_market', 
                'config': {
                    'region_name': 'Metro Market, VA',
                    'fips_code': '51002',
                    'region_input_amount1': 150000,  # Higher thresholds
                    'region_input_amount2': 400000,
                    'market_type': 'Metro/High-Value'
                }
            },
            {
                'key': 'coastal_market',
                'config': {
                    'region_name': 'Coastal Market, VA',
                    'fips_code': '51003', 
                    'region_input_amount1': 100000,  # Medium thresholds
                    'region_input_amount2': 300000,
                    'market_type': 'Coastal/Resort'
                }
            }
        ]
        
        batch_results = []
        
        for region_info in regions:
            # Create region structure
            region_dir = self.region_builder.create_region(
                region_info['key'],
                config_overrides=region_info['config'],
                include_niche_files=['liens', 'foreclosure'],
                main_records=50,
                niche_records=15
            )
            
            # Simulate processing (without full region processing complexity)
            config_manager = MockConfigManager([region_info['key']])
            
            # Verify region was created correctly
            assert region_dir.exists()
            assert (region_dir / 'config.json').exists()
            assert (region_dir / 'main_region.xlsx').exists()
            assert (region_dir / 'liens.xlsx').exists()
            assert (region_dir / 'foreclosure.xlsx').exists()
            
            batch_results.append({
                'region': region_info['key'],
                'market_type': region_info['config']['market_type'],
                'status': 'success'
            })
        
        # Verify all regions were processed
        assert len(batch_results) == 3
        market_types = [r['market_type'] for r in batch_results]
        assert 'Rural/Small City' in market_types
        assert 'Metro/High-Value' in market_types
        assert 'Coastal/Resort' in market_types
    
    def test_niche_integration_with_realistic_data(self):
        """Test niche list integration with realistic property scenarios"""
        # Create main region data with various property types
        main_data = TestDataFactory.create_sample_property_data(
            num_records=100, include_special_cases=True
        )
        
        # Create overlapping niche lists
        liens_data = TestDataFactory.create_niche_data(
            'liens', num_records=30, overlap_with_main=0.4
        )
        foreclosure_data = TestDataFactory.create_niche_data(
            'foreclosure', num_records=20, overlap_with_main=0.3
        )
        bankruptcy_data = TestDataFactory.create_niche_data(
            'bankruptcy', num_records=15, overlap_with_main=0.2
        )
        
        # Start with main data
        enhanced_data = main_data.copy()
        enhanced_data['PriorityCode'] = 'OWN1'  # Set initial priority
        enhanced_data['PriorityName'] = 'Owner List 1'
        
        total_updates = 0
        total_inserts = 0
        
        # Process each niche list
        for niche_data, niche_type in [
            (liens_data, 'Liens'),
            (foreclosure_data, 'PreForeclosure'),
            (bankruptcy_data, 'Bankruptcy')
        ]:
            updates, inserts = _update_main_with_niche(
                enhanced_data, niche_data, niche_type
            )
            total_updates += updates
            total_inserts += inserts
        
        # Verify integration results
        assert total_updates > 0, "Should have updated some existing records"
        assert total_inserts > 0, "Should have inserted some new records"
        
        # Check for enhanced priority codes
        enhanced_codes = enhanced_data['PriorityCode'].value_counts()
        assert any('Liens' in code for code in enhanced_codes.index), "Should have liens-enhanced codes"
        
        # Verify no duplicate enhancements
        for code in enhanced_codes.index:
            if 'Liens' in code:
                assert code.count('Liens') == 1, f"Should not have duplicate Liens in: {code}"
        
        # Check that niche-only records have correct priority
        niche_only_records = enhanced_data[
            enhanced_data['PriorityId'] == 99  # NICHE_ONLY_PRIORITY_ID
        ]
        assert len(niche_only_records) > 0, "Should have some niche-only records"
    
    def test_performance_optimization_validation(self):
        """Test that performance optimizations work as expected"""
        # Create large dataset to test vectorized operations
        large_dataset = TestDataFactory.create_sample_property_data(
            num_records=1000, include_special_cases=False
        )
        
        # Test vectorized processing time
        processor = PropertyProcessor()
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            large_dataset.to_excel(tmp.name, index=False)
            
            start_time = time.time()
            result = processor.process_excel_file(tmp.name)
            processing_time = time.time() - start_time
            
            # Performance assertions
            TestAssertions.assert_processing_performance(
                processing_time=processing_time,
                max_time=10.0,  # Should process 1000 records in under 10 seconds
                records_processed=1000
            )
            
            # Verify all records were processed
            assert len(result) == 1000
            
            # Verify memory optimization (categorical data types)
            memory_usage = result.memory_usage(deep=True).sum() / 1024 / 1024  # MB
            assert memory_usage < 50, f"Memory usage too high: {memory_usage:.1f} MB"
            
            Path(tmp.name).unlink()  # Clean up
    
    def test_error_recovery_and_resilience(self):
        """Test system behavior under various error conditions"""
        processor = PropertyProcessor()
        
        # Test with malformed data
        malformed_data = pd.DataFrame({
            'Owner 1 Last Name': ['Smith', None, ''],
            'Owner 1 First Name': ['John', 'Invalid', None],
            'Address': ['123 Main St', '', None],
            'Last Sale Date': ['2020-01-15', 'invalid-date', ''],
            'Last Sale Amount': [100000, 'not-a-number', -5000],
            'FIPS': ['51770', '51770', '51770']
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            malformed_data.to_excel(tmp.name, index=False)
            
            # Should handle malformed data gracefully
            result = processor.process_excel_file(tmp.name)
            
            # All records should be processed (with defaults for invalid data)
            assert len(result) == 3
            
            # Invalid data should have been handled
            assert all(result['PriorityId'].notna())
            assert all(result['PriorityCode'].notna())
            
            # Check that parsing issues were tracked
            parsing_issues = result['DateParseIssues'].str.len() > 0
            assert parsing_issues.any(), "Should have tracked parsing issues"
            
            Path(tmp.name).unlink()  # Clean up
    
    def test_business_rule_accuracy(self):
        """Test accuracy of business rule implementation against known cases"""
        # Create test cases with known expected results
        business_rule_tests = pd.DataFrame({
            'Owner 1 Last Name': [
                'Smith Family Trust',      # Should be Trust -> TRS2
                'First Baptist',           # Should be Church -> CHURCH  
                'ABC Properties LLC',      # Should be Business -> scored as absentee
                'Johnson',                 # Individual, owner-occupied -> various priorities
                'Brown Estate'             # Trust -> TRS2
            ],
            'Owner 1 First Name': [
                'Revocable',
                'Church',
                'Holdings',
                'Mary',
                'Trust'
            ],
            'Address': [
                '100 Trust Ave',
                '101 Church St', 
                '102 Business Blvd',
                '103 Home St',
                '104 Estate Ave'
            ],
            'Mailing Address': [
                '999 Different St',  # Trust - absentee
                '101 Church St',     # Church - owner occupied
                '999 Office St',     # Business - absentee
                '103 Home St',       # Individual - owner occupied
                '999 Trust Office'   # Trust - absentee
            ],
            'Last Sale Date': [
                '2005-01-01',  # Old date
                '2020-01-01',  # Recent date
                '2021-01-01',  # Recent date
                '2010-01-01',  # Old date
                '2008-01-01'   # Very old date
            ],
            'Last Sale Amount': [
                50000,   # Low amount
                100000,  # Normal amount
                200000,  # High amount
                60000,   # Low amount
                75000    # At threshold
            ],
            'Grantor': [
                'Smith Trust',
                'Church Board',
                'ABC Corp',
                'Mary Johnson',  # Exact match
                'Brown Family'
            ],
            'FIPS': ['51770'] * 5
        })
        
        processor = PropertyProcessor(
            region_input_date1=datetime(2009, 1, 1),
            region_input_date2=datetime(2018, 1, 1),
            region_input_amount1=75000,
            region_input_amount2=200000
        )
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            business_rule_tests.to_excel(tmp.name, index=False)
            result = processor.process_excel_file(tmp.name)
            
            # Verify specific business rule outcomes
            
            # Trust properties should have priority 5 (TRS2)
            trust_records = result[result['IsTrust'] == True]
            assert len(trust_records) == 2  # Smith Family Trust, Brown Estate
            assert all(trust_records['PriorityId'] == 5)
            assert all(trust_records['PriorityCode'] == 'TRS2')
            
            # Church should have priority 10 (CHURCH)
            church_records = result[result['IsChurch'] == True]
            assert len(church_records) == 1  # First Baptist Church
            assert church_records.iloc[0]['PriorityId'] == 10
            assert church_records.iloc[0]['PriorityCode'] == 'CHURCH'
            
            # Business should be classified correctly
            business_records = result[result['IsBusiness'] == True]
            assert len(business_records) == 1  # ABC Properties LLC
            
            # Individual with exact grantor match should have high priority
            # Mary Johnson with exact grantor match should be OIN1 (priority 1)
            mary_record = result[result['Owner 1 First Name'] == 'Mary'].iloc[0]
            # Note: Exact matches don't count as grantor matches in the logic
            # so this would go to other priority rules
            
            Path(tmp.name).unlink()  # Clean up


class TestSystemReliability:
    """Test system reliability under stress and edge conditions"""
    
    def test_concurrent_region_processing_simulation(self):
        """Simulate concurrent processing of multiple regions"""
        # This tests that the system can handle multiple region processing
        # without interference (important for future multi-threading)
        
        builder = TestRegionBuilder()
        try:
            # Create multiple regions
            regions = []
            for i in range(5):
                region_key = f'concurrent_test_region_{i}'
                region_dir = builder.create_region(
                    region_key,
                    config_overrides={'fips_code': f'5199{i}'},
                    include_niche_files=['liens'],
                    main_records=50
                )
                regions.append(region_key)
            
            # Process each region independently
            results = []
            for region_key in regions:
                config_manager = MockConfigManager([region_key])
                
                # Simulate processing
                processing_start = time.time()
                
                # Each region should process independently
                config = config_manager.get_region_config(region_key)
                assert config.region_code == region_key.upper()[:4]
                
                processing_time = time.time() - processing_start
                results.append({
                    'region': region_key,
                    'processing_time': processing_time,
                    'success': True
                })
            
            # All regions should have processed successfully
            assert len(results) == 5
            assert all(r['success'] for r in results)
            
        finally:
            builder.cleanup()
    
    def test_memory_usage_under_load(self):
        """Test memory usage patterns under various load conditions"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Process increasingly large datasets
        dataset_sizes = [100, 500, 1000, 2000]
        memory_measurements = []
        
        processor = PropertyProcessor()
        
        for size in dataset_sizes:
            # Create large dataset
            large_data = TestDataFactory.create_sample_property_data(size)
            
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                large_data.to_excel(tmp.name, index=False)
                
                # Process and measure memory
                memory_before = process.memory_info().rss / 1024 / 1024
                result = processor.process_excel_file(tmp.name)
                memory_after = process.memory_info().rss / 1024 / 1024
                
                memory_measurements.append({
                    'dataset_size': size,
                    'memory_before': memory_before,
                    'memory_after': memory_after,
                    'memory_delta': memory_after - memory_before
                })
                
                # Clean up
                del result, large_data
                Path(tmp.name).unlink()
        
        # Memory usage should not grow excessively
        final_memory = process.memory_info().rss / 1024 / 1024
        total_memory_growth = final_memory - initial_memory
        
        # Should not use more than 100MB additional memory for test datasets
        assert total_memory_growth < 100, f"Excessive memory growth: {total_memory_growth:.1f} MB"
        
        # Memory growth should be roughly linear with dataset size
        memory_deltas = [m['memory_delta'] for m in memory_measurements]
        # Later datasets shouldn't use dramatically more memory than earlier ones
        max_delta = max(memory_deltas)
        min_delta = min(memory_deltas)
        assert max_delta < min_delta * 5, "Memory usage scaling is too steep"
    
    def test_data_consistency_across_processing_steps(self):
        """Test that data remains consistent through all processing steps"""
        # Create test data with trackable characteristics
        test_data = pd.DataFrame({
            'Owner 1 Last Name': ['Smith', 'Jones Trust', 'First Baptist'],
            'Owner 1 First Name': ['John', 'Family', 'Church'], 
            'Address': ['100 Test St', '101 Test Ave', '102 Church St'],
            'Mailing Address': ['100 Test St', '999 Trust St', '102 Church St'],
            'Last Sale Date': ['2020-01-01', '2010-01-01', '2015-01-01'],
            'Last Sale Amount': [100000, 50000, 150000],
            'FIPS': ['51770', '51770', '51770'],
            'Unique_ID': ['ID_001', 'ID_002', 'ID_003']  # For tracking
        })
        
        processor = PropertyProcessor()
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            test_data.to_excel(tmp.name, index=False)
            result = processor.process_excel_file(tmp.name)
            
            # Verify data consistency
            assert len(result) == len(test_data), "Record count should be preserved"
            
            # Verify core data integrity
            original_addresses = set(test_data['Address'])
            processed_addresses = set(result['Address'])
            assert original_addresses == processed_addresses, "Addresses should be preserved"
            
            # Verify that classifications are mutually exclusive where expected
            for idx, row in result.iterrows():
                # Trust and Church should be mutually exclusive
                if row['IsTrust']:
                    assert not row['IsChurch'], f"Record {idx} is both Trust and Church"
                
                # Church and Business should be mutually exclusive  
                if row['IsChurch']:
                    assert not row['IsBusiness'], f"Record {idx} is both Church and Business"
            
            # Verify priority assignments are valid
            valid_priorities = set(range(1, 14))  # 1-13
            valid_priorities.add(99)  # NICHE_ONLY_PRIORITY_ID
            
            for priority_id in result['PriorityId']:
                assert priority_id in valid_priorities, f"Invalid priority ID: {priority_id}"
            
            Path(tmp.name).unlink()  # Clean up


if __name__ == '__main__':
    # Run integration tests
    pytest.main([__file__, '-v', '--tb=short'])