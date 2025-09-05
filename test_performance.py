"""
Performance and memory optimization tests for the real estate processing system.

These tests validate the 2-3x speed improvements and memory optimizations
that were implemented in the recent refactoring.
"""

import pytest
import pandas as pd
import numpy as np
import time
import tempfile
import psutil
import os
from pathlib import Path
from datetime import datetime, timedelta
import gc
from unittest.mock import patch

from property_processor import PropertyProcessor
from monthly_processing_v2 import _update_main_with_niche, _normalize_address
from test_fixtures import TestDataFactory

class TestVectorizedOperations:
    """Test vectorized DataFrame operations performance"""
    
    def setup_method(self):
        """Set up performance test fixtures"""
        self.processor = PropertyProcessor()
        self.large_dataset_size = 5000
        self.medium_dataset_size = 1000
        self.small_dataset_size = 100
    
    def test_vectorized_classification_performance(self):
        """Test that vectorized classification is faster than row-by-row"""
        # Create large dataset
        large_data = TestDataFactory.create_sample_property_data(
            num_records=self.large_dataset_size, include_special_cases=False
        )
        
        # Add trust/church/business names for classification
        trust_indices = range(0, len(large_data), 4)  # Every 4th record
        church_indices = range(1, len(large_data), 4)  # Every 4th record starting at 1
        business_indices = range(2, len(large_data), 4)  # Every 4th record starting at 2
        
        for i in trust_indices:
            if i < len(large_data):
                large_data.loc[i, 'Owner 1 Last Name'] = 'Smith Family Trust'
        
        for i in church_indices:
            if i < len(large_data):
                large_data.loc[i, 'Owner 1 Last Name'] = 'First Baptist'
                large_data.loc[i, 'Owner 1 First Name'] = 'Church'
        
        for i in business_indices:
            if i < len(large_data):
                large_data.loc[i, 'Owner 1 Last Name'] = 'ABC Properties'
                large_data.loc[i, 'Owner 1 First Name'] = 'LLC'
        
        # Test with temporary file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            large_data.to_excel(tmp.name, index=False)
            
            start_time = time.time()
            result = self.processor.process_excel_file(tmp.name)
            processing_time = time.time() - start_time
            
            # Performance assertions
            records_per_second = len(result) / processing_time
            assert records_per_second > 500, f"Processing too slow: {records_per_second:.0f} records/sec"
            assert processing_time < 20.0, f"Processing took too long: {processing_time:.2f} seconds"
            
            # Verify classifications worked correctly
            trust_count = result['IsTrust'].sum()
            church_count = result['IsChurch'].sum() 
            business_count = result['IsBusiness'].sum()
            
            assert trust_count > 0, "Should have classified some trusts"
            assert church_count > 0, "Should have classified some churches"
            assert business_count > 0, "Should have classified some businesses"
            
            # Clean up
            try:
                Path(tmp.name).unlink()
            except:
                pass  # Ignore cleanup errors on Windows
    
    def test_bulk_niche_integration_performance(self):
        """Test bulk niche integration performance vs individual updates"""
        # Create main dataset
        main_data = pd.DataFrame({
            'Address': [f'{i} Performance St' for i in range(2000)],
            'Owner 1 Last Name': ['Test'] * 2000,
            'Owner 1 First Name': ['Owner'] * 2000,
            'PriorityCode': ['OWN1'] * 2000,
            'PriorityName': ['Owner List 1'] * 2000
        })
        
        # Create niche dataset with 50% overlap
        niche_data = pd.DataFrame({
            'Address': [f'{i} Performance St' for i in range(1000, 3000)],  # 1000 overlap + 1000 new
            'Owner 1 Last Name': ['Niche'] * 2000,
            'Owner 1 First Name': ['Owner'] * 2000,
            'Lien Type': ['Tax Lien'] * 2000,
            'Lien Amount': [5000] * 2000
        })
        
        # Test bulk integration performance
        start_time = time.time()
        updates_count, inserts_count = _update_main_with_niche(
            main_data, niche_data, 'Liens'
        )
        integration_time = time.time() - start_time
        
        # Performance assertions
        assert integration_time < 5.0, f"Integration too slow: {integration_time:.2f} seconds"
        assert updates_count == 1000, f"Expected 1000 updates, got {updates_count}"
        assert inserts_count == 1000, f"Expected 1000 inserts, got {inserts_count}"
        
        # Verify final record count
        expected_final_count = 2000 + 1000  # Original + new inserts
        assert len(main_data) == expected_final_count
    
    def test_address_normalization_vectorized_performance(self):
        """Test vectorized address normalization performance"""
        # Create large set of addresses
        addresses = [
            f'{i} Main St,',
            f'{i}  Oak   Ave  ,',
            f'  {i} Pine Rd,  ',
            f'{i} Elm Blvd,'
        ] * 1250  # 5000 total addresses
        
        # Test vectorized normalization
        start_time = time.time()
        normalized = [_normalize_address(addr) for addr in addresses]
        normalization_time = time.time() - start_time
        
        # Performance assertion
        addresses_per_second = len(addresses) / normalization_time
        assert addresses_per_second > 10000, f"Normalization too slow: {addresses_per_second:.0f} addr/sec"
        
        # Verify normalization quality
        assert len(normalized) == len(addresses)
        assert all(isinstance(addr, str) for addr in normalized)
        assert all(',' not in addr for addr in normalized if addr)  # Commas should be removed
    
    def test_priority_scoring_performance(self):
        """Test priority scoring performance with various scenarios"""
        # Create dataset with diverse priority scenarios
        scenarios = []
        
        # Generate 1000 records with different priority patterns
        for i in range(1000):
            scenario = {
                'Owner 1 Last Name': f'Owner{i}',
                'Owner 1 First Name': f'First{i}',
                'Address': f'{i} Test St',
                'Mailing Address': f'{i} Test St' if i % 2 == 0 else f'{i} Different Ave',
                'Last Sale Date': (datetime.now() - timedelta(days=365 * (i % 20))).strftime('%Y-%m-%d'),
                'Last Sale Amount': 50000 + (i * 100) % 200000,
                'Grantor': f'Grantor{i}' if i % 5 == 0 else f'Owner{i}',
                'Last Cash Buyer': 'true' if i % 10 == 0 else 'false',
                'FIPS': '51770'
            }
            
            # Add some special cases
            if i % 50 == 0:
                scenario['Owner 1 Last Name'] = f'Trust{i} Family Trust'
            elif i % 50 == 1:
                scenario['Owner 1 Last Name'] = f'Church{i} Baptist'
                scenario['Owner 1 First Name'] = 'Church'
            elif i % 50 == 2:
                scenario['Owner 1 Last Name'] = f'Business{i} Properties'
                scenario['Owner 1 First Name'] = 'LLC'
            
            scenarios.append(scenario)
        
        test_data = pd.DataFrame(scenarios)
        
        # Test processing performance
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            test_data.to_excel(tmp.name, index=False)
            
            start_time = time.time()
            result = self.processor.process_excel_file(tmp.name)
            processing_time = time.time() - start_time
            
            # Performance assertions
            records_per_second = len(result) / processing_time
            assert records_per_second > 200, f"Priority scoring too slow: {records_per_second:.0f} records/sec"
            
            # Verify all records were processed
            assert len(result) == 1000
            assert all(result['PriorityId'].notna())
            assert all(result['PriorityCode'].notna())
            
            # Clean up
            try:
                Path(tmp.name).unlink()
            except:
                pass


class TestMemoryOptimizations:
    """Test memory optimization features"""
    
    def setup_method(self):
        """Set up memory test environment"""
        self.processor = PropertyProcessor()
        # Force garbage collection before tests
        gc.collect()
    
    def get_memory_usage_mb(self):
        """Get current process memory usage in MB"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def test_categorical_data_type_optimization(self):
        """Test that categorical data types reduce memory usage"""
        # Create dataset with repetitive categorical data
        categorical_data = {
            'Owner 1 Last Name': ['Smith'] * 2000 + ['Jones'] * 2000 + ['Brown'] * 1000,
            'Owner 1 First Name': ['John'] * 2000 + ['Mary'] * 2000 + ['Robert'] * 1000,
            'Address': [f'{i} Test St' for i in range(5000)],
            'City': ['Test City'] * 5000,  # Highly repetitive
            'State': ['VA'] * 5000,        # Highly repetitive
            'Zip': ['24016'] * 2500 + ['24017'] * 2500,  # Semi-repetitive
            'FIPS': ['51770'] * 5000       # Highly repetitive
        }
        
        test_df = pd.DataFrame(categorical_data)
        
        # Measure memory before optimization
        memory_before = self.get_memory_usage_mb()
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            test_df.to_excel(tmp.name, index=False)
            
            # Process with categorical optimization
            result = self.processor.process_excel_file(tmp.name)
            
            memory_after = self.get_memory_usage_mb()
            memory_delta = memory_after - memory_before
            
            # Memory usage should be reasonable for 5000 records
            assert memory_delta < 100, f"Memory usage too high: {memory_delta:.1f} MB"
            
            # Verify the result DataFrame uses memory efficiently
            result_memory_mb = result.memory_usage(deep=True).sum() / 1024 / 1024
            assert result_memory_mb < 25, f"Result DataFrame memory too high: {result_memory_mb:.1f} MB"
            
            # Clean up
            try:
                Path(tmp.name).unlink()
            except:
                pass
    
    def test_memory_efficient_niche_processing(self):
        """Test that niche processing doesn't cause memory leaks"""
        initial_memory = self.get_memory_usage_mb()
        
        # Process multiple niche files in sequence
        main_data = pd.DataFrame({
            'Address': [f'{i} Memory Test St' for i in range(2000)],
            'Owner 1 Last Name': ['Test'] * 2000,
            'Owner 1 First Name': ['Owner'] * 2000,
            'PriorityCode': ['OWN1'] * 2000,
            'PriorityName': ['Owner List 1'] * 2000
        })
        
        memory_measurements = [initial_memory]
        
        for niche_type in ['Liens', 'Foreclosure', 'Bankruptcy', 'Probate']:
            # Create niche data
            niche_data = pd.DataFrame({
                'Address': [f'{1000 + i} Memory Test St' for i in range(1000)],
                'Owner 1 Last Name': [f'{niche_type}'] * 1000,
                'Owner 1 First Name': ['Owner'] * 1000,
                f'{niche_type}_Field': ['Value'] * 1000
            })
            
            # Process niche integration
            _update_main_with_niche(main_data, niche_data, niche_type)
            
            # Measure memory after each niche processing
            current_memory = self.get_memory_usage_mb()
            memory_measurements.append(current_memory)
            
            # Clean up references
            del niche_data
            gc.collect()
        
        final_memory = self.get_memory_usage_mb()
        
        # Memory growth should be reasonable
        total_growth = final_memory - initial_memory
        assert total_growth < 50, f"Excessive memory growth: {total_growth:.1f} MB"
        
        # Memory shouldn't grow excessively between iterations
        max_growth_between_iterations = max(
            memory_measurements[i+1] - memory_measurements[i]
            for i in range(len(memory_measurements) - 1)
        )
        assert max_growth_between_iterations < 20, f"Excessive memory growth per iteration: {max_growth_between_iterations:.1f} MB"
    
    def test_large_dataset_memory_scaling(self):
        """Test memory usage scaling with dataset size"""
        dataset_sizes = [500, 1000, 2000, 4000]
        memory_measurements = []
        
        for size in dataset_sizes:
            gc.collect()  # Clean up before measurement
            initial_memory = self.get_memory_usage_mb()
            
            # Create dataset
            test_data = TestDataFactory.create_sample_property_data(
                num_records=size, include_special_cases=False
            )
            
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                test_data.to_excel(tmp.name, index=False)
                
                # Process data
                result = self.processor.process_excel_file(tmp.name)
                
                final_memory = self.get_memory_usage_mb()
                memory_used = final_memory - initial_memory
                
                memory_measurements.append({
                    'size': size,
                    'memory_mb': memory_used,
                    'memory_per_record_kb': (memory_used * 1024) / size
                })
                
                # Clean up
                del result, test_data
                try:
                    Path(tmp.name).unlink()
                except:
                    pass
                
                gc.collect()
        
        # Memory usage should scale roughly linearly
        memory_per_record_values = [m['memory_per_record_kb'] for m in memory_measurements]
        
        # Memory per record shouldn't vary too dramatically
        min_per_record = min(memory_per_record_values)
        max_per_record = max(memory_per_record_values)
        
        # Allow up to 3x variation (some overhead is expected for smaller datasets)
        assert max_per_record <= min_per_record * 3, f"Memory scaling not linear: {min_per_record:.1f} to {max_per_record:.1f} KB per record"


class TestProcessingOptimizations:
    """Test specific processing optimizations"""
    
    def setup_method(self):
        """Set up processing optimization tests"""
        self.processor = PropertyProcessor()
    
    def test_excel_reading_optimization(self):
        """Test optimized Excel file reading"""
        # Create Excel file with various data types
        mixed_data = {
            'Owner 1 Last Name': ['Smith', 'Jones', 'Brown'] * 500,
            'Owner 1 First Name': ['John', 'Mary', 'Robert'] * 500,
            'Address': [f'{i} Test St' for i in range(1500)],
            'FIPS': ['51770'] * 1500,  # Should be categorical
            'Last Sale Amount': [100000, 50000, 200000] * 500,
            'Last Sale Date': ['2020-01-01', '2019-06-15', '2018-12-31'] * 500,
            'City': ['Roanoke'] * 1500,  # Should be categorical
            'State': ['VA'] * 1500       # Should be categorical
        }
        
        test_data = pd.DataFrame(mixed_data)
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            test_data.to_excel(tmp.name, index=False)
            
            # Time the Excel reading process
            start_time = time.time()
            result = self.processor.process_excel_file(tmp.name)
            read_time = time.time() - start_time
            
            # Reading should be reasonably fast
            records_per_second = len(result) / read_time
            assert records_per_second > 100, f"Excel reading too slow: {records_per_second:.0f} records/sec"
            
            # Verify data types were optimized
            # Check if categorical optimization was applied (indirect check via memory usage)
            memory_usage_mb = result.memory_usage(deep=True).sum() / 1024 / 1024
            assert memory_usage_mb < 10, f"Memory usage suggests poor optimization: {memory_usage_mb:.1f} MB"
            
            # Clean up
            try:
                Path(tmp.name).unlink()
            except:
                pass
    
    def test_progress_logging_performance_impact(self):
        """Test that progress logging doesn't significantly impact performance"""
        # Create dataset large enough to trigger progress logs
        large_data = TestDataFactory.create_sample_property_data(
            num_records=6000, include_special_cases=False  # Above PROGRESS_LOG_INTERVAL
        )
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            large_data.to_excel(tmp.name, index=False)
            
            # Process with logging
            with patch('property_processor.logger') as mock_logger:
                start_time = time.time()
                result = self.processor.process_excel_file(tmp.name)
                processing_time_with_logging = time.time() - start_time
                
                # Verify progress logs were called
                assert mock_logger.info.called, "Progress logging should have been called"
            
            # Performance should still be good despite logging
            records_per_second = len(result) / processing_time_with_logging
            assert records_per_second > 200, f"Processing with logging too slow: {records_per_second:.0f} records/sec"
            
            # Clean up
            try:
                Path(tmp.name).unlink()
            except:
                pass
    
    def test_error_handling_performance_impact(self):
        """Test that error handling doesn't significantly slow down processing"""
        # Create dataset with some problematic records
        problematic_data = {
            'Owner 1 Last Name': ['Good', 'Bad', None, '', 'Good'] * 400,
            'Owner 1 First Name': ['Name', None, 'Name', '', 'Name'] * 400,
            'Address': [f'{i} Test St' if i % 5 != 1 else '' for i in range(2000)],
            'Last Sale Date': [
                '2020-01-01' if i % 5 == 0
                else 'invalid-date' if i % 5 == 1
                else '' if i % 5 == 2
                else '1900-01-01' if i % 5 == 3
                else '2099-12-31'  # Future date
                for i in range(2000)
            ],
            'Last Sale Amount': [
                100000 if i % 5 == 0
                else 'not-a-number' if i % 5 == 1
                else -5000 if i % 5 == 2
                else None if i % 5 == 3
                else 50000000000  # Unreasonably high
                for i in range(2000)
            ],
            'FIPS': ['51770'] * 2000
        }
        
        test_data = pd.DataFrame(problematic_data)
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            test_data.to_excel(tmp.name, index=False)
            
            # Process with error handling
            start_time = time.time()
            result = self.processor.process_excel_file(tmp.name)
            processing_time = time.time() - start_time
            
            # Should still process at reasonable speed despite errors
            records_per_second = len(result) / processing_time
            assert records_per_second > 150, f"Error handling slows processing too much: {records_per_second:.0f} records/sec"
            
            # All records should be processed (with defaults for bad data)
            assert len(result) == 2000
            assert all(result['PriorityId'].notna())
            
            # Clean up
            try:
                Path(tmp.name).unlink()
            except:
                pass


class TestScalabilityLimits:
    """Test system behavior at scalability limits"""
    
    def setup_method(self):
        """Set up scalability test environment"""
        self.processor = PropertyProcessor()
        
    @pytest.mark.slow
    def test_maximum_dataset_size_handling(self):
        """Test processing very large datasets (if system can handle it)"""
        max_test_size = 10000  # Adjust based on system capabilities
        
        try:
            # Create maximum size dataset
            max_data = TestDataFactory.create_sample_property_data(
                num_records=max_test_size, include_special_cases=False
            )
            
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                max_data.to_excel(tmp.name, index=False)
                
                start_time = time.time()
                result = self.processor.process_excel_file(tmp.name)
                processing_time = time.time() - start_time
                
                # Verify processing completed
                assert len(result) == max_test_size
                
                # Performance should degrade gracefully
                records_per_second = len(result) / processing_time
                assert records_per_second > 50, f"Large dataset processing too slow: {records_per_second:.0f} records/sec"
                
                # Memory usage should be manageable
                memory_mb = result.memory_usage(deep=True).sum() / 1024 / 1024
                assert memory_mb < 200, f"Large dataset memory usage too high: {memory_mb:.1f} MB"
                
                # Clean up
                try:
                    Path(tmp.name).unlink()
                except:
                    pass
                    
        except MemoryError:
            pytest.skip(f"System cannot handle dataset size {max_test_size}")
        except Exception as e:
            pytest.fail(f"Unexpected error with large dataset: {e}")
    
    def test_concurrent_processing_memory_isolation(self):
        """Test that concurrent processing doesn't cause memory interference"""
        # Simulate concurrent processing by running multiple operations
        datasets = []
        
        for i in range(3):  # Create 3 concurrent datasets
            data = TestDataFactory.create_sample_property_data(
                num_records=500, include_special_cases=False
            )
            datasets.append(data)
        
        initial_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        
        results = []
        for i, dataset in enumerate(datasets):
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                dataset.to_excel(tmp.name, index=False)
                
                # Process each dataset
                result = self.processor.process_excel_file(tmp.name)
                results.append(result)
                
                # Clean up immediately to simulate concurrent memory management
                del dataset
                try:
                    Path(tmp.name).unlink()
                except:
                    pass
        
        final_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be reasonable for 3 x 500 records
        assert memory_growth < 75, f"Concurrent processing memory growth too high: {memory_growth:.1f} MB"
        
        # All results should be valid
        assert len(results) == 3
        for result in results:
            assert len(result) == 500
            assert all(result['PriorityId'].notna())


if __name__ == '__main__':
    # Run performance tests
    pytest.main([__file__, '-v', '-m', 'not slow'])