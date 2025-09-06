"""
Comprehensive Priority Scoring Validation Tests

Tests the critical business logic for property priority scoring based on real
Roanoke County data and business rules. Validates the 13-level priority system
and region-specific thresholds.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from property_processor import PropertyProcessor, PropertyClassification, PropertyPriority


class TestPriorityScoring:
    
    def setup_method(self):
        """Setup test instance with Roanoke County parameters"""
        # Use actual Roanoke County parameters from real data processing
        self.roanoke_date1 = datetime(2017, 9, 3)  # ABS1 cutoff
        self.roanoke_date2 = datetime(2024, 9, 3)  # BUY cutoff  
        self.roanoke_amount1 = 40000.0  # Low threshold
        self.roanoke_amount2 = 350000.0  # High threshold
        
        self.processor = PropertyProcessor(
            region_input_date1=self.roanoke_date1,
            region_input_date2=self.roanoke_date2,
            region_input_amount1=self.roanoke_amount1,
            region_input_amount2=self.roanoke_amount2
        )
    
    def test_roanoke_county_thresholds(self):
        """Test that Roanoke County thresholds are correctly applied"""
        scorer = self.processor.scorer
        
        assert scorer.region_input_date1 == self.roanoke_date1
        assert scorer.region_input_date2 == self.roanoke_date2
        assert scorer.region_input_amount1 == self.roanoke_amount1
        assert scorer.region_input_amount2 == self.roanoke_amount2
        
        # Verify priority definitions exist
        assert len(scorer.priorities) == 12  # 12 priority levels defined (including missing ones)
        assert 1 in scorer.priorities  # OIN1
        assert 11 in scorer.priorities  # DEFAULT
        assert 13 in scorer.priorities  # OWN20
    
    def test_trust_priority_scoring(self):
        """Test trust properties get priority 5 (TRS2)"""
        # Create test property data
        property_data = pd.Series({
            'Last Sale Date': '2020-01-01',
            'Last Sale Amount': '$100,000',
            'Address': '123 Main St',
            'Mailing Address': '456 Oak Ave'
        })
        
        # Create trust classification
        classification = PropertyClassification(is_trust=True)
        
        # Score the property
        priority = self.processor.scorer.score_property(property_data, classification)
        
        assert priority.priority_id == 5
        assert priority.priority_code == "TRS2"
        assert "Trust" in priority.priority_name
    
    def test_church_priority_scoring(self):
        """Test church properties get priority 10"""
        property_data = pd.Series({
            'Last Sale Date': '2020-01-01',
            'Last Sale Amount': '$100,000',
            'Address': '123 Church St',
            'Mailing Address': '123 Church St'
        })
        
        # Create church classification
        classification = PropertyClassification(is_church=True)
        
        # Score the property
        priority = self.processor.scorer.score_property(property_data, classification)
        
        assert priority.priority_id == 10
        assert priority.priority_code == "CHURCH"
        assert "Church" in priority.priority_name
    
    def test_owner_occupied_priority_hierarchy(self):
        """Test owner occupied property priority scoring hierarchy"""
        base_data = {
            'Address': '123 Main St',
            'Mailing Address': '123 Main St'  # Same = owner occupied
        }
        
        test_cases = [
            # OIN1: Owner occupied + grantor match = Priority 1
            {
                'data': {**base_data, 'Last Sale Date': '2020-01-01', 'Last Sale Amount': '$100,000'},
                'classification': PropertyClassification(is_owner_occupied=True, owner_grantor_match=True),
                'expected_priority': 1,
                'expected_code': 'OIN1',
                'description': 'Owner occupied with grantor match'
            },
            
            # OWN1: Owner occupied + old sale = Priority 2  
            {
                'data': {**base_data, 'Last Sale Date': '2010-01-01', 'Last Sale Amount': '$100,000'},
                'classification': PropertyClassification(is_owner_occupied=True),
                'expected_priority': 2,
                'expected_code': 'OWN1',
                'description': 'Owner occupied with old sale date'
            },
            
            # OON1: Owner occupied + low amount = Priority 3
            {
                'data': {**base_data, 'Last Sale Date': '2020-01-01', 'Last Sale Amount': '$25,000'},
                'classification': PropertyClassification(is_owner_occupied=True),
                'expected_priority': 3,
                'expected_code': 'OON1', 
                'description': 'Owner occupied with low sale amount'
            },
            
            # OWN20: Very old owner occupied = Priority 13
            {
                'data': {**base_data, 'Last Sale Date': '1990-01-01', 'Last Sale Amount': '$100,000'},
                'classification': PropertyClassification(is_owner_occupied=True),
                'expected_priority': 13,
                'expected_code': 'OWN20',
                'description': 'Very old owner occupied property'
            }
        ]
        
        for case in test_cases:
            property_data = pd.Series(case['data'])
            priority = self.processor.scorer.score_property(property_data, case['classification'])
            
            assert priority.priority_id == case['expected_priority'], \
                f"Failed: {case['description']} - expected {case['expected_priority']}, got {priority.priority_id}"
            assert priority.priority_code == case['expected_code'], \
                f"Failed: {case['description']} - expected {case['expected_code']}, got {priority.priority_code}"
    
    def test_absentee_priority_hierarchy(self):
        """Test absentee (non-owner occupied) property priority scoring"""
        base_data = {
            'Address': '123 Main St',
            'Mailing Address': '456 Oak Ave'  # Different = absentee
        }
        
        test_cases = [
            # INH1: Absentee + grantor match = Priority 6
            {
                'data': {**base_data, 'Last Sale Date': '2020-01-01', 'Last Sale Amount': '$100,000'},
                'classification': PropertyClassification(is_owner_occupied=False, owner_grantor_match=True),
                'expected_priority': 6,
                'expected_code': 'INH1',
                'description': 'Absentee with grantor match'
            },
            
            # ABS1: Absentee + old sale = Priority 7  
            {
                'data': {**base_data, 'Last Sale Date': '2010-01-01', 'Last Sale Amount': '$100,000'},
                'classification': PropertyClassification(is_owner_occupied=False),
                'expected_priority': 7,
                'expected_code': 'ABS1',
                'description': 'Absentee with old sale date'
            },
            
            # TRS1: Absentee + low amount = Priority 8
            {
                'data': {**base_data, 'Last Sale Date': '2020-01-01', 'Last Sale Amount': '$25,000'},
                'classification': PropertyClassification(is_owner_occupied=False),
                'expected_priority': 8,
                'expected_code': 'TRS1',
                'description': 'Absentee with low sale amount'
            },
            
            # BUY1: Recent cash buyer = Priority 9
            {
                'data': {**base_data, 'Last Sale Date': '2024-10-01', 'Last Sale Amount': '$200,000'},
                'classification': PropertyClassification(is_owner_occupied=False),
                'expected_priority': 9,
                'expected_code': 'BUY1',
                'description': 'Recent absentee buyer'
            }
        ]
        
        for case in test_cases:
            property_data = pd.Series(case['data'])
            priority = self.processor.scorer.score_property(property_data, case['classification'])
            
            assert priority.priority_id == case['expected_priority'], \
                f"Failed: {case['description']} - expected {case['expected_priority']}, got {priority.priority_id}"
            assert priority.priority_code == case['expected_code'], \
                f"Failed: {case['description']} - expected {case['expected_code']}, got {priority.priority_code}"
    
    def test_date_based_classification_abs1(self):
        """Test ABS1 classification based on date thresholds"""
        # Properties sold before 2017-09-03 should be ABS1 if absentee
        
        test_dates = [
            ('2017-09-02', True, 'Day before cutoff - should be ABS1'),
            ('2017-09-03', True, 'Exact cutoff date - should be ABS1 (inclusive)'),
            ('2017-09-04', False, 'Day after cutoff - should NOT be ABS1'),
            ('2010-01-01', True, 'Much older - should be ABS1'),
            ('2020-01-01', False, 'Recent - should NOT be ABS1')
        ]
        
        for date_str, should_be_abs1, description in test_dates:
            property_data = pd.Series({
                'Last Sale Date': date_str,
                'Last Sale Amount': '$100,000',
                'Address': '123 Main St',
                'Mailing Address': '456 Oak Ave'  # Absentee
            })
            
            classification = PropertyClassification(is_owner_occupied=False)
            priority = self.processor.scorer.score_property(property_data, classification)
            
            if should_be_abs1:
                assert priority.priority_id == 7, f"Failed: {description} - expected ABS1 (7), got {priority.priority_id}"
                assert priority.priority_code == 'ABS1', f"Failed: {description} - expected ABS1, got {priority.priority_code}"
            else:
                assert priority.priority_id != 7, f"Failed: {description} - should NOT be ABS1, got {priority.priority_id}"
    
    def test_date_based_classification_buy1(self):
        """Test BUY1 classification based on recent buyer logic"""
        # Properties sold after 2024-09-03 should be BUY1 if absentee
        
        test_dates = [
            ('2024-09-04', True, 'Day after cutoff - should be BUY1'),
            ('2024-09-03', True, 'Exact cutoff date - should be BUY1 (inclusive)'),
            ('2024-09-02', False, 'Day before cutoff - should NOT be BUY1'),
            ('2024-12-01', True, 'Much newer - should be BUY1'),
            ('2020-01-01', False, 'Old - should NOT be BUY1')
        ]
        
        for date_str, should_be_buy1, description in test_dates:
            property_data = pd.Series({
                'Last Sale Date': date_str,
                'Last Sale Amount': '$200,000',  # Not low amount
                'Address': '123 Main St',
                'Mailing Address': '456 Oak Ave'  # Absentee
            })
            
            classification = PropertyClassification(is_owner_occupied=False)
            priority = self.processor.scorer.score_property(property_data, classification)
            
            if should_be_buy1:
                assert priority.priority_id == 9, f"Failed: {description} - expected BUY1 (9), got {priority.priority_id}"
                assert priority.priority_code == 'BUY1', f"Failed: {description} - expected BUY1, got {priority.priority_code}"
            else:
                assert priority.priority_id != 9, f"Failed: {description} - should NOT be BUY1, got {priority.priority_id}"
    
    def test_amount_based_classification(self):
        """Test low amount classification (OON1, TRS1)"""
        # Properties with sale amount <= $40,000 should get low amount priority
        
        test_amounts = [
            ('$39,999', True, 'Just under threshold - should be low amount'),
            ('$40,000', True, 'Exact threshold - should be low amount'),
            ('$40,001', False, 'Just over threshold - should NOT be low amount'),
            ('$25,000', True, 'Much lower - should be low amount'),
            ('$100,000', False, 'Higher - should NOT be low amount')
        ]
        
        for amount_str, should_be_low, description in test_amounts:
            # Test owner occupied (should be OON1 = priority 3)
            owner_data = pd.Series({
                'Last Sale Date': '2020-01-01',
                'Last Sale Amount': amount_str,
                'Address': '123 Main St',
                'Mailing Address': '123 Main St'  # Owner occupied
            })
            
            owner_classification = PropertyClassification(is_owner_occupied=True)
            owner_priority = self.processor.scorer.score_property(owner_data, owner_classification)
            
            # Test absentee (should be TRS1 = priority 8)
            absentee_data = pd.Series({
                'Last Sale Date': '2020-01-01',
                'Last Sale Amount': amount_str,
                'Address': '123 Main St',
                'Mailing Address': '456 Oak Ave'  # Absentee
            })
            
            absentee_classification = PropertyClassification(is_owner_occupied=False)
            absentee_priority = self.processor.scorer.score_property(absentee_data, absentee_classification)
            
            if should_be_low:
                assert owner_priority.priority_id == 3, f"Owner failed: {description} - expected OON1 (3), got {owner_priority.priority_id}"
                assert absentee_priority.priority_id == 8, f"Absentee failed: {description} - expected TRS1 (8), got {absentee_priority.priority_id}"
            else:
                assert owner_priority.priority_id != 3, f"Owner failed: {description} - should NOT be OON1"
                assert absentee_priority.priority_id != 8, f"Absentee failed: {description} - should NOT be TRS1"
    
    def test_default_priority_assignment(self):
        """Test that unclassified properties get DEFAULT priority"""
        property_data = pd.Series({
            'Last Sale Date': '2020-01-01',
            'Last Sale Amount': '$100,000',
            'Address': '123 Main St',
            'Mailing Address': '456 Oak Ave'
        })
        
        # Create default classification (nothing special)
        classification = PropertyClassification(is_owner_occupied=False)
        
        priority = self.processor.scorer.score_property(property_data, classification)
        
        assert priority.priority_id == 11
        assert priority.priority_code == "DEFAULT"
        assert "Default" in priority.priority_name
    
    def test_priority_hierarchy_no_overlaps(self):
        """Test that each property gets exactly one priority level"""
        # Test various combinations to ensure no overlapping priorities
        test_properties = [
            {
                'data': pd.Series({
                    'Last Sale Date': '2010-01-01',
                    'Last Sale Amount': '$25,000',
                    'Address': '123 Main St',
                    'Mailing Address': '123 Main St'
                }),
                'classification': PropertyClassification(is_owner_occupied=True, owner_grantor_match=True),
                'description': 'Multiple criteria - should pick highest priority'
            }
        ]
        
        for case in test_properties:
            priority = self.processor.scorer.score_property(case['data'], case['classification'])
            
            # Should get exactly one priority ID
            assert isinstance(priority.priority_id, int)
            assert priority.priority_id in self.processor.scorer.priorities
            assert priority.priority_code is not None
            assert priority.priority_name is not None
    
    def test_real_data_priority_distribution_validation(self):
        """Validate against actual Roanoke County priority distribution"""
        # From real processing: Top priorities were OWN20 (41.5%), DEFAULT (34.9%), OWN1 (12.3%)
        
        # Create test data matching real distribution patterns
        test_properties = []
        
        # OWN20 cases (very old owner occupied)
        for i in range(100):
            test_properties.append({
                'data': pd.Series({
                    'Last Sale Date': '1990-01-01',
                    'Last Sale Amount': '$50,000',
                    'Address': f'{i} Main St',
                    'Mailing Address': f'{i} Main St'
                }),
                'classification': PropertyClassification(is_owner_occupied=True),
                'expected_priority': 13
            })
        
        # DEFAULT cases (absentee, recent, medium amount)
        for i in range(80):
            test_properties.append({
                'data': pd.Series({
                    'Last Sale Date': '2020-01-01',
                    'Last Sale Amount': '$150,000',
                    'Address': f'{i} Oak St',
                    'Mailing Address': f'{i+100} Pine St'
                }),
                'classification': PropertyClassification(is_owner_occupied=False),
                'expected_priority': 11
            })
        
        # Process all test properties
        priority_counts = {}
        for case in test_properties:
            priority = self.processor.scorer.score_property(case['data'], case['classification'])
            priority_counts[priority.priority_id] = priority_counts.get(priority.priority_id, 0) + 1
        
        # Validate distribution makes sense
        total_properties = len(test_properties)
        own20_pct = (priority_counts.get(13, 0) / total_properties) * 100
        default_pct = (priority_counts.get(11, 0) / total_properties) * 100
        
        # Should roughly match real distribution patterns
        assert own20_pct > 30, f"OWN20 percentage too low: {own20_pct:.1f}%"
        assert default_pct > 30, f"DEFAULT percentage too low: {default_pct:.1f}%"
    
    def test_roanoke_county_results_validation(self):
        """Validate against actual Roanoke County processing results"""
        # From real processing output:
        # OWN20: 9,797 (41.5%), DEFAULT: 8,241 (34.9%), OWN1: 2,908 (12.3%)
        # Landlord-ABS1: 1,422 (6.0%), ABS1: 388 (1.6%)
        
        # Test cases representing the major priority types found in real data
        real_world_cases = [
            # OWN20 cases (largest category - 41.5%)
            {
                'data': pd.Series({
                    'Last Sale Date': '1990-01-01',  # Very old
                    'Last Sale Amount': '$45,000',
                    'Address': '123 Main St',
                    'Mailing Address': '123 Main St'  # Owner occupied
                }),
                'classification': PropertyClassification(is_owner_occupied=True),
                'expected_priority': 13,
                'expected_code': 'OWN20'
            },
            
            # DEFAULT cases (second largest - 34.9%)
            {
                'data': pd.Series({
                    'Last Sale Date': '2020-01-01',  # Recent but not BUY1
                    'Last Sale Amount': '$150,000',  # Medium amount
                    'Address': '456 Oak St',
                    'Mailing Address': '789 Pine St'  # Absentee
                }),
                'classification': PropertyClassification(is_owner_occupied=False),
                'expected_priority': 11,
                'expected_code': 'DEFAULT'
            },
            
            # OWN1 cases (third largest - 12.3%)
            {
                'data': pd.Series({
                    'Last Sale Date': '2010-01-01',  # Old but not very old
                    'Last Sale Amount': '$80,000',
                    'Address': '321 Elm St',
                    'Mailing Address': '321 Elm St'  # Owner occupied
                }),
                'classification': PropertyClassification(is_owner_occupied=True),
                'expected_priority': 2,
                'expected_code': 'OWN1'
            },
            
            # ABS1 cases (1.6% in real data)
            {
                'data': pd.Series({
                    'Last Sale Date': '2015-01-01',  # Old sale
                    'Last Sale Amount': '$95,000',
                    'Address': '654 Maple St',
                    'Mailing Address': '987 Oak Ave'  # Absentee
                }),
                'classification': PropertyClassification(is_owner_occupied=False),
                'expected_priority': 7,
                'expected_code': 'ABS1'
            },
            
            # Trust cases (from real data)
            {
                'data': pd.Series({
                    'Last Sale Date': '2018-01-01',
                    'Last Sale Amount': '$120,000',
                    'Address': '111 Trust Ave',
                    'Mailing Address': '222 Trust Ave'
                }),
                'classification': PropertyClassification(is_trust=True),
                'expected_priority': 5,
                'expected_code': 'TRS2'
            }
        ]
        
        # Process all test cases and validate
        for i, case in enumerate(real_world_cases):
            priority = self.processor.scorer.score_property(case['data'], case['classification'])
            
            assert priority.priority_id == case['expected_priority'], \
                f"Case {i+1} failed: expected priority {case['expected_priority']}, got {priority.priority_id}"
            assert priority.priority_code == case['expected_code'], \
                f"Case {i+1} failed: expected code {case['expected_code']}, got {priority.priority_code}"
        
        print(f"[OK] All {len(real_world_cases)} real-world test cases passed!")
    
    def test_priority_scoring_performance(self):
        """Test priority scoring performance with realistic dataset"""
        # Test with size similar to Roanoke County (23,628 records)
        test_size = 5000  # Smaller for CI/testing
        
        import time
        start_time = time.time()
        
        # Generate test data
        test_cases = []
        for i in range(test_size):
            case_type = i % 5
            if case_type == 0:  # OWN20
                data = pd.Series({
                    'Last Sale Date': '1995-01-01',
                    'Last Sale Amount': f'${50000 + (i % 10000)}',
                    'Address': f'{i} Main St',
                    'Mailing Address': f'{i} Main St'
                })
                classification = PropertyClassification(is_owner_occupied=True)
            elif case_type == 1:  # DEFAULT
                data = pd.Series({
                    'Last Sale Date': '2020-01-01',
                    'Last Sale Amount': f'${100000 + (i % 50000)}',
                    'Address': f'{i} Oak St',
                    'Mailing Address': f'{i+1000} Pine St'
                })
                classification = PropertyClassification(is_owner_occupied=False)
            elif case_type == 2:  # Trust
                data = pd.Series({
                    'Last Sale Date': '2018-01-01',
                    'Last Sale Amount': f'${75000 + (i % 25000)}',
                    'Address': f'{i} Trust Ave',
                    'Mailing Address': f'{i} Trust Ave'
                })
                classification = PropertyClassification(is_trust=True)
            elif case_type == 3:  # ABS1
                data = pd.Series({
                    'Last Sale Date': '2012-01-01',
                    'Last Sale Amount': f'${60000 + (i % 15000)}',
                    'Address': f'{i} Elm St',
                    'Mailing Address': f'{i+2000} Maple St'
                })
                classification = PropertyClassification(is_owner_occupied=False)
            else:  # Church
                data = pd.Series({
                    'Last Sale Date': '2019-01-01',
                    'Last Sale Amount': f'${80000 + (i % 20000)}',
                    'Address': f'{i} Church St',
                    'Mailing Address': f'{i} Church St'
                })
                classification = PropertyClassification(is_church=True)
            
            test_cases.append((data, classification))
        
        # Process all cases
        results = []
        for data, classification in test_cases:
            priority = self.processor.scorer.score_property(data, classification)
            results.append(priority)
        
        end_time = time.time()
        processing_time = end_time - start_time
        records_per_second = test_size / processing_time
        
        # Performance assertions
        assert processing_time < 5.0, f"Performance too slow: {processing_time:.2f} seconds for {test_size} records"
        assert records_per_second > 1000, f"Throughput too low: {records_per_second:.0f} records/second"
        
        # Validate results distribution
        priority_counts = {}
        for result in results:
            priority_counts[result.priority_code] = priority_counts.get(result.priority_code, 0) + 1
        
        assert len(priority_counts) >= 4, "Should have multiple priority types"
        assert 'OWN20' in priority_counts, "Should have OWN20 priorities"
        assert 'DEFAULT' in priority_counts, "Should have DEFAULT priorities"
        
        print(f"[OK] Performance test passed: {records_per_second:,.0f} records/second")


if __name__ == "__main__":
    # Run tests directly
    import sys
    pytest.main([__file__] + sys.argv[1:])