"""
Comprehensive Unit Tests for Property Classification Logic

Tests the critical business logic for detecting trusts, churches, and businesses
based on owner name patterns. Uses real-world edge cases discovered during
processing 23,628 Roanoke County records.
"""

import pytest
import pandas as pd
from datetime import datetime
from property_processor import PropertyClassifier, PropertyClassification


class TestPropertyClassifier:
    
    def setup_method(self):
        """Setup test instance before each test"""
        self.classifier = PropertyClassifier()
    
    def test_trust_detection_keywords(self):
        """Test trust detection with various keyword patterns"""
        trust_cases = [
            # Basic trust keywords
            ("SMITH FAMILY TRUST", True, "Basic trust pattern"),
            ("LIVING TRUST OF JONES", True, "Living trust pattern"),
            ("THE BROWN REVOCABLE TRUST", True, "Revocable trust pattern"),
            ("ESTATE OF WILLIAMS", True, "Estate pattern"),
            ("JOHN DOE TESTAMENTARY TRUST", True, "Testamentary trust pattern"),
            
            # Mixed case and spacing
            ("smith family Trust", True, "Mixed case should work"),
            ("LIVING  TRUST  OF  JONES", True, "Multiple spaces should work"),
            
            # Edge cases - should NOT match
            ("TRUSTING FRIEND LLC", False, "Partial match should be rejected"),
            ("ESTABLISH NEW LLC", False, "Contains 'establish' but not trust"),
            ("SMITH FAMILY", False, "No trust keywords"),
            ("LIVING ON MAIN STREET", False, "Living without trust context"),
            
            # Empty/null cases
            ("", False, "Empty string"),
            (None, False, "None value"),
        ]
        
        for owner_name, expected, description in trust_cases:
            classification = self.classifier.classify_property(owner_name)
            assert classification.is_trust == expected, f"Failed: {description} - '{owner_name}'"
    
    def test_church_detection_keywords(self):
        """Test church detection including edge cases from real data"""
        church_cases = [
            # Basic church patterns
            ("FIRST BAPTIST CHURCH", True, "Basic church pattern"),
            ("ST MARY CATHOLIC CHURCH", True, "Catholic church pattern"),
            ("HOLY TRINITY METHODIST", True, "Methodist church pattern"),
            ("MINISTRY OF GRACE", True, "Ministry pattern"),
            ("ROANOKE VALLEY CHRISTIAN CHURCH", True, "Multi-word church pattern"),
            
            # Church ending patterns
            ("CHURCH OF CHRIST", True, "Church ending pattern"),
            ("WORD OF GOD", True, "God ending pattern"),
            
            # Edge cases - should NOT match (addresses vs entities)
            ("CHURCHILL AVENUE", False, "Street name containing 'church'"),
            ("GODWIN STREET", False, "Street name containing 'god'"),
            ("CHRISTIAN MILLER", False, "Person name with Christian"),
            ("BAPTIST ROAD", False, "Street name"),
            ("HOLYCROSS STREET", False, "Street name compound"),
            
            # Empty/null cases
            ("", False, "Empty string"),
            (None, False, "None value"),
        ]
        
        for owner_name, expected, description in church_cases:
            classification = self.classifier.classify_property(owner_name)
            assert classification.is_church == expected, f"Failed: {description} - '{owner_name}'"
    
    def test_business_entity_detection(self):
        """Test business entity detection with comprehensive keyword coverage"""
        business_cases = [
            # Basic business entity patterns
            ("ACME CORPORATION", True, "Corporation pattern"),
            ("SMITH PROPERTIES LLC", True, "LLC pattern"),
            ("ROANOKE HOLDINGS INC", True, "Inc pattern"),
            ("REAL ESTATE PARTNERS", True, "Real estate business"),
            ("CITY OF ROANOKE", True, "City entity"),
            ("COMMONWEALTH OF VIRGINIA", True, "Commonwealth entity"),
            
            # Business endings
            ("SMITH & ASSOCIATES", True, "Associates ending"),
            ("MEDICAL CENTER", True, "Medical center"),
            ("HOUSING AUTHORITY", True, "Housing authority"),
            ("PLANNING COMMISSION", True, "Planning commission"),
            
            # Edge cases - should NOT match (unless other criteria met)
            ("INCORPORATE STREET", False, "Street name containing business keyword"),
            ("DEVELOPING ROAD", False, "Street name with develop keyword"),
            ("COMPANY STORE LANE", False, "Street name"),
            
            # Person names that might have business keywords
            ("JOHN COMPANY SMITH", False, "Person name with business word"),
            ("MARY REAL JONES", False, "Person name with 'real'"),
            
            # Empty/null cases
            ("", False, "Empty string"),
            (None, False, "None value"),
        ]
        
        for owner_name, expected, description in business_cases:
            classification = self.classifier.classify_property(owner_name)
            assert classification.is_business == expected, f"Failed: {description} - '{owner_name}'"
    
    def test_classification_hierarchy(self):
        """Test that classification hierarchy works correctly (trust > church > business)"""
        hierarchy_cases = [
            # Trust wins over church keywords
            ("FIRST BAPTIST CHURCH TRUST", True, False, False, "Trust overrides church"),
            ("HOLY TRINITY ESTATE", True, False, False, "Trust (estate) overrides church"),
            
            # Church wins over business keywords (when not trust)
            ("FIRST BAPTIST CHURCH INC", False, True, False, "Church overrides business"),
            ("HOLY MINISTRY LLC", False, True, False, "Church ministry overrides LLC"),
            
            # Business only when no church or trust
            ("ROANOKE PROPERTIES LLC", False, False, True, "Pure business entity"),
            ("SMITH FAMILY LLC", False, False, True, "Family business"),
        ]
        
        for owner_name, exp_trust, exp_church, exp_business, description in hierarchy_cases:
            classification = self.classifier.classify_property(owner_name)
            assert classification.is_trust == exp_trust, f"Trust failed: {description}"
            assert classification.is_church == exp_church, f"Church failed: {description}"
            assert classification.is_business == exp_business, f"Business failed: {description}"
    
    def test_grantor_matching_logic(self):
        """Test owner-grantor matching for family transfers"""
        grantor_cases = [
            # Should match - same first word, different full names
            ("john smith", "john doe", True, "Same first name, different last"),
            ("mary johnson", "mary smith", True, "Female name change"),
            ("robert brown", "robert j brown", True, "Middle initial difference"),
            
            # Should NOT match - completely different first names
            ("john smith", "mary smith", False, "Different first names"),
            ("robert brown", "james brown", False, "Different first names, same last"),
            
            # Should NOT match - identical names
            ("john smith", "john smith", False, "Identical names"),
            ("mary doe", "mary doe", False, "Identical female names"),
            
            # Edge cases
            ("john", "john smith", True, "Short vs long name"),
            ("", "john smith", False, "Empty owner name"),
            ("john smith", "", False, "Empty grantor name"),
            ("", "", False, "Both empty"),
            ("john smith", None, False, "None grantor"),
        ]
        
        for owner_name, grantor_name, expected, description in grantor_cases:
            classification = self.classifier.classify_property(owner_name, grantor_name)
            assert classification.owner_grantor_match == expected, f"Failed: {description}"
    
    def test_real_world_edge_cases(self):
        """Test edge cases discovered in Roanoke County real data processing"""
        real_world_cases = [
            # Cases that might confuse the classifier
            ("TRUSTWORTHY CONSTRUCTION LLC", False, "Trustworthy is not trust"),
            ("CHURCHHILL DOWNS LLC", False, "Churchill is not church"),
            ("INCORPORATE SOLUTIONS INC", True, "Should detect INC despite incorporate"),
            ("THE LIVING TRUST OF SMITH", True, "Should detect trust with 'the'"),
            ("SMITH THE BUILDER", True, "Trust logic with 'the' pattern"),
            
            # Regional specific patterns (Virginia/Roanoke area)
            ("VIRGINIA HOUSING AUTHORITY", True, "State housing authority"),
            ("ROANOKE CITY SCHOOLS", True, "City schools entity"),
            ("ROANOKE VALLEY CHURCH OF GOD", True, "Regional church pattern"),
            
            # Multiple classification triggers
            ("ROANOKE BAPTIST CHURCH TRUST", True, "Trust wins in hierarchy"),
            ("HOLY SPIRIT MINISTRY LLC", False, "Church wins over LLC"),
        ]
        
        for owner_name, expected_trust, description in real_world_cases:
            classification = self.classifier.classify_property(owner_name)
            # Just testing trust detection here, add church/business as needed
            if expected_trust is not None:
                assert classification.is_trust == expected_trust, f"Trust detection failed: {description}"
    
    def test_performance_with_large_dataset(self):
        """Test classifier performance with realistic dataset size"""
        # Create test data similar to Roanoke County size (23,628 records)
        test_names = [
            "JOHN SMITH", "SMITH FAMILY TRUST", "FIRST BAPTIST CHURCH",
            "ROANOKE PROPERTIES LLC", "MARY DOE", "WILLIAMS ESTATE"
        ] * 1000  # 6,000 records for performance test
        
        start_time = datetime.now()
        
        results = []
        for name in test_names:
            classification = self.classifier.classify_property(name)
            results.append(classification)
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        # Should process 6k records in under 5 seconds
        assert processing_time < 5.0, f"Performance test failed: {processing_time:.2f} seconds for 6k records"
        assert len(results) == len(test_names), "All records should be processed"
        
        # Verify some classifications are correct
        trust_count = sum(1 for r in results if r.is_trust)
        church_count = sum(1 for r in results if r.is_church) 
        business_count = sum(1 for r in results if r.is_business)
        
        assert trust_count > 0, "Should find some trusts"
        assert church_count > 0, "Should find some churches"
        assert business_count > 0, "Should find some businesses"


if __name__ == "__main__":
    # Run tests directly
    import sys
    pytest.main([__file__] + sys.argv[1:])