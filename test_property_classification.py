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
        """Test trust detection with priority-based classification"""
        trust_cases = [
            # Strong trust indicators (highest priority)
            ("SMITH FAMILY TRUST", True, "Strong trust phrase"),
            ("LIVING TRUST OF JONES", True, "Strong trust phrase"),
            ("THE BROWN REVOCABLE TRUST", True, "Strong trust phrase"),
            ("ESTATE OF WILLIAMS", True, "Strong trust phrase"),
            ("TESTAMENTARY TRUST OF DOE", True, "Strong trust phrase"),
            
            # Mixed case and spacing
            ("smith family Trust", True, "Mixed case should work"),
            ("LIVING  TRUST  OF  JONES", True, "Multiple spaces should work"),
            
            # Personal names with trust-like surnames (blocked by personal name detection)
            ("TRUSSELL JANET", False, "Personal name with 'trus' surname"),
            ("TRUSSLER MYRNA", False, "Personal name with 'trus' surname"),
            ("PETRUS JOAN", False, "Personal name with 'trus' substring"),
            
            # Address contexts (blocked by address detection)
            ("LIVING ON MAIN STREET", False, "Address context - not entity"),
            ("LIVING STREET", False, "Street address - not entity"),
            
            # Business entities take priority over weak trust matches
            ("TRUSTWORTHY CONSTRUCTION LLC", False, "LLC overrides partial 'trus' match"),
            
            # Edge cases
            ("ESTABLISH NEW LLC", False, "No trust indicators, LLC makes it business"),
            ("SMITH FAMILY", False, "No trust keywords"),
            
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
            
            # Personal names with church-like surnames (should NOT match due to personal name detection)
            ("UPCHURCH DAVID", False, "Personal name with church surname"),
            ("CHURCH BARBARA", False, "Personal name with church surname"),
            ("CHURCH RANDY", False, "Personal name with church surname"),
            
            # Edge cases
            ("CHRISTIAN MILLER", False, "Person name with Christian - detected as personal"),
            
            # Empty/null cases
            ("", False, "Empty string"),
            (None, False, "None value"),
        ]
        
        for owner_name, expected, description in church_cases:
            classification = self.classifier.classify_property(owner_name)
            assert classification.is_church == expected, f"Failed: {description} - '{owner_name}'"
    
    def test_business_entity_detection(self):
        """Test business entity detection with priority-based classification"""
        business_cases = [
            # Strong business indicators (highest priority)
            ("ACME CORPORATION", True, "Strong business suffix"),
            ("SMITH PROPERTIES LLC", True, "Strong business suffix"),
            ("ROANOKE HOLDINGS INC", True, "Strong business suffix"),
            ("MEDICAL CENTER", True, "Strong business phrase"),
            ("VIRGINIA HOUSING AUTHORITY", True, "Strong business phrase"),
            ("CITY OF ROANOKE", True, "Strong business phrase"),
            ("COMMONWEALTH OF VIRGINIA", True, "Strong business phrase"),
            ("CREDIT UNION FEDERAL", True, "Strong business phrase"),
            
            # Business overrides partial matches from other categories
            ("TRUSTWORTHY CONSTRUCTION LLC", True, "LLC overrides partial 'trus' match"),
            ("ESTABLISH LLC", True, "LLC suffix makes it business"),
            ("HOLY CONSTRUCTION COMPANY", True, "Company suffix overrides 'holy' church match"),
            
            # Weak business indicators (only match if no address context)
            ("ROANOKE PROPERTIES", True, "Weak business keywords, no address context"),
            ("REAL ESTATE PARTNERS", True, "Business descriptive phrases"),
            
            # Personal names (blocked by personal name detection)
            ("JOHN COMPANY", False, "Personal name detected"),
            ("MARY REAL", False, "Personal name detected"),
            
            # Address contexts (blocked by address detection)
            ("COMPANY STREET", False, "Street address - not entity"),
            ("REAL ESTATE DRIVE", False, "Address context blocks classification"),
            
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
    
    def test_priority_classification_system(self):
        """Test the new priority-based classification system"""
        priority_cases = [
            # Priority 1: Strong business indicators win over everything
            ("TRUSTWORTHY CONSTRUCTION LLC", False, False, True, "Business suffix overrides trust keyword"),
            ("HOLY MEDICAL CENTER", False, False, True, "Medical center overrides holy church keyword"),
            ("CHURCH PROPERTIES INC", False, False, True, "INC overrides church keyword"),
            
            # Priority 2: Strong trust indicators  
            ("SMITH FAMILY TRUST", True, False, False, "Strong trust phrase"),
            ("LIVING TRUST OF JONES", True, False, False, "Strong trust phrase"),
            
            # Priority 3: Strong church indicators
            ("FIRST BAPTIST CHURCH", False, True, False, "Strong church phrase"),
            ("CHURCH OF GOD", False, True, False, "Strong church phrase"),
            
            # Priority 4: Weak matches (only if no personal name conflict)
            ("VIRGINIA PROPERTIES", False, False, True, "Weak business match only"),
            ("ROANOKE DEVELOPMENT", False, False, True, "Weak business match only"),
            
            # Context-aware blocking
            ("LIVING STREET", False, False, False, "Address context blocks all classification"),
            ("CHURCH AVENUE", False, False, False, "Address context blocks all classification"),
            ("TRUSTWORTHY FRIEND", False, False, False, "Personal name blocks classification"),
        ]
        
        for name, exp_trust, exp_church, exp_business, description in priority_cases:
            classification = self.classifier.classify_property(name)
            
            assert classification.is_trust == exp_trust, f"Trust failed for {name}: {description}"
            assert classification.is_church == exp_church, f"Church failed for {name}: {description}"
            assert classification.is_business == exp_business, f"Business failed for {name}: {description}"
            
            # Ensure only one classification type (or none)
            total_classifications = sum([classification.is_trust, classification.is_church, classification.is_business])
            assert total_classifications <= 1, f"Multiple classifications for {name}: T:{classification.is_trust} C:{classification.is_church} B:{classification.is_business}"
    
    def test_personal_name_detection(self):
        """Test the personal name detection feature that prevents over-matching"""
        personal_name_cases = [
            # Should be detected as personal names (2 words, no strong business indicators)
            ("JOHN SMITH", True, "Simple personal name"),
            ("MARY JOHNSON", True, "Simple personal name"),
            ("CHURCH BARBARA", True, "Surname happens to be Church"),
            ("TRUSSELL JANET", True, "Surname contains 'trus'"),
            ("UPCHURCH DAVID", True, "Surname contains 'church'"),
            ("REAL ESTATE", True, "Two word phrase"),
            
            # Should NOT be detected as personal names (strong business/entity indicators)
            ("SMITH FAMILY TRUST", False, "Contains entity phrase"),
            ("FIRST BAPTIST CHURCH", False, "Contains entity phrase"),
            ("ROANOKE PROPERTIES LLC", False, "Contains strong business indicator"),
            ("MEDICAL CENTER AUTHORITY", False, "More than 2 words"),
            ("HOUSING AUTHORITY", False, "Strong business indicator"),
            
            # Edge cases
            ("JOHN", False, "Only one word"),
            ("JOHN SMITH TRUST", False, "More than 2 words"),
            ("", False, "Empty string"),
        ]
        
        for name, expected, description in personal_name_cases:
            is_personal = self.classifier._is_likely_personal_name(name.lower())
            assert is_personal == expected, f"Personal name detection failed: {description} - '{name}' (got {is_personal}, expected {expected})"
            
            # Verify that personal names don't get classified as entities
            if expected:  # If it should be detected as personal
                classification = self.classifier.classify_property(name)
                assert not classification.is_trust, f"Personal name incorrectly classified as trust: {name}"
                assert not classification.is_church, f"Personal name incorrectly classified as church: {name}"
                assert not classification.is_business, f"Personal name incorrectly classified as business: {name}"


if __name__ == "__main__":
    # Run tests directly
    import sys
    pytest.main([__file__] + sys.argv[1:])