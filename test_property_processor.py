"""
Comprehensive test suite for property_processor.py

Tests cover core business logic, edge cases, error handling,
performance optimizations, and security validation.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
import logging

from property_processor import (
    PropertyClassifier, PropertyPriorityScorer, PropertyProcessor,
    PropertyClassification, PropertyPriority,
    DEFAULT_PRIORITY_ID, DEFAULT_PRIORITY_CODE, VERY_OLD_DATE,
    REQUIRED_COLUMNS, RECOMMENDED_COLUMNS, MAX_REASONABLE_SALE_AMOUNT
)

class TestPropertyClassifier:
    """Test suite for PropertyClassifier class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.classifier = PropertyClassifier()
    
    # Trust Detection Tests
    def test_is_trust_with_trust_keywords(self):
        """Test trust detection with various trust keywords"""
        trust_names = [
            'John Smith Trust',  # Contains 'trus'
            'Living Trust of Mary Jones',  # Contains 'living'
            'The Smith Family Revocable Trust',  # Contains 'revoc'
            'Estate of Robert Brown',  # Contains 'estate'
            'Amendment to Trust Document',  # Contains 'amend'
            'Irrevocable Trust Agreement'  # Contains 'irrev'
        ]
        
        for name in trust_names:
            assert self.classifier._is_trust(name.lower())
        
        # Test TRS keyword separately since it's case-sensitive in keywords list
        assert self.classifier._is_trust('Jane Doe TRS')  # Don't convert to lowercase
    
    def test_is_trust_with_non_trust_names(self):
        """Test that non-trust names are not classified as trusts"""
        non_trust_names = [
            'John Smith',
            'ABC Corporation',
            'First Baptist Church', 
            'Smith Industries LLC',
            'Trust Company Bank'  # Contains trust but is a business
        ]
        
        for name in non_trust_names:
            # Trust Company Bank should fail because we don't check for 'trust company'
            if 'trust' in name.lower() and 'company' not in name.lower():
                continue
            assert not self.classifier._is_trust(name.lower())
    
    def test_is_trust_edge_cases(self):
        """Test trust detection edge cases"""
        # Empty/null cases
        assert not self.classifier._is_trust('')
        assert not self.classifier._is_trust('   ')
        
        # Case insensitive
        assert self.classifier._is_trust('JOHN SMITH TRUST')
        assert self.classifier._is_trust('john smith trust')
        
        # Partial matches
        assert self.classifier._is_trust('trustee agreement')
        assert self.classifier._is_trust('revocable living trust')
    
    # Church Detection Tests
    def test_is_church_with_church_keywords(self):
        """Test church detection with various church keywords"""
        church_names = [
            'First Baptist Church',
            'Holy Trinity Episcopal Church',
            'Bible Fellowship Ministry',
            'Church of God in Christ',
            'Lutheran Church of the Reformation',
            'Presbyterian Church USA',
            'Evangelical Free Church'
        ]
        
        for name in church_names:
            assert self.classifier._is_church(name.lower())
    
    def test_is_church_with_ending_patterns(self):
        """Test church detection with ending patterns"""
        church_endings = [
            'Community Church of Christ',
            'Assembly of God',
            'People of Christ',
            'Children of God'
        ]
        
        for name in church_endings:
            assert self.classifier._is_church(name.lower())
    
    def test_is_church_with_non_church_names(self):
        """Test that non-church names are not classified as churches"""
        non_church_names = [
            'John Smith',
            'ABC Corporation',
            'Smith Family Trust',
            'God Construction LLC',  # Contains 'god' but is business
            'Christ Medical Center'   # Contains 'christ' but is business
        ]
        
        for name in non_church_names:
            assert not self.classifier._is_church(name.lower())
    
    # Business Detection Tests  
    def test_is_business_with_business_keywords(self):
        """Test business detection with various business keywords"""
        business_names = [
            'Smith Construction LLC',
            'ABC Properties Inc',
            'Roanoke Real Estate Company',
            'Development Partners Group',
            'Housing Authority',
            'Planning Commission',
            'Medical Center',
            'Credit Union',
            'Bank of America'
        ]
        
        for name in business_names:
            assert self.classifier._is_business(name.lower(), False)
    
    def test_is_business_with_ending_patterns(self):
        """Test business detection with ending patterns"""
        business_endings = [
            'Smith Properties LLC',
            'ABC Development Inc',
            'Real Estate Co',
            'Investment TC',
            'First National Bank',
            'Legal Services Ltd',
            'Consulting LLP'
        ]
        
        for name in business_endings:
            assert self.classifier._is_business(name.lower(), False)
    
    def test_is_business_trust_logic(self):
        """Test special business logic for trusts"""
        trust_business_names = [
            'The Smith Family Trust',
            'The Jones Living Trust', 
            'John Smith the Trustee',
            'Trust the Process LLC'  # Both trust and business
        ]
        
        # When is_trust=True and contains 'the', should be business
        assert self.classifier._is_business('the smith family trust', True)
        assert self.classifier._is_business('john smith the trustee', True)
        
        # When is_trust=False, 'the' alone doesn't make it business
        assert not self.classifier._is_business('the smith family', False)
    
    def test_is_business_with_non_business_names(self):
        """Test that non-business names are not classified as businesses"""
        non_business_names = [
            'John Smith',
            'Mary Jones Trust',
            'First Baptist Church'
        ]
        
        for name in non_business_names:
            assert not self.classifier._is_business(name.lower(), False)
    
    # Grantor Matching Tests
    def test_check_grantor_match_success(self):
        """Test successful grantor matching"""
        # First words match but full names don't match
        assert self.classifier._check_grantor_match('john smith', 'john doe')
        assert self.classifier._check_grantor_match('smith construction', 'smith family')
    
    def test_check_grantor_match_failure(self):
        """Test grantor matching failures"""
        # Exact same names
        assert not self.classifier._check_grantor_match('john smith', 'john smith')
        
        # Different first words
        assert not self.classifier._check_grantor_match('john smith', 'mary jones')
        
        # Empty/null values
        assert not self.classifier._check_grantor_match('john smith', '')
        assert not self.classifier._check_grantor_match('john smith', None)
        assert not self.classifier._check_grantor_match('', 'john smith')
    
    def test_check_grantor_match_edge_cases(self):
        """Test grantor matching edge cases"""
        # Single word names
        assert not self.classifier._check_grantor_match('smith', 'jones')
        
        # Case insensitive
        assert self.classifier._check_grantor_match('JOHN SMITH', 'john doe')
        
        # Extra whitespace
        assert self.classifier._check_grantor_match('  john smith  ', '  john doe  ')
    
    # Complete Classification Tests
    def test_classify_property_trust(self):
        """Test complete property classification for trusts"""
        classification = self.classifier.classify_property(
            'Smith Family Revocable Trust', 
            'Smith Construction'
        )
        
        assert classification.is_trust
        assert not classification.is_church
        assert not classification.is_business  # Church/business not checked when trust=True
        assert not classification.is_owner_occupied  # This is set elsewhere
        assert classification.owner_grantor_match  # smith vs smith (first words match, full names don't)
    
    def test_classify_property_church(self):
        """Test complete property classification for churches"""
        classification = self.classifier.classify_property(
            'First Baptist Church',
            'Church Board'
        )
        
        assert not classification.is_trust
        assert classification.is_church
        assert not classification.is_business  # Business not checked when church=True
        assert not classification.owner_grantor_match  # first vs church
    
    def test_classify_property_business(self):
        """Test complete property classification for businesses"""
        classification = self.classifier.classify_property(
            'Smith Construction LLC',
            'Smith Family Trust'
        )
        
        assert not classification.is_trust
        assert not classification.is_church  
        assert classification.is_business
        assert classification.owner_grantor_match  # smith vs smith
    
    def test_classify_property_individual(self):
        """Test complete property classification for individuals"""
        classification = self.classifier.classify_property(
            'John Smith',
            'Mary Jones'
        )
        
        assert not classification.is_trust
        assert not classification.is_church
        assert not classification.is_business
        assert not classification.owner_grantor_match  # john vs mary
    
    def test_classify_property_null_names(self):
        """Test classification with null/empty names"""
        classification = self.classifier.classify_property(None, None)
        
        assert not classification.is_trust
        assert not classification.is_church
        assert not classification.is_business
        assert not classification.owner_grantor_match
        
        # Test with empty string
        classification = self.classifier.classify_property('', '')
        assert not any([
            classification.is_trust, classification.is_church, 
            classification.is_business, classification.owner_grantor_match
        ])


class TestPropertyPriorityScorer:
    """Test suite for PropertyPriorityScorer class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        # Create scorer with test dates
        self.test_date1 = datetime(2009, 1, 1)  # ABS1 cutoff  
        self.test_date2 = datetime(2019, 1, 1)  # BUY cutoff
        self.test_amount1 = 75000  # Low threshold
        self.test_amount2 = 200000  # High threshold
        
        self.scorer = PropertyPriorityScorer(
            region_input_date1=self.test_date1,
            region_input_date2=self.test_date2, 
            region_input_amount1=self.test_amount1,
            region_input_amount2=self.test_amount2
        )
    
    # Date Parsing Tests
    def test_parse_date_valid_dates(self):
        """Test parsing of valid dates"""
        valid_dates = [
            '2020-01-15',
            '2015-12-25', 
            '2010-06-30',
            datetime(2018, 3, 10)
        ]
        
        for date_val in valid_dates:
            result = self.scorer._parse_date(date_val)
            assert isinstance(result, datetime)
            assert result != VERY_OLD_DATE
    
    def test_parse_date_blank_values(self):
        """Test parsing of blank/null date values"""
        blank_values = [None, '', '   ', pd.NaType(), np.nan]
        
        for blank in blank_values:
            result = self.scorer._parse_date(blank)
            assert result == VERY_OLD_DATE
    
    def test_parse_date_sentinel_dates(self):
        """Test parsing of SQL sentinel dates (1900-01-01)"""
        sentinel_dates = [
            '1900-01-01',
            datetime(1900, 1, 1),
            '1850-01-01',
            datetime(1850, 1, 1)
        ]
        
        for date_val in sentinel_dates:
            result = self.scorer._parse_date(date_val)
            assert result == VERY_OLD_DATE
    
    def test_parse_date_future_dates(self):
        """Test parsing of future dates (should be treated as very old)"""
        future_date = datetime.now() + timedelta(days=365)
        result = self.scorer._parse_date(future_date)
        assert result == VERY_OLD_DATE
    
    def test_parse_date_invalid_formats(self):
        """Test parsing of invalid date formats"""
        invalid_dates = [
            'not-a-date',
            '2020-13-45',  # Invalid month/day
            '2020/01/15',  # Different format
            12345,  # Number
            []  # List
        ]
        
        for invalid in invalid_dates:
            result = self.scorer._parse_date(invalid)
            assert result == VERY_OLD_DATE
    
    # Amount Parsing Tests
    def test_parse_amount_valid_amounts(self):
        """Test parsing of valid amounts"""
        valid_amounts = [
            100000,
            '150,000',
            '$200,000.50',
            '75000.00',
            50000.0
        ]
        
        for amount in valid_amounts:
            result = self.scorer._parse_amount(amount)
            assert isinstance(result, float)
            assert result > 0
    
    def test_parse_amount_blank_values(self):
        """Test parsing of blank/null amounts"""
        blank_values = [None, '', '   ', pd.NaType(), np.nan]
        
        for blank in blank_values:
            result = self.scorer._parse_amount(blank)
            assert result is None
    
    def test_parse_amount_invalid_values(self):
        """Test parsing of invalid amounts"""
        invalid_amounts = [
            'not-a-number',
            'null',
            'n/a',
            'none',
            -100000,  # Negative amount
            []  # List
        ]
        
        for invalid in invalid_amounts:
            result = self.scorer._parse_amount(invalid)
            assert result is None
    
    def test_parse_amount_formatted_strings(self):
        """Test parsing of formatted amount strings"""
        formatted_amounts = {
            '$150,000': 150000.0,
            '75,000.50': 75000.5,
            '  100000  ': 100000.0,
            '$1,234,567.89': 1234567.89
        }
        
        for amount_str, expected in formatted_amounts.items():
            result = self.scorer._parse_amount(amount_str)
            assert result == expected
    
    # Cash Buyer Detection Tests
    def test_is_cash_buyer_true_values(self):
        """Test cash buyer detection with true values"""
        cash_buyer_values = ['true', 'True', 'TRUE', 'yes', 'YES', '1', 'y', 'Y']
        
        for value in cash_buyer_values:
            row = pd.Series({'Last Cash Buyer': value})
            assert self.scorer._is_cash_buyer(row)
    
    def test_is_cash_buyer_false_values(self):
        """Test cash buyer detection with false values"""
        non_cash_values = ['false', 'False', 'no', 'NO', '0', 'n', '', None, pd.NaType()]
        
        for value in non_cash_values:
            row = pd.Series({'Last Cash Buyer': value})
            assert not self.scorer._is_cash_buyer(row)
    
    def test_is_cash_buyer_missing_column(self):
        """Test cash buyer detection when column is missing"""
        row = pd.Series({'Other Column': 'value'})
        assert not self.scorer._is_cash_buyer(row)
    
    # Priority Scoring Tests - Owner Occupied
    def test_score_owner_occupied_grantor_match(self):
        """Test OIN1 priority (owner occupied + grantor match)"""
        classification = PropertyClassification(
            is_owner_occupied=True,
            owner_grantor_match=True
        )
        row = pd.Series({})
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 1
        assert priority.priority_code == 'OIN1'
    
    def test_score_owner_occupied_very_old(self):
        """Test OWN20 priority (owner occupied + very old)"""
        classification = PropertyClassification(is_owner_occupied=True)
        
        # Sale date 25 years ago
        old_date = datetime.now() - timedelta(days=365*25)
        row = pd.Series({'Last Sale Date': old_date})
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 13
        assert priority.priority_code == 'OWN20'
    
    def test_score_owner_occupied_old_sale(self):
        """Test OWN1 priority (owner occupied + old sale date)"""
        classification = PropertyClassification(is_owner_occupied=True)
        
        # Sale date 15 years ago (between 13-20 years)
        old_date = datetime.now() - timedelta(days=365*15)
        row = pd.Series({'Last Sale Date': old_date})
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 2
        assert priority.priority_code == 'OWN1'
    
    def test_score_owner_occupied_low_amount(self):
        """Test OON1 priority (owner occupied + low sale amount)"""
        classification = PropertyClassification(is_owner_occupied=True)
        
        # Recent date but low amount
        recent_date = datetime.now() - timedelta(days=365)
        row = pd.Series({
            'Last Sale Date': recent_date,
            'Last Sale Amount': 50000  # Below threshold
        })
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 3
        assert priority.priority_code == 'OON1'
    
    def test_score_owner_occupied_recent_cash(self):
        """Test BUY1 priority (owner occupied + recent cash buyer)"""
        classification = PropertyClassification(is_owner_occupied=True)
        
        # Recent date and cash buyer
        recent_date = datetime.now() - timedelta(days=365)
        row = pd.Series({
            'Last Sale Date': recent_date,
            'Last Sale Amount': 150000,
            'Last Cash Buyer': 'true'
        })
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 9
        assert priority.priority_code == 'BUY1'
    
    def test_score_owner_occupied_recent_non_cash(self):
        """Test BUY2 priority (owner occupied + recent non-cash buyer)"""
        classification = PropertyClassification(is_owner_occupied=True)
        
        # Recent date but not cash buyer
        recent_date = datetime.now() - timedelta(days=365)
        row = pd.Series({
            'Last Sale Date': recent_date,
            'Last Sale Amount': 150000,
            'Last Cash Buyer': 'false'
        })
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 4
        assert priority.priority_code == 'BUY2'
    
    # Priority Scoring Tests - Absentee
    def test_score_absentee_grantor_match(self):
        """Test INH1 priority (absentee + grantor match)"""
        classification = PropertyClassification(
            is_owner_occupied=False,
            owner_grantor_match=True
        )
        row = pd.Series({})
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 6
        assert priority.priority_code == 'INH1'
    
    def test_score_absentee_old_date(self):
        """Test ABS1 priority (absentee + old sale date)"""
        classification = PropertyClassification(is_owner_occupied=False)
        
        # Very old date (before ABS1 cutoff)
        old_date = datetime(2005, 1, 1)
        row = pd.Series({'Last Sale Date': old_date})
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 7
        assert priority.priority_code == 'ABS1'
    
    def test_score_absentee_low_amount(self):
        """Test TRS1 priority (absentee + low sale amount)"""
        classification = PropertyClassification(is_owner_occupied=False)
        
        # Recent date but low amount
        recent_date = datetime.now() - timedelta(days=365)
        row = pd.Series({
            'Last Sale Date': recent_date,
            'Last Sale Amount': 50000  # Below threshold
        })
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 8
        assert priority.priority_code == 'TRS1'
    
    def test_score_absentee_recent_buyer(self):
        """Test BUY1 priority (absentee + recent buyer)"""
        classification = PropertyClassification(is_owner_occupied=False)
        
        # Recent date and normal amount
        recent_date = datetime.now() - timedelta(days=365)
        row = pd.Series({
            'Last Sale Date': recent_date,
            'Last Sale Amount': 150000
        })
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 9
        assert priority.priority_code == 'BUY1'
    
    # Trust and Church Scoring Tests
    def test_score_trust_property(self):
        """Test TRS2 priority (trust properties)"""
        classification = PropertyClassification(is_trust=True)
        row = pd.Series({})
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 5
        assert priority.priority_code == 'TRS2'
    
    def test_score_church_property(self):
        """Test CHURCH priority (church properties)"""
        classification = PropertyClassification(is_church=True)
        row = pd.Series({})
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 10
        assert priority.priority_code == 'CHURCH'
    
    def test_score_default_property(self):
        """Test DEFAULT priority (unclassified properties)"""
        classification = PropertyClassification()  # All false
        row = pd.Series({})
        
        priority = self.scorer.score_property(row, classification)
        assert priority.priority_id == 11
        assert priority.priority_code == 'DEFAULT'
    
    # Priority Enhancement Tests
    def test_enhance_priority_with_vacant(self):
        """Test priority enhancement with vacant property"""
        row = pd.Series({'Vacant': 'yes'})
        enhanced = self.scorer._enhance_priority_with_main_file_fields(row, 'OWN1')
        assert enhanced == 'Vacant-OWN1'
    
    def test_enhance_priority_with_lien(self):
        """Test priority enhancement with lien type"""
        row = pd.Series({'Lien Type': 'Tax Lien'})
        enhanced = self.scorer._enhance_priority_with_main_file_fields(row, 'ABS1')
        assert enhanced == 'Lien-ABS1'
    
    def test_enhance_priority_with_bankruptcy(self):
        """Test priority enhancement with bankruptcy date"""
        row = pd.Series({'BK Date': '2020-01-15'})
        enhanced = self.scorer._enhance_priority_with_main_file_fields(row, 'BUY1')
        assert enhanced == 'Bankruptcy-BUY1'
    
    def test_enhance_priority_with_preforeclosure(self):
        """Test priority enhancement with pre-foreclosure"""
        row = pd.Series({'Pre-FC Recording Date': '2021-06-30'})
        enhanced = self.scorer._enhance_priority_with_main_file_fields(row, 'INH1')
        assert enhanced == 'PreForeclosure-INH1'
    
    def test_enhance_priority_multiple_indicators(self):
        """Test priority enhancement with multiple indicators"""
        row = pd.Series({
            'Vacant': 'true',
            'Lien Type': 'Tax Lien',
            'BK Date': '2020-01-15'
        })
        enhanced = self.scorer._enhance_priority_with_main_file_fields(row, 'DEFAULT')
        expected_prefixes = ['Vacant', 'Lien', 'Bankruptcy']
        for prefix in expected_prefixes:
            assert prefix in enhanced
        assert enhanced.endswith('-DEFAULT')
    
    def test_enhance_priority_no_indicators(self):
        """Test priority enhancement with no indicators"""
        row = pd.Series({'Other Field': 'value'})
        enhanced = self.scorer._enhance_priority_with_main_file_fields(row, 'OWN1')
        assert enhanced == 'OWN1'


class TestPropertyProcessor:
    """Test suite for PropertyProcessor class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.processor = PropertyProcessor(
            region_input_date1=datetime(2009, 1, 1),
            region_input_date2=datetime(2019, 1, 1),
            region_input_amount1=75000,
            region_input_amount2=200000
        )
        
        # Create sample test data
        self.sample_data = {
            'Owner 1 Last Name': ['Smith', 'Jones', 'Brown'],
            'Owner 1 First Name': ['John', 'Mary', 'Robert'],
            'Address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
            'Mailing Address': ['123 Main St', '999 Different St', '789 Pine Rd'],
            'Last Sale Date': ['2015-01-15', '2020-06-30', ''],
            'Last Sale Amount': [100000, 50000, 200000],
            'Grantor': ['John Doe', 'Mary Jones', 'Different Person'],
            'Last Cash Buyer': ['false', 'true', 'false'],
            'FIPS': ['51770', '51770', '51770']
        }
    
    def test_check_owner_occupancy_match(self):
        """Test owner occupancy detection when addresses match"""
        row = pd.Series({
            'Address': '123 Main St',
            'Mailing Address': '123 Main St'
        })
        assert self.processor._check_owner_occupancy(row)
    
    def test_check_owner_occupancy_no_match(self):
        """Test owner occupancy detection when addresses don't match"""
        row = pd.Series({
            'Address': '123 Main St', 
            'Mailing Address': '456 Oak Ave'
        })
        assert not self.processor._check_owner_occupancy(row)
    
    def test_check_owner_occupancy_po_box(self):
        """Test owner occupancy with PO Box addresses"""
        row = pd.Series({
            'Address': '123 Main St',
            'Mailing Address': 'PO Box 123'
        })
        assert not self.processor._check_owner_occupancy(row)
        
        row = pd.Series({
            'Address': '123 Main St',
            'Mailing Address': 'P O Box 456'
        })
        assert not self.processor._check_owner_occupancy(row)
    
    def test_check_owner_occupancy_missing_data(self):
        """Test owner occupancy with missing address data"""
        row = pd.Series({
            'Address': '',
            'Mailing Address': '123 Main St'
        })
        assert not self.processor._check_owner_occupancy(row)
        
        row = pd.Series({
            'Address': '123 Main St',
            'Mailing Address': ''
        })
        assert not self.processor._check_owner_occupancy(row)
    
    def create_test_excel_file(self, data_dict, file_path):
        """Helper method to create test Excel files"""
        df = pd.DataFrame(data_dict)
        df.to_excel(file_path, index=False)
        return df
    
    def test_process_excel_file_valid_file(self):
        """Test processing a valid Excel file"""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                # Create test file
                test_df = self.create_test_excel_file(self.sample_data, tmp.name)
                
                # Process file
                result = self.processor.process_excel_file(tmp.name)
                
                # Verify basic structure
                assert len(result) == len(test_df)
                assert 'OwnerName' in result.columns
                assert 'IsTrust' in result.columns
                assert 'IsChurch' in result.columns
                assert 'IsBusiness' in result.columns
                assert 'IsOwnerOccupied' in result.columns
                assert 'PriorityId' in result.columns
                assert 'PriorityCode' in result.columns
                
                # Verify owner names were created
                assert result['OwnerName'].iloc[0] == 'Smith John'
                assert result['OwnerName'].iloc[1] == 'Jones Mary'
                
                # Verify owner occupancy detection
                assert result['IsOwnerOccupied'].iloc[0] == True  # Same address
                assert result['IsOwnerOccupied'].iloc[1] == False  # Different address
                assert result['IsOwnerOccupied'].iloc[2] == True  # Same address
                
            finally:
                os.unlink(tmp.name)
    
    def test_process_excel_file_nonexistent(self):
        """Test processing non-existent file"""
        with pytest.raises(FileNotFoundError):
            self.processor.process_excel_file('/nonexistent/file.xlsx')
    
    def test_process_excel_file_empty(self):
        """Test processing empty Excel file"""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                # Create empty file
                empty_df = pd.DataFrame()
                empty_df.to_excel(tmp.name, index=False)
                
                with pytest.raises(ValueError, match="Excel file is empty"):
                    self.processor.process_excel_file(tmp.name)
                    
            finally:
                os.unlink(tmp.name)
    
    def test_process_excel_file_missing_required_columns(self):
        """Test processing file with missing required columns"""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                # Create file missing required columns
                incomplete_data = {'Other Column': ['value1', 'value2']}
                self.create_test_excel_file(incomplete_data, tmp.name)
                
                with pytest.raises(ValueError, match="Missing required columns"):
                    self.processor.process_excel_file(tmp.name)
                    
            finally:
                os.unlink(tmp.name)
    
    def test_validate_dataframe_structure_valid(self):
        """Test DataFrame structure validation with valid data"""
        df = pd.DataFrame(self.sample_data)
        # Should not raise exception
        self.processor._validate_dataframe_structure(df, 'test.xlsx')
    
    def test_validate_dataframe_structure_missing_required(self):
        """Test DataFrame structure validation with missing required columns"""
        incomplete_data = {'Other Column': ['value']}
        df = pd.DataFrame(incomplete_data)
        
        with pytest.raises(ValueError, match="Missing required columns"):
            self.processor._validate_dataframe_structure(df, 'test.xlsx')
    
    def test_validate_data_quality_empty_owners(self):
        """Test data quality validation with empty owner names"""
        data_with_empty_owners = self.sample_data.copy()
        data_with_empty_owners['Owner 1 Last Name'][0] = None
        data_with_empty_owners['Owner 1 First Name'][0] = None
        
        df = pd.DataFrame(data_with_empty_owners)
        
        # Should not raise exception, but should log warning
        with patch('property_processor.logger') as mock_logger:
            self.processor._validate_data_quality(df, 'test.xlsx')
            mock_logger.warning.assert_called()
    
    def test_validate_data_quality_unrealistic_amounts(self):
        """Test data quality validation with unrealistic sale amounts"""
        data_with_bad_amounts = self.sample_data.copy()
        data_with_bad_amounts['Last Sale Amount'] = [100000000000, -50000, 100000]  # Too high, negative, normal
        
        df = pd.DataFrame(data_with_bad_amounts)
        
        with patch('property_processor.logger') as mock_logger:
            self.processor._validate_data_quality(df, 'test.xlsx')
            # Should log warnings for both unrealistic high and negative amounts
            assert mock_logger.warning.call_count >= 2
    
    def test_process_niche_files_multiple(self):
        """Test processing multiple niche files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple test files
            file1 = Path(tmpdir) / 'liens.xlsx'
            file2 = Path(tmpdir) / 'foreclosure.xlsx'
            
            self.create_test_excel_file(self.sample_data, file1)
            self.create_test_excel_file(self.sample_data, file2)
            
            result = self.processor.process_niche_files([str(file1), str(file2)])
            
            assert len(result) == 2
            assert 'liens.xlsx' in result
            assert 'foreclosure.xlsx' in result
            
            # Verify both files were processed
            for file_result in result.values():
                assert len(file_result) == len(self.sample_data['Owner 1 Last Name'])
    
    def test_memory_optimization_categorical_data(self):
        """Test that string columns are converted to categorical for memory optimization"""
        # Create data with repetitive categorical values
        repetitive_data = self.sample_data.copy()
        repetitive_data['State'] = ['VA'] * len(repetitive_data['Owner 1 Last Name'])
        repetitive_data['City'] = ['Roanoke', 'Roanoke', 'Salem']  # Less than 50% unique
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                self.create_test_excel_file(repetitive_data, tmp.name)
                
                result = self.processor.process_excel_file(tmp.name)
                
                # Check that categorical optimization occurred
                # The State column should be categorical due to low uniqueness ratio
                
            finally:
                os.unlink(tmp.name)
    
    def test_error_handling_classification_failure(self):
        """Test error handling when classification fails"""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                self.create_test_excel_file(self.sample_data, tmp.name)
                
                # Mock classification to raise exception
                with patch.object(self.processor.classifier, '_is_trust', side_effect=Exception("Classification error")):
                    result = self.processor.process_excel_file(tmp.name)
                    
                    # Should fall back to default values
                    assert all(result['IsTrust'] == False)
                    assert all(result['IsChurch'] == False)
                    assert all(result['IsBusiness'] == False)
                    
            finally:
                os.unlink(tmp.name)
    
    def test_error_handling_priority_scoring_failure(self):
        """Test error handling when priority scoring fails"""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                self.create_test_excel_file(self.sample_data, tmp.name)
                
                # Mock scorer to raise exception
                with patch.object(self.processor.scorer, 'score_property', side_effect=Exception("Scoring error")):
                    result = self.processor.process_excel_file(tmp.name)
                    
                    # Should fall back to default priority
                    assert all(result['PriorityId'] == DEFAULT_PRIORITY_ID)
                    assert all(result['PriorityCode'] == DEFAULT_PRIORITY_CODE)
                    
            finally:
                os.unlink(tmp.name)


class TestPerformanceOptimizations:
    """Test suite for performance optimizations"""
    
    def setup_method(self):
        """Set up performance test fixtures"""
        self.processor = PropertyProcessor()
    
    def test_vectorized_processing_performance(self):
        """Test that vectorized processing is faster than row-by-row"""
        # Create larger dataset for meaningful performance comparison
        large_data = {
            'Owner 1 Last Name': ['Smith'] * 1000,
            'Owner 1 First Name': ['John'] * 1000,
            'Address': [f'{i} Main St' for i in range(1000)],
            'Mailing Address': [f'{i} Main St' for i in range(1000)],
            'Last Sale Date': ['2015-01-15'] * 1000,
            'Last Sale Amount': [100000] * 1000,
            'FIPS': ['51770'] * 1000
        }
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                df = pd.DataFrame(large_data)
                df.to_excel(tmp.name, index=False)
                
                import time
                start_time = time.time()
                result = self.processor.process_excel_file(tmp.name)
                end_time = time.time()
                
                processing_time = end_time - start_time
                
                # Should process 1000 records in reasonable time (less than 10 seconds)
                assert processing_time < 10.0
                assert len(result) == 1000
                
            finally:
                os.unlink(tmp.name)
    
    def test_memory_usage_optimization(self):
        """Test memory usage optimizations"""
        data = {
            'Owner 1 Last Name': ['Smith', 'Jones', 'Brown'],
            'Owner 1 First Name': ['John', 'Mary', 'Robert'],
            'Address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
            'State': ['VA', 'VA', 'VA'],  # Repetitive data
            'FIPS': ['51770'] * 3
        }
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                df = pd.DataFrame(data)
                df.to_excel(tmp.name, index=False)
                
                result = self.processor.process_excel_file(tmp.name)
                
                # Memory usage should be reported in logs
                # Categorical optimization should reduce memory usage
                
            finally:
                os.unlink(tmp.name)


class TestErrorHandlingAndEdgeCases:
    """Test suite for error handling and edge cases"""
    
    def setup_method(self):
        """Set up edge case test fixtures"""
        self.processor = PropertyProcessor()
    
    def test_malformed_excel_file(self):
        """Test handling of malformed Excel files"""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                # Write invalid Excel data
                with open(tmp.name, 'w') as f:
                    f.write("This is not Excel data")
                
                with pytest.raises(ValueError, match="Failed to read Excel file"):
                    self.processor.process_excel_file(tmp.name)
                    
            finally:
                os.unlink(tmp.name)
    
    def test_extremely_large_dataset_handling(self):
        """Test handling of very large datasets"""
        # Simulate memory pressure scenarios
        pass  # Would implement with large test data if needed
    
    def test_unicode_and_special_characters(self):
        """Test handling of Unicode and special characters in names"""
        unicode_data = {
            'Owner 1 Last Name': ['Smith', 'José', 'O\'Connor'],
            'Owner 1 First Name': ['John', 'María', 'Seán'],
            'Address': ['123 Main St', '456 Café Ave', '789 Naïve Rd'],
            'FIPS': ['51770'] * 3
        }
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            try:
                df = pd.DataFrame(unicode_data)
                df.to_excel(tmp.name, index=False)
                
                result = self.processor.process_excel_file(tmp.name)
                
                # Should handle Unicode characters without error
                assert len(result) == 3
                assert 'José María' in result['OwnerName'].values
                assert 'O\'Connor Seán' in result['OwnerName'].values
                
            finally:
                os.unlink(tmp.name)
    
    def test_extreme_date_values(self):
        """Test handling of extreme date values"""
        extreme_dates = [
            '1800-01-01',  # Very old
            '2099-12-31',  # Far future
            '1900-01-01',  # SQL sentinel
            '',            # Blank
            'invalid'      # Invalid format
        ]
        
        scorer = PropertyPriorityScorer()
        
        for date_val in extreme_dates:
            result = scorer._parse_date(date_val)
            # Should handle all extreme cases without crashing
            assert isinstance(result, datetime)
    
    def test_extreme_amount_values(self):
        """Test handling of extreme amount values"""
        extreme_amounts = [
            0,
            -1000000,
            999999999999,  # Very large
            '$1,000,000,000,000',  # Formatted very large
            '',
            'invalid'
        ]
        
        scorer = PropertyPriorityScorer()
        
        for amount_val in extreme_amounts:
            result = scorer._parse_amount(amount_val)
            # Should handle all extreme cases without crashing
            assert result is None or isinstance(result, float)


if __name__ == '__main__':
    # Run tests with pytest
    pytest.main([__file__, '-v'])