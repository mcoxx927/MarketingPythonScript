"""
Test script for the updated date parsing logic.
Tests that blank dates and 1900-01-01 dates are treated as "very old" for priority scoring.
"""

import pandas as pd
from datetime import datetime, timedelta
from property_processor import PropertyPriorityScorer

def test_date_parsing():
    """Test the date parsing logic with various inputs"""
    print("Testing Date Parsing Logic")
    print("=" * 40)
    
    # Create test region config (matches monthly_processing.py)
    scorer = PropertyPriorityScorer(
        region_input_date1=datetime(2009, 1, 1),  # ABS1 cutoff
        region_input_date2=datetime(2019, 1, 1),  # BUY1/BUY2 cutoff  
        region_input_amount1=75000,
        region_input_amount2=200000
    )
    
    # Test various date inputs
    test_dates = [
        None,                    # Blank/null
        '',                     # Empty string
        '1900-01-01',           # SQL sentinel date
        '1900-01-02',           # SQL sentinel date (user mentioned this)
        '2008-06-15',           # Before region_input_date1 (should be ABS1)
        '2015-03-20',           # Between date1 and date2
        '2020-08-10',           # After region_input_date2 (recent)
        '2025-12-01',           # Future date (invalid)
        'invalid_date',         # Invalid string
    ]
    
    print("Date Input -> Parsed Date -> Qualifies for ABS1 (before 2009-01-01)?")
    print("-" * 70)
    
    for test_date in test_dates:
        try:
            parsed = scorer._parse_date(test_date)
            qualifies_abs1 = parsed <= datetime(2009, 1, 1)
            print(f"{str(test_date):15} -> {parsed.strftime('%Y-%m-%d'):10} -> {qualifies_abs1}")
        except Exception as e:
            print(f"{str(test_date):15} -> ERROR: {e}")
    
    print("\nExpected behavior:")
    print("- Blank/null dates should parse to 1850-01-01 and qualify for ABS1")
    print("- 1900-01-01/1900-01-02 should parse to 1850-01-01 and qualify for ABS1") 
    print("- Dates before 2009-01-01 should qualify for ABS1")
    print("- Future/invalid dates should parse to 1850-01-01 and qualify for ABS1")

def test_priority_scoring():
    """Test that blank dates get high priority scores"""
    print("\n\nTesting Priority Scoring Logic with Direct Method Calls")
    print("=" * 55)
    
    # Create scorer and classifier to test directly
    from property_processor import PropertyPriorityScorer, PropertyClassifier
    
    scorer = PropertyPriorityScorer(
        region_input_date1=datetime(2009, 1, 1),
        region_input_date2=datetime(2019, 1, 1), 
        region_input_amount1=75000,
        region_input_amount2=200000
    )
    
    classifier = PropertyClassifier()
    
    # Test cases with different date scenarios
    test_cases = [
        {
            'name': 'Blank Date Absentee',
            'data': {
                'OwnerName': 'Test Owner 1',
                'Property Address': '123 Main St',
                'Owner Address': '456 Oak Ave',  # Different = absentee
                'Last Sale Date': None,          # Blank date
                'Last Sale Amount': 100000,
                'Grantor Name': 'Different Person'
            },
            'expected': 'ABS1 (Priority 7) - blank date should be very old'
        },
        {
            'name': '1900-01-01 Date Owner Occupied',
            'data': {
                'OwnerName': 'Test Owner 2',
                'Property Address': '789 Pine St', 
                'Owner Address': '789 Pine St',  # Same = owner occupied
                'Last Sale Date': '1900-01-01',  # Sentinel date
                'Last Sale Amount': 150000,
                'Grantor Name': 'Test Owner 2'
            },
            'expected': 'OWN20 (Priority 13) - 1900 date should be very old'
        },
        {
            'name': '2008 Date Absentee',
            'data': {
                'OwnerName': 'Test Owner 3',
                'Property Address': '321 Elm Ave',
                'Owner Address': '999 Far St',   # Different = absentee
                'Last Sale Date': '2008-05-01',  # Before 2009 cutoff
                'Last Sale Amount': 120000,
                'Grantor Name': 'Other Person'
            },
            'expected': 'ABS1 (Priority 7) - 2008 is before 2009 cutoff'
        }
    ]
    
    print("Test Results:")
    print("-" * 80)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest Case {i}: {test_case['name']}")
        print(f"Expected: {test_case['expected']}")
        
        try:
            # Create a pandas Series from the test data
            row = pd.Series(test_case['data'])
            
            # Classify the property
            classification = classifier.classify_property(row)
            
            # Score the priority
            priority = scorer.score_property(row, classification)
            
            # Parse the date to see what it becomes
            parsed_date = scorer._parse_date(row.get('Last Sale Date'))
            
            print(f"Results:")
            print(f"  Original Date: {row.get('Last Sale Date')}")
            print(f"  Parsed Date: {parsed_date.strftime('%Y-%m-%d')}")
            print(f"  Owner Occupied: {classification.is_owner_occupied}")
            print(f"  Priority: {priority.priority_code} (ID: {priority.priority_id}) - {priority.priority_name}")
            
            # Check if it meets expectations
            if (test_case['name'] == 'Blank Date Absentee' and priority.priority_code == 'ABS1') or \
               (test_case['name'] == '1900-01-01 Date Owner Occupied' and priority.priority_code == 'OWN20') or \
               (test_case['name'] == '2008 Date Absentee' and priority.priority_code == 'ABS1'):
                print(f"  Result: ✓ PASS")
            else:
                print(f"  Result: ✗ UNEXPECTED - check logic")
                
        except Exception as e:
            print(f"  Error: {e}")
    
    print("\n" + "=" * 55)
    print("Summary: Blank dates and 1900-01-01 dates should now be treated as")
    print("'very old' dates that qualify for high-priority processing (ABS1, OWN20, etc.)")

if __name__ == "__main__":
    test_date_parsing()
    test_priority_scoring()