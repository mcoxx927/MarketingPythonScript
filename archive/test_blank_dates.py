"""
Create a small test Excel file with blank dates to verify the fix works in production.
"""

import pandas as pd
from pathlib import Path

def create_test_excel():
    """Create a small test Excel file with blank and 1900-01-01 dates"""
    
    test_data = [
        {
            'Owner 1 Last Name': 'Smith',
            'Owner 1 First Name': 'John',
            'Property Address': '123 Main St, Roanoke, VA',
            'Owner Address': '456 Oak Ave, Roanoke, VA',  # Different = absentee
            'Last Sale Date': None,                       # Blank date - should get ABS1
            'Last Sale Amount': 100000,
            'Grantor Name': 'Previous Owner',
            # Add more columns to match expected format
            'Zip': '24012',
            'City': 'Roanoke',
            'State': 'VA'
        },
        {
            'Owner 1 Last Name': 'Johnson',
            'Owner 1 First Name': 'Mary',
            'Property Address': '789 Pine St, Roanoke, VA',
            'Owner Address': '789 Pine St, Roanoke, VA',  # Same = owner occupied  
            'Last Sale Date': '1900-01-01',               # Sentinel date - should get OWN20
            'Last Sale Amount': 150000,
            'Grantor Name': 'Mary Johnson',               # Same name = grantor match
            'Zip': '24012',
            'City': 'Roanoke', 
            'State': 'VA'
        },
        {
            'Owner 1 Last Name': 'Wilson',
            'Owner 1 First Name': 'Bob',
            'Property Address': '321 Elm Ave, Roanoke, VA',
            'Owner Address': '555 Far St, Richmond, VA',  # Different = absentee
            'Last Sale Date': '2008-05-01',               # Before 2009 cutoff - should get ABS1
            'Last Sale Amount': 120000,
            'Grantor Name': 'Old Owner',
            'Zip': '24012',
            'City': 'Roanoke',
            'State': 'VA'  
        },
        {
            'Owner 1 Last Name': 'Brown',
            'Owner 1 First Name': 'Sarah',
            'Property Address': '999 Test Blvd, Roanoke, VA',
            'Owner Address': '999 Test Blvd, Roanoke, VA', # Same = owner occupied
            'Last Sale Date': '2020-03-15',                # Recent date
            'Last Sale Amount': 250000,
            'Grantor Name': 'Sarah Brown',                 # Same name = grantor match  
            'Zip': '24012',
            'City': 'Roanoke',
            'State': 'VA'
        }
    ]
    
    df = pd.DataFrame(test_data)
    
    # Create Excel files directory if it doesn't exist
    excel_dir = Path("Excel files")
    excel_dir.mkdir(exist_ok=True)
    
    # Save as Excel file
    output_file = excel_dir / "Test_Blank_Dates.xlsx"
    df.to_excel(output_file, index=False)
    
    print(f"Created test file: {output_file}")
    print(f"Records: {len(df)}")
    print("\nTest cases:")
    print("1. John Smith - Blank date, absentee -> Should be ABS1 (Priority 7)")
    print("2. Mary Johnson - 1900-01-01, owner occupied, grantor match -> Should be OIN1 (Priority 1)")
    print("3. Bob Wilson - 2008 date, absentee -> Should be ABS1 (Priority 7)") 
    print("4. Sarah Brown - Recent date, owner occupied, grantor match -> Should be OIN1 (Priority 1)")
    print(f"\nFile saved: {output_file}")

if __name__ == "__main__":
    create_test_excel()