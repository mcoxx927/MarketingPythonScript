"""
Check the results of our blank date test to verify the fix is working.
"""

import pandas as pd
from pathlib import Path

def check_results():
    """Check the processed results for blank date handling"""
    
    # Read the processed main region file
    main_file = Path("output/monthly_2025_09/main_region_processed_20250903.xlsx")
    
    if main_file.exists():
        print("Reading processed results...")
        df = pd.read_excel(main_file)
        
        # Look for our test records
        print(f"Total processed records: {len(df):,}")
        
        # Filter to test records by looking for specific names or addresses
        test_records = df[df['OwnerName'].str.contains('Smith|Johnson|Wilson|Brown', na=False, case=False)]
        
        if len(test_records) > 0:
            print(f"\nFound {len(test_records)} test records:")
            print("-" * 80)
            
            for idx, row in test_records.iterrows():
                print(f"Owner: {row['OwnerName']}")
                print(f"  Original Sale Date: {row.get('Last Sale Date', 'N/A')}")
                print(f"  Parsed Sale Date: {row.get('ParsedSaleDate', 'N/A')}")
                print(f"  Owner Occupied: {row.get('IsOwnerOccupied', False)}")
                print(f"  Priority: {row.get('PriorityCode', 'N/A')} - {row.get('PriorityName', 'N/A')}")
                print()
        else:
            print("No test records found in main processed file.")
            
        # Check overall priority distribution
        print("\nOverall Priority Distribution:")
        print("-" * 40)
        priority_dist = df['PriorityCode'].value_counts().head(10)
        for priority, count in priority_dist.items():
            pct = (count / len(df)) * 100
            print(f"  {priority}: {count:,} ({pct:.1f}%)")
            
        # Check for blank date indicators
        print(f"\nDate Quality Metrics:")
        print(f"  Records with ParsedSaleDate = 1850-01-01: {(df['ParsedSaleDate'] == '1850-01-01 00:00:00').sum():,}")
        print(f"  Records with ABS1 priority: {(df['PriorityCode'] == 'ABS1').sum():,}")
        print(f"  Records with OWN20 priority: {(df['PriorityCode'] == 'OWN20').sum():,}")
        
    else:
        print(f"Processed file not found: {main_file}")

if __name__ == "__main__":
    check_results()