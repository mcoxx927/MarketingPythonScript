"""
Test the property processor with a small sample to validate logic
"""

import pandas as pd
from property_processor import PropertyProcessor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def test_small_sample():
    """Test with a small sample from the main file"""
    
    # Load just first 100 records for testing
    main_file = "Excel files/Property Export Roanoke+City_2C+VA.xlsx"
    df = pd.read_excel(main_file, nrows=100)
    
    print("=== SAMPLE DATA TEST ===")
    print(f"Testing with {len(df)} records")
    
    # Show some raw data
    print("\nSample owner names:")
    for i in range(min(5, len(df))):
        last_name = df.iloc[i]['Owner 1 Last Name']
        first_name = df.iloc[i]['Owner 1 First Name']
        full_name = f"{last_name} {first_name}".strip()
        print(f"  {i+1}. {full_name}")
    
    # Create a temporary small Excel file for testing
    test_file = "test_sample.xlsx"
    df.to_excel(test_file, index=False)
    
    # Process it
    processor = PropertyProcessor()
    result_df = processor.process_excel_file(test_file)
    
    # Show results
    print("\n=== PROCESSING RESULTS ===")
    print(f"Trusts found: {result_df['IsTrust'].sum()}")
    print(f"Churches found: {result_df['IsChurch'].sum()}")
    print(f"Businesses found: {result_df['IsBusiness'].sum()}")
    print(f"Owner Occupied: {result_df['IsOwnerOccupied'].sum()}")
    
    print("\nPriority Distribution:")
    priority_counts = result_df['PriorityName'].value_counts()
    for priority, count in priority_counts.items():
        print(f"  {priority}: {count}")
    
    # Show some classified examples
    print("\n=== CLASSIFICATION EXAMPLES ===")
    
    # Show trusts
    trusts = result_df[result_df['IsTrust'] == True]
    if len(trusts) > 0:
        print(f"\nTrusts found ({len(trusts)}):")
        for idx, row in trusts.head(3).iterrows():
            print(f"  - {row['OwnerName']}")
    
    # Show churches
    churches = result_df[result_df['IsChurch'] == True]
    if len(churches) > 0:
        print(f"\nChurches found ({len(churches)}):")
        for idx, row in churches.head(3).iterrows():
            print(f"  - {row['OwnerName']}")
    
    # Show businesses
    businesses = result_df[result_df['IsBusiness'] == True]
    if len(businesses) > 0:
        print(f"\nBusinesses found ({len(businesses)}):")
        for idx, row in businesses.head(3).iterrows():
            print(f"  - {row['OwnerName']}")
    
    # Save sample results
    result_df.to_excel("test_results.xlsx", index=False)
    print(f"\nTest results saved to: test_results.xlsx")

if __name__ == "__main__":
    test_small_sample()