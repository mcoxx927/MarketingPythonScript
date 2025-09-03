import pandas as pd
import os
from pathlib import Path

def analyze_main_region_file():
    """Focus on the main region file for detailed analysis"""
    main_file = Path("Excel files/Property Export Roanoke+City_2C+VA.xlsx")
    
    if not main_file.exists():
        print(f"Main region file not found: {main_file}")
        return
    
    print("=== MAIN REGION FILE ANALYSIS ===\n")
    
    try:
        df = pd.read_excel(main_file)
        
        print(f"MAIN REGION FILE: {main_file.name}")
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        
        # Show sample of key fields for business logic understanding
        key_fields = ['Owner 1 Last Name', 'Owner 1 First Name', 'Address', 'Owner Occupied', 
                     'Mailing Address', 'Last Sale Date', 'Last Sale Amount', 'Est. Loan-to-Value',
                     'Est. Equity', 'Lien Type', 'BK Date', 'ListPriority']
        
        print(f"\nKey Business Fields Sample:")
        if all(field in df.columns for field in key_fields):
            sample_df = df[key_fields].head(3)
            print(sample_df.to_string(index=False))
        
        # Check for missing main region file and show all files
        print(f"\nAvailable files:")
        excel_dir = Path("Excel files") 
        for file_path in excel_dir.glob("*.xlsx"):
            file_size = len(pd.read_excel(file_path))
            print(f"  {file_path.name}: {file_size} rows")
            
    except Exception as e:
        print(f"Error reading main region file: {e}")

if __name__ == "__main__":
    analyze_main_region_file()