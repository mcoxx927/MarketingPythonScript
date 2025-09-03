"""
Check the actual column names in niche files to fix the address matching.
"""

import pandas as pd
from pathlib import Path

def check_niche_columns():
    """Check what columns the niche files actually have"""
    
    excel_dir = Path("Excel files")
    excel_files = list(excel_dir.glob("*.xlsx"))
    
    # Find the largest file (main region)
    main_file = max(excel_files, key=lambda f: f.stat().st_size)
    niche_files = [f for f in excel_files if f != main_file]
    
    print("MAIN REGION FILE COLUMNS:")
    print("=" * 50)
    main_df = pd.read_excel(main_file, nrows=1)
    for i, col in enumerate(main_df.columns):
        print(f"{i:2d}: {col}")
    
    print(f"\n\nNICHE FILE COLUMNS:")
    print("=" * 50)
    
    for niche_file in niche_files[:3]:  # Check first 3 niche files
        print(f"\nFile: {niche_file.name}")
        print("-" * 40)
        try:
            df = pd.read_excel(niche_file, nrows=1)
            print(f"Columns ({len(df.columns)}):")
            for i, col in enumerate(df.columns):
                print(f"  {i:2d}: {col}")
                
            # Look for address-like columns
            address_cols = [col for col in df.columns if 'address' in col.lower() or 'addr' in col.lower()]
            if address_cols:
                print(f"Address columns: {address_cols}")
            else:
                print("No obvious address columns found")
                
        except Exception as e:
            print(f"Error reading {niche_file.name}: {e}")

if __name__ == "__main__":
    check_niche_columns()