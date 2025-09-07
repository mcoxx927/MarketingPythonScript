#!/usr/bin/env python3
"""
Quick test script to verify skip trace flag detection with actual ROA Skip Trace File data.
This tests that our updated _detect_skip_trace_flags() function properly handles the actual data formats.
"""

import pandas as pd
from skip_trace_processor import _detect_skip_trace_flags

def test_actual_skip_trace_flags():
    """Test flag detection with actual ROA Skip Trace File data"""
    try:
        # Load the actual skip trace file
        print("Loading ROA Skip Trace File.xlsx...")
        df = pd.read_excel("ROA Skip Trace File.xlsx")
        
        print(f"Loaded {len(df):,} total records")
        
        # Filter to Roanoke County (FIPS 51161)
        roanoke_df = df[df['Property FIPS'] == 51161].copy()
        print(f"Found {len(roanoke_df):,} Roanoke County records (FIPS 51161)")
        
        if len(roanoke_df) == 0:
            print("ERROR: No Roanoke County records found!")
            return False
        
        # Count flags in the data
        flag_counts = {}
        total_flags = 0
        
        print("\nAnalyzing skip trace flags...")
        for idx, row in roanoke_df.iterrows():
            flags = _detect_skip_trace_flags(row)
            total_flags += len(flags)
            for flag in flags:
                flag_counts[flag] = flag_counts.get(flag, 0) + 1
        
        # Print results
        print(f"\nSkip Trace Flag Detection Results:")
        print(f"{'Flag Type':<20} {'Count':<10}")
        print("-" * 30)
        
        for flag_type in ['STDeceased', 'STBankruptcy', 'STForeclosure', 'STLien', 'STJudgment', 'STQuitclaim']:
            count = flag_counts.get(flag_type, 0)
            print(f"{flag_type:<20} {count:<10}")
        
        print("-" * 30)
        print(f"{'Total Records w/ Flags':<20} {len([i for i, row in roanoke_df.iterrows() if _detect_skip_trace_flags(row)]):<10}")
        print(f"{'Total Flag Instances':<20} {total_flags:<10}")
        
        # Check specific columns manually to verify our logic
        print(f"\nManual verification of data formats:")
        
        # Check Owner Is Deceased column
        deceased_col = 'Owner Is Deceased'
        if deceased_col in roanoke_df.columns:
            deceased_values = roanoke_df[deceased_col].dropna().unique()[:5]  # First 5 unique values
            print(f"{deceased_col}: {[type(v).__name__ + ':' + str(v) for v in deceased_values]}")
            
            # Count 1.0 values (should match STDeceased count)
            deceased_count = sum(1 for val in roanoke_df[deceased_col] if pd.notna(val) and float(val) == 1.0)
            print(f"Manual count of {deceased_col} = 1.0: {deceased_count}")
        
        # Check distress date columns
        date_columns = ['Owner Bankruptcy', 'Owner Foreclosure', 'Lien', 'Judgment', 'Quitclaim']
        
        for col in date_columns:
            if col in roanoke_df.columns:
                # Count non-null, non-"No Data" values
                valid_dates = roanoke_df[col][
                    (roanoke_df[col].notna()) & 
                    (roanoke_df[col] != 'No Data') & 
                    (roanoke_df[col] != '')
                ]
                
                if len(valid_dates) > 0:
                    sample_val = valid_dates.iloc[0]
                    print(f"{col}: {len(valid_dates)} valid dates (sample: {type(sample_val).__name__}:{sample_val})")
                else:
                    print(f"{col}: 0 valid dates")
        
        # Success criteria: Should find substantial flags (user said ~200)
        expected_min_flags = 150  # Allow some margin
        if total_flags >= expected_min_flags:
            print(f"\n[SUCCESS] Found {total_flags} total flag instances (expected ~200)")
            return True
        else:
            print(f"\n[WARNING] Only found {total_flags} flag instances, expected ~200")
            return False
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_actual_skip_trace_flags()
    if success:
        print("\nSkip trace flag detection is working correctly with actual data!")
        exit(0)
    else:
        print("\nSkip trace flag detection needs further investigation.")
        exit(1)