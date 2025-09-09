import pandas as pd
import sys
from pathlib import Path

# Add the parent directory to the path to import modules
sys.path.append(str(Path(__file__).parent.parent))

from monthly_processing_v2 import _normalize_address


def enhanced_niche_matching(main_df: pd.DataFrame, niche_df: pd.DataFrame, niche_type: str) -> dict:
    """
    Enhanced niche matching with Parcel ID priority, address fallback, and enhancement queue.
    
    Returns:
        dict with keys: 'parcel_matches', 'address_matches', 'needs_enhancement', 'stats'
    """
    results = {
        'parcel_matches': pd.DataFrame(),
        'address_matches': pd.DataFrame(), 
        'needs_enhancement': pd.DataFrame(),
        'stats': {}
    }
    
    # Clean input data
    main_clean = main_df.copy()
    niche_clean = niche_df[niche_df.get('Address', '').notna() & (niche_df.get('Address', '') != '')].copy()
    
    print(f"\n=== ENHANCED MATCHING TEST FOR {niche_type} ===")
    print(f"Main file records: {len(main_clean):,}")
    print(f"Niche file records: {len(niche_clean):,}")
    
    # Normalize addresses for all records
    main_clean['_NormalizedAddress'] = main_clean['Address'].apply(_normalize_address)
    niche_clean['_NormalizedAddress'] = niche_clean['Address'].apply(_normalize_address)
    
    # Check what parcel columns exist (include APN, PIN, etc.)
    main_parcel_cols = [col for col in main_clean.columns if any(term in col.lower() for term in ['parcel', 'apn', 'pin', 'map_id'])]
    niche_parcel_cols = [col for col in niche_clean.columns if any(term in col.lower() for term in ['parcel', 'apn', 'pin', 'map_id'])]
    
    print(f"Main file parcel columns: {main_parcel_cols}")
    print(f"Niche file parcel columns: {niche_parcel_cols}")
    
    # PHASE 1: PARCEL ID MATCHING
    parcel_matched_niche_indices = set()
    
    if main_parcel_cols and niche_parcel_cols:
        # Use first available parcel column from each
        main_parcel_col = main_parcel_cols[0]
        niche_parcel_col = niche_parcel_cols[0]
        
        print(f"\nPHASE 1: Parcel ID matching ({main_parcel_col} vs {niche_parcel_col})")
        
        # Clean parcel IDs - normalize both to XXXXXXX format (no dashes)
        def normalize_parcel(parcel_val):
            """Remove dashes and normalize parcel format for comparison"""
            if pd.isna(parcel_val):
                return ''
            parcel_str = str(parcel_val).strip().upper().replace('-', '')
            return parcel_str if parcel_str and parcel_str != 'NAN' else ''
        
        main_clean['_CleanParcel'] = main_clean[main_parcel_col].apply(normalize_parcel)
        niche_clean['_CleanParcel'] = niche_clean[niche_parcel_col].apply(normalize_parcel)
        
        # Remove empty/null parcels
        main_valid_parcels = main_clean[main_clean['_CleanParcel'].notna() & 
                                      (main_clean['_CleanParcel'] != '') & 
                                      (main_clean['_CleanParcel'] != 'NAN')]
        niche_valid_parcels = niche_clean[niche_clean['_CleanParcel'].notna() & 
                                        (niche_clean['_CleanParcel'] != '') & 
                                        (niche_clean['_CleanParcel'] != 'NAN')]
        
        print(f"Main records with valid parcel IDs: {len(main_valid_parcels):,}")
        print(f"Niche records with valid parcel IDs: {len(niche_valid_parcels):,}")
        
        # Find parcel matches
        parcel_matches = niche_valid_parcels[niche_valid_parcels['_CleanParcel'].isin(main_valid_parcels['_CleanParcel'])]
        parcel_matched_niche_indices = set(parcel_matches.index)
        
        print(f"Parcel ID matches found: {len(parcel_matches):,}")
        
        # Show sample parcel matches for validation
        if len(parcel_matches) > 0:
            print(f"\nSample parcel matches:")
            for i, (_, row) in enumerate(parcel_matches.head(3).iterrows()):
                main_match = main_valid_parcels[main_valid_parcels['_CleanParcel'] == row['_CleanParcel']].iloc[0]
                print(f"  {i+1}. Parcel: {row['_CleanParcel']}")
                print(f"     Niche: {row.get('Address', 'N/A')}")
                print(f"     Main:  {main_match.get('Address', 'N/A')}")
        
        results['parcel_matches'] = parcel_matches
    else:
        print(f"\nPHASE 1: SKIPPED - No parcel columns available")
    
    # PHASE 2: ADDRESS MATCHING (for remaining records)
    remaining_niche = niche_clean[~niche_clean.index.isin(parcel_matched_niche_indices)]
    
    print(f"\nPHASE 2: Address matching for remaining {len(remaining_niche):,} records")
    
    # Create address lookup for main file
    main_address_map = main_clean.groupby('_NormalizedAddress').groups
    
    # Find address matches
    address_matches = remaining_niche[remaining_niche['_NormalizedAddress'].isin(main_address_map.keys())]
    address_matched_niche_indices = set(address_matches.index)
    
    print(f"Address matches found: {len(address_matches):,}")
    
    # Show sample address matches for validation
    if len(address_matches) > 0:
        print(f"\nSample address matches:")
        for i, (_, row) in enumerate(address_matches.head(3).iterrows()):
            main_indices = main_address_map[row['_NormalizedAddress']]
            main_match = main_clean.iloc[list(main_indices)[0]]
            print(f"  {i+1}. Address: {row['_NormalizedAddress']}")
            print(f"     Niche: {row.get('Address', 'N/A')}")
            print(f"     Main:  {main_match.get('Address', 'N/A')}")
    
    results['address_matches'] = address_matches
    
    # PHASE 3: ENHANCEMENT QUEUE (unmatched records)
    all_matched_indices = parcel_matched_niche_indices.union(address_matched_niche_indices)
    needs_enhancement = niche_clean[~niche_clean.index.isin(all_matched_indices)]
    
    print(f"\nPHASE 3: Enhancement queue")
    print(f"Records needing enhancement: {len(needs_enhancement):,}")
    
    # Show sample enhancement queue records
    if len(needs_enhancement) > 0:
        print(f"\nSample enhancement queue records:")
        for i, (_, row) in enumerate(needs_enhancement.head(3).iterrows()):
            parcel_val = row.get(niche_parcel_cols[0] if niche_parcel_cols else 'Parcel ID', 'N/A')
            print(f"  {i+1}. Parcel: {parcel_val} | Address: {row.get('Address', 'N/A')}")
    
    results['needs_enhancement'] = needs_enhancement
    
    # STATISTICS
    total_niche = len(niche_clean)
    parcel_match_count = len(results['parcel_matches'])
    address_match_count = len(results['address_matches'])
    enhancement_count = len(results['needs_enhancement'])
    total_matched = parcel_match_count + address_match_count
    
    results['stats'] = {
        'total_niche_records': total_niche,
        'parcel_matches': parcel_match_count,
        'address_matches': address_match_count,
        'total_matches': total_matched,
        'needs_enhancement': enhancement_count,
        'match_rate': (total_matched / total_niche * 100) if total_niche > 0 else 0,
        'parcel_match_rate': (parcel_match_count / total_niche * 100) if total_niche > 0 else 0,
        'address_match_rate': (address_match_count / total_niche * 100) if total_niche > 0 else 0
    }
    
    print(f"\n=== MATCHING STATISTICS ===")
    print(f"Total niche records: {total_niche:,}")
    print(f"Parcel ID matches: {parcel_match_count:,} ({results['stats']['parcel_match_rate']:.1f}%)")
    print(f"Address matches: {address_match_count:,} ({results['stats']['address_match_rate']:.1f}%)")
    print(f"Total matches: {total_matched:,} ({results['stats']['match_rate']:.1f}%)")
    print(f"Need enhancement: {enhancement_count:,} ({(enhancement_count/total_niche*100):.1f}%)")
    
    return results


def test_with_real_data():
    """Test enhanced matching with real Roanoke code enforcement data"""
    
    # Load real data files
    try:
        # Load main region file (check if exists)
        main_file_candidates = [
            "regions/roanoke_city_va/main_region.xlsx",
            "regions/roanoke_county_va/main_region.xlsx",
            Path("Excel files").glob("*Roanoke*.xlsx")
        ]
        
        main_file = None
        for candidate in main_file_candidates:
            if isinstance(candidate, str) and Path(candidate).exists():
                main_file = candidate
                break
            elif hasattr(candidate, '__iter__'):
                try:
                    main_file = str(next(candidate))
                    break
                except StopIteration:
                    continue
        
        if not main_file:
            print("ERROR: Could not find main region file for testing")
            return
        
        print(f"Loading main file: {main_file}")
        main_df = pd.read_excel(main_file)
        
        # Load code enforcement file
        niche_file = "regions/roanoke_city_va/roanoke_city_va_code_enforcement_20250225.xlsx"
        if not Path(niche_file).exists():
            print(f"ERROR: Code enforcement file not found: {niche_file}")
            return
        
        print(f"Loading niche file: {niche_file}")
        niche_df = pd.read_excel(niche_file)
        
        # Run enhanced matching test
        results = enhanced_niche_matching(main_df, niche_df, "CodeEnforcement")
        
        # Save results for review
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
        
        if len(results['parcel_matches']) > 0:
            parcel_file = f"test_results_parcel_matches_{timestamp}.xlsx"
            results['parcel_matches'].to_excel(parcel_file, index=False)
            print(f"Parcel matches saved to: {parcel_file}")
        
        if len(results['address_matches']) > 0:
            address_file = f"test_results_address_matches_{timestamp}.xlsx"
            results['address_matches'].to_excel(address_file, index=False)
            print(f"Address matches saved to: {address_file}")
        
        if len(results['needs_enhancement']) > 0:
            enhancement_file = f"test_results_needs_enhancement_{timestamp}.xlsx"
            results['needs_enhancement'].to_excel(enhancement_file, index=False)
            print(f"Enhancement queue saved to: {enhancement_file}")
        
        return results
        
    except Exception as e:
        print(f"ERROR in testing: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    print("=== ENHANCED NICHE MATCHING TEST ===")
    test_with_real_data()