"""
Skip Trace Integration Processor

This script integrates skip trace data back into existing enhanced region files,
adding Golden Address fields and skip trace distress flags.

Usage:
    python skip_trace_processor.py --region roanoke_city_va --enhanced-file "path/to/enhanced/file" --skip-trace-file "path/to/skip/trace/file"
    python skip_trace_processor.py --all-regions --skip-trace-file "path/to/skip/trace/file"
"""

import logging
import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import re

from multi_region_config import MultiRegionConfigManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Console output
    ]
)
logger = logging.getLogger(__name__)

def _normalize_address(address_str) -> str:
    """Normalize address for matching (reuse from monthly_processing_v2.py)"""
    if pd.isna(address_str) or address_str == '':
        return ''
    
    # Convert to string and normalize
    addr = str(address_str).upper().strip()
    
    # Remove common variations
    addr = addr.replace(' ST,', ' ST')
    addr = addr.replace(' AVE,', ' AVE') 
    addr = addr.replace(' RD,', ' RD')
    addr = addr.replace(' DR,', ' DR')
    addr = addr.replace(' BLVD,', ' BLVD')
    
    # Remove trailing commas and extra spaces
    addr = addr.replace(',', ' ').strip()
    
    # Collapse multiple spaces
    addr = re.sub(r'\s+', ' ', addr)
    
    return addr

def _normalize_city(city_str) -> str:
    """Normalize city name for matching"""
    if pd.isna(city_str) or city_str == '':
        return ''
    
    # Convert to string and normalize
    city = str(city_str).upper().strip()
    
    # Remove common variations and standardize
    city = city.replace('.', '')  # Remove periods
    city = re.sub(r'\s+', ' ', city)  # Collapse spaces
    
    return city

def _create_address_city_key(address: str, city: str) -> str:
    """Create compound key for address + city matching"""
    norm_addr = _normalize_address(address)
    norm_city = _normalize_city(city)
    
    if norm_addr and norm_city:
        return f"{norm_addr}|{norm_city}"
    elif norm_addr:
        return norm_addr  # Fallback to address only
    else:
        return ""

def _detect_skip_trace_flags(row: pd.Series) -> List[str]:
    """Detect skip trace flags based on actual data format and return appropriate ST codes"""
    import datetime
    flags = []
    
    # Owner Is Deceased - check for 1.0 (Excel converts TRUE to 1.0)
    deceased_val = row.get('Owner Is Deceased')
    if pd.notna(deceased_val):
        try:
            if float(deceased_val) == 1.0:
                flags.append('STDeceased')
        except (ValueError, TypeError):
            # Handle non-numeric values - check for string TRUE
            if str(deceased_val).lower() in ['true', 'yes', '1', 'y']:
                flags.append('STDeceased')
    
    # Date-based distress indicators - check for actual datetime objects
    date_columns = {
        'Owner Bankruptcy': 'STBankruptcy',
        'Owner Foreclosure': 'STForeclosure',
        'Lien': 'STLien',
        'Judgment': 'STJudgment',
        'Quitclaim': 'STQuitclaim'
    }
    
    for col_name, st_code in date_columns.items():
        if col_name in row:
            value = row[col_name]
            # Check if it's a datetime object (indicates actual distress date)
            if (pd.notna(value) and 
                isinstance(value, (datetime.datetime, pd.Timestamp)) and
                str(value) != 'No Data'):
                flags.append(st_code)
    
    return flags

def _match_skip_trace_hybrid(enhanced_df: pd.DataFrame, skip_trace_df: pd.DataFrame, region_fips: str) -> pd.DataFrame:
    """
    Match skip trace data using hybrid approach: APN+FIPS primary, address fallback
    
    Args:
        enhanced_df: Main enhanced region DataFrame
        skip_trace_df: Skip trace data DataFrame  
        region_fips: Expected FIPS code for this region
        
    Returns:
        Enhanced DataFrame with skip trace data integrated
    """
    logger.info("Starting hybrid skip trace matching...")
    
    # Filter skip trace data to this region's FIPS
    # Handle both string and numeric FIPS codes
    skip_trace_fips = skip_trace_df['Property FIPS'].astype(str).str.replace('.0', '').str.strip()
    region_fips_str = str(region_fips).strip()
    st_region_data = skip_trace_df[skip_trace_fips == region_fips_str].copy()
    
    if st_region_data.empty:
        logger.warning(f"No skip trace data found for FIPS {region_fips}")
        # Add empty skip trace columns to enhanced_df
        enhanced_df['Golden_Address'] = None
        enhanced_df['Golden_City'] = None
        enhanced_df['Golden_State'] = None
        enhanced_df['Golden_Zip'] = None
        enhanced_df['Golden_Address_Differs'] = False
        enhanced_df['ST_Flags'] = ''
        
        # Initialize skip trace boolean flag columns
        st_flag_columns = ['HasSTBankruptcy', 'HasSTForeclosure', 'HasSTLien', 'HasSTJudgment', 'HasSTQuitclaim', 'HasSTDeceased']
        for col in st_flag_columns:
            if col not in enhanced_df.columns:
                enhanced_df[col] = False
        return enhanced_df
    
    logger.info(f"Found {len(st_region_data)} skip trace records for FIPS {region_fips}")
    
    # Initialize skip trace columns in enhanced_df
    enhanced_df['Golden_Address'] = None
    enhanced_df['Golden_City'] = None
    enhanced_df['Golden_State'] = None
    enhanced_df['Golden_Zip'] = None
    enhanced_df['Golden_Address_Differs'] = False  
    enhanced_df['ST_Flags'] = ''
    
    # Initialize skip trace boolean flag columns if they don't exist
    st_flag_columns = ['HasSTBankruptcy', 'HasSTForeclosure', 'HasSTLien', 'HasSTJudgment', 'HasSTQuitclaim', 'HasSTDeceased']
    for col in st_flag_columns:
        if col not in enhanced_df.columns:
            enhanced_df[col] = False
    
    matches_apn = 0
    matches_address = 0
    
    # Phase 1: Primary matching on APN + FIPS (if APN column exists)
    if 'APN' in enhanced_df.columns and 'Property APN' in st_region_data.columns:
        logger.info("Phase 1: Matching on APN + FIPS...")
        
        # Create lookup dictionary for APN matches
        apn_lookup = {}
        for idx, row in st_region_data.iterrows():
            apn = str(row['Property APN']).strip()
            if apn and apn != 'nan':
                apn_lookup[apn] = row
        
        # Apply APN matches
        for idx, enh_row in enhanced_df.iterrows():
            enh_apn = str(enh_row.get('APN', '')).strip()
            if enh_apn and enh_apn != 'nan' and enh_apn in apn_lookup:
                st_row = apn_lookup[enh_apn]
                
                # Apply Golden Address fields
                if pd.notna(st_row.get('Golden Address')):
                    enhanced_df.loc[idx, 'Golden_Address'] = st_row['Golden Address']
                    # Check if different from original mailing address
                    original_addr = enhanced_df.loc[idx, 'Mailing Address']
                    enhanced_df.loc[idx, 'Golden_Address_Differs'] = (
                        pd.notna(original_addr) and 
                        str(st_row['Golden Address']).strip() != str(original_addr).strip()
                    )
                
                # Apply Golden City, State, Zip
                if pd.notna(st_row.get('Golden City')):
                    enhanced_df.loc[idx, 'Golden_City'] = st_row['Golden City']
                if pd.notna(st_row.get('Golden State')):
                    enhanced_df.loc[idx, 'Golden_State'] = st_row['Golden State']
                if pd.notna(st_row.get('Golden Zip')):
                    enhanced_df.loc[idx, 'Golden_Zip'] = st_row['Golden Zip']
                
                # Apply skip trace flags
                st_flags = _detect_skip_trace_flags(st_row)
                if st_flags:
                    enhanced_df.loc[idx, 'ST_Flags'] = ','.join(st_flags)
                
                matches_apn += 1
        
        logger.info(f"APN+FIPS matches: {matches_apn}")
    
    # Phase 2: Address+City-based matching for unmatched records
    logger.info("Phase 2: Address+City-based matching for remaining records...")
    
    # Create address+city lookup for skip trace data (primary)
    address_city_lookup = {}
    # Also maintain address-only lookup as fallback
    address_only_lookup = {}
    
    for idx, row in st_region_data.iterrows():
        # Try address+city combination first (most accurate)
        if 'Property City' in row and pd.notna(row['Property City']):
            addr_city_key = _create_address_city_key(row['Property Address'], row['Property City'])
            if addr_city_key and '|' in addr_city_key:  # Only if we have both address and city
                address_city_lookup[addr_city_key] = row
        
        # Also create address-only fallback
        norm_addr = _normalize_address(row['Property Address'])
        if norm_addr:
            address_only_lookup[norm_addr] = row
    
    # Apply address+city matches to records that don't already have skip trace data
    city_matches = 0
    fallback_matches = 0
    
    for idx, enh_row in enhanced_df.iterrows():
        # Skip if already matched by APN
        if pd.notna(enhanced_df.loc[idx, 'Golden_Address']) or (pd.notna(enhanced_df.loc[idx, 'ST_Flags']) and enhanced_df.loc[idx, 'ST_Flags'] != ''):
            continue
        
        st_row = None
        match_type = None
        
        # First try: Address + City matching (most accurate)
        if 'City' in enh_row and pd.notna(enh_row['City']):
            addr_city_key = _create_address_city_key(enh_row['Address'], enh_row['City'])
            if addr_city_key and '|' in addr_city_key and addr_city_key in address_city_lookup:
                st_row = address_city_lookup[addr_city_key]
                match_type = "address+city"
                city_matches += 1
        
        # Second try: Address-only fallback (less accurate, but still useful)
        if st_row is None:
            norm_addr = _normalize_address(enh_row['Address'])
            if norm_addr and norm_addr in address_only_lookup:
                st_row = address_only_lookup[norm_addr]
                match_type = "address-only"
                fallback_matches += 1
        
        # Apply match if found
        if st_row is not None:
            # Apply Golden Address fields
            if pd.notna(st_row.get('Golden Address')):
                enhanced_df.loc[idx, 'Golden_Address'] = st_row['Golden Address']
                # Check if different from original mailing address
                original_addr = enhanced_df.loc[idx, 'Mailing Address']
                enhanced_df.loc[idx, 'Golden_Address_Differs'] = (
                    pd.notna(original_addr) and 
                    str(st_row['Golden Address']).strip() != str(original_addr).strip()
                )
            
            # Apply Golden City, State, Zip
            if pd.notna(st_row.get('Golden City')):
                enhanced_df.loc[idx, 'Golden_City'] = st_row['Golden City']
            if pd.notna(st_row.get('Golden State')):
                enhanced_df.loc[idx, 'Golden_State'] = st_row['Golden State']
            if pd.notna(st_row.get('Golden Zip')):
                enhanced_df.loc[idx, 'Golden_Zip'] = st_row['Golden Zip']
            
            # Apply skip trace flags
            st_flags = _detect_skip_trace_flags(st_row)
            if st_flags:
                enhanced_df.loc[idx, 'ST_Flags'] = ','.join(st_flags)
    
    matches_address = city_matches + fallback_matches
    
    logger.info(f"Address+City matches: {city_matches}")
    logger.info(f"Address-only fallback matches: {fallback_matches}")
    logger.info(f"Total address matches: {matches_address}")
    
    # Phase 3: Update boolean skip trace flags for records with ST flags
    logger.info("Phase 3: Updating boolean skip trace flags...")
    flag_updates = 0
    
    # Mapping of ST flags to boolean column names
    st_flag_mapping = {
        'STBankruptcy': 'HasSTBankruptcy',
        'STForeclosure': 'HasSTForeclosure', 
        'STLien': 'HasSTLien',
        'STJudgment': 'HasSTJudgment',
        'STQuitclaim': 'HasSTQuitclaim',
        'STDeceased': 'HasSTDeceased'
    }
    
    for idx, row in enhanced_df.iterrows():
        st_flags = row.get('ST_Flags', '')
        if st_flags:
            # Set boolean flags for each detected ST flag
            st_flag_list = st_flags.split(',')
            for flag in st_flag_list:
                if flag in st_flag_mapping:
                    col_name = st_flag_mapping[flag]
                    enhanced_df.loc[idx, col_name] = True
                    flag_updates += 1
    
    # Clean up temporary columns if they exist
    temp_columns = ['_NormalizedAddress']
    existing_temp_columns = [col for col in temp_columns if col in enhanced_df.columns]
    if existing_temp_columns:
        enhanced_df.drop(columns=existing_temp_columns, inplace=True)
    
    logger.info(f"Skip trace integration complete:")
    logger.info(f"  Total APN+FIPS matches: {matches_apn}")
    logger.info(f"  Total address+city matches: {city_matches}")
    logger.info(f"  Total address-only fallback matches: {fallback_matches}")
    logger.info(f"  Total address matches: {matches_address}")
    logger.info(f"  Total records with Golden Address: {enhanced_df['Golden_Address'].notna().sum()}")
    logger.info(f"  Golden Address differs from original: {enhanced_df['Golden_Address_Differs'].sum()}")
    logger.info(f"  Records with ST flags: {(enhanced_df['ST_Flags'] != '').sum()}")
    logger.info(f"  Skip trace boolean flags updated: {flag_updates}")
    
    return enhanced_df

def process_region_skip_trace(region_key: str, enhanced_file_path: str, skip_trace_file_path: str, 
                             config_manager: MultiRegionConfigManager) -> Dict:
    """
    Process skip trace integration for a single region
    
    Args:
        region_key: Region identifier (e.g., 'roanoke_city_va')
        enhanced_file_path: Path to existing enhanced region file
        skip_trace_file_path: Path to skip trace data file
        config_manager: Configuration manager instance
        
    Returns:
        Dictionary with processing results
    """
    print("=" * 70)
    print(f"SKIP TRACE PROCESSING: {region_key.upper()}")
    print("=" * 70)
    
    # Get region configuration
    config = config_manager.get_region_config(region_key)
    region_code = config.region_code.lower()
    
    # Set up region-specific logging
    log_file = Path("output") / region_key / datetime.now().strftime('%Y_%m') / f"{region_code}_skip_trace_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    try:
        print(f"Region: {config.region_name}")
        print(f"FIPS Code: {config.fips_code}")
        print(f"Enhanced File: {enhanced_file_path}")
        print(f"Skip Trace File: {skip_trace_file_path}")
        print()
        
        # Load enhanced region file
        print("STEP 1: Loading enhanced region file...")
        enhanced_file = Path(enhanced_file_path)
        if not enhanced_file.exists():
            raise FileNotFoundError(f"Enhanced file not found: {enhanced_file_path}")
        
        enhanced_df = pd.read_excel(enhanced_file)
        print(f"Loaded {len(enhanced_df):,} enhanced records")
        
        # Load skip trace file
        print("\\nSTEP 2: Loading skip trace file...")
        skip_trace_file = Path(skip_trace_file_path)
        if not skip_trace_file.exists():
            raise FileNotFoundError(f"Skip trace file not found: {skip_trace_file_path}")
        
        skip_trace_df = pd.read_excel(skip_trace_file)
        print(f"Loaded {len(skip_trace_df):,} skip trace records")
        
        # Validate skip trace file has required columns
        required_st_columns = ['Golden Address', 'Property FIPS', 'Property Address']
        optional_st_columns = ['Property APN', 'Golden City', 'Golden State', 'Golden Zip', 'Owner Bankruptcy', 'Owner Foreclosure', 'Lien', 'Judgment', 'Quitclaim', 'Owner Is Deceased']
        
        missing_required = [col for col in required_st_columns if col not in skip_trace_df.columns]
        if missing_required:
            raise ValueError(f"Skip trace file missing required columns: {missing_required}")
        
        available_optional = [col for col in optional_st_columns if col in skip_trace_df.columns]
        print(f"Available optional columns: {available_optional}")
        
        # Process skip trace integration
        print("\\nSTEP 3: Integrating skip trace data...")
        updated_df = _match_skip_trace_hybrid(enhanced_df, skip_trace_df, config.fips_code)
        
        # Save updated file in place
        print("\\nSTEP 4: Saving updated file...")
        updated_df.to_excel(enhanced_file, index=False)
        print(f"Updated file saved: {enhanced_file}")
        
        # Generate summary stats
        golden_address_count = updated_df['Golden_Address'].notna().sum()
        golden_city_count = updated_df['Golden_City'].notna().sum()
        golden_state_count = updated_df['Golden_State'].notna().sum()
        golden_zip_count = updated_df['Golden_Zip'].notna().sum()
        golden_differs_count = updated_df['Golden_Address_Differs'].sum()
        st_flags_count = (updated_df['ST_Flags'] != '').sum()
        
        print("\\nFINAL SUMMARY")
        print("=" * 70)
        print(f"Region: {config.region_name}")
        print(f"Total Records: {len(updated_df):,}")
        print(f"Records with Golden Address: {golden_address_count:,}")
        print(f"Records with Golden City: {golden_city_count:,}")
        print(f"Records with Golden State: {golden_state_count:,}")
        print(f"Records with Golden Zip: {golden_zip_count:,}")
        print(f"Golden Address differs from original: {golden_differs_count:,}")
        print(f"Records with Skip Trace flags: {st_flags_count:,}")
        
        if st_flags_count > 0:
            # Show skip trace flag distribution
            st_flag_dist = {}
            for flags in updated_df[updated_df['ST_Flags'] != '']['ST_Flags']:
                for flag in str(flags).split(','):
                    st_flag_dist[flag] = st_flag_dist.get(flag, 0) + 1
            
            print(f"\\nSkip Trace Flag Distribution:")
            for flag, count in sorted(st_flag_dist.items()):
                pct = (count / len(updated_df)) * 100
                print(f"   {flag}: {count:,} ({pct:.1f}%)")
        
        print("=" * 70)
        
        # Clean up logging handler
        logger.removeHandler(file_handler)
        file_handler.close()
        
        return {
            'success': True,
            'region_name': config.region_name,
            'total_records': len(updated_df),
            'golden_address_count': golden_address_count,
            'golden_city_count': golden_city_count,
            'golden_state_count': golden_state_count,
            'golden_zip_count': golden_zip_count,
            'golden_differs_count': golden_differs_count,
            'st_flags_count': st_flags_count,
            'output_file': str(enhanced_file)
        }
        
    except Exception as e:
        logger.error(f"Skip trace processing failed for {region_key}: {e}")
        print(f"\\nERROR: Skip trace processing failed - {e}")
        
        # Clean up logging handler
        logger.removeHandler(file_handler)
        file_handler.close()
        
        return {'success': False, 'error': str(e)}

def find_enhanced_files(region_key: str, config_manager: MultiRegionConfigManager) -> List[Path]:
    """Find enhanced files for a region"""
    output_dir = Path("output") / region_key
    
    if not output_dir.exists():
        return []
    
    # Look for enhanced files in current and recent months
    enhanced_files = []
    for month_dir in output_dir.iterdir():
        if month_dir.is_dir():
            pattern = f"*_main_region_enhanced_*.xlsx"
            enhanced_files.extend(month_dir.glob(pattern))
    
    # Sort by modification time (newest first)
    enhanced_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    
    return enhanced_files

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Skip Trace Integration Processor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python skip_trace_processor.py --region roanoke_city_va --enhanced-file "output/roanoke_city_va/2024_01/roa_main_region_enhanced_20240115.xlsx" --skip-trace-file "skip_trace_data.xlsx"
  python skip_trace_processor.py --all-regions --skip-trace-file "skip_trace_data.xlsx"
  python skip_trace_processor.py --region roanoke_city_va --skip-trace-file "skip_trace_data.xlsx"  # Auto-find latest enhanced file
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--region", help="Process specific region (e.g., roanoke_city_va)")
    group.add_argument("--all-regions", action="store_true", help="Process all regions")
    
    parser.add_argument("--skip-trace-file", required=True, help="Path to skip trace data file")
    parser.add_argument("--enhanced-file", help="Path to enhanced region file (optional, will auto-find latest if not specified)")
    
    args = parser.parse_args()
    
    try:
        # Initialize configuration manager
        config_manager = MultiRegionConfigManager()
        
        if args.region:
            # Process single region
            enhanced_file_path = args.enhanced_file
            
            # Auto-find latest enhanced file if not specified
            if not enhanced_file_path:
                enhanced_files = find_enhanced_files(args.region, config_manager)
                if not enhanced_files:
                    print(f"ERROR: No enhanced files found for region {args.region}")
                    print("Run monthly_processing_v2.py first to generate enhanced files")
                    exit(1)
                
                enhanced_file_path = str(enhanced_files[0])
                print(f"Auto-selected enhanced file: {enhanced_file_path}")
            
            result = process_region_skip_trace(args.region, enhanced_file_path, args.skip_trace_file, config_manager)
            
            if result['success']:
                print("\\n[SUCCESS] Skip trace processing completed successfully!")
            else:
                print(f"\\n[ERROR] Skip trace processing failed: {result.get('error', 'Unknown error')}")
                exit(1)
                
        elif args.all_regions:
            # Process all regions
            print("\\n[BATCH] SKIP TRACE PROCESSING ALL REGIONS")
            print("=" * 70)
            
            results = []
            for region_key in config_manager.configs.keys():
                print(f"\\nStarting skip trace processing for {region_key}...")
                
                # Find latest enhanced file for this region
                enhanced_files = find_enhanced_files(region_key, config_manager)
                if not enhanced_files:
                    print(f"WARNING: No enhanced files found for region {region_key}, skipping")
                    results.append({'success': False, 'region_key': region_key, 'error': 'No enhanced files found'})
                    continue
                
                enhanced_file_path = str(enhanced_files[0])
                result = process_region_skip_trace(region_key, enhanced_file_path, args.skip_trace_file, config_manager)
                result['region_key'] = region_key
                results.append(result)
            
            # Summary of all regions
            print("\\n\\n[SUMMARY] SKIP TRACE BATCH PROCESSING SUMMARY")
            print("=" * 70)
            
            successful = [r for r in results if r.get('success', False)]
            failed = [r for r in results if not r.get('success', False)]
            
            print(f"Successfully processed: {len(successful)} regions")
            print(f"Failed: {len(failed)} regions")
            
            if successful:
                total_records = sum(r.get('total_records', 0) for r in successful)
                total_golden_address = sum(r.get('golden_address_count', 0) for r in successful)
                total_golden_city = sum(r.get('golden_city_count', 0) for r in successful)
                total_golden_state = sum(r.get('golden_state_count', 0) for r in successful)
                total_golden_zip = sum(r.get('golden_zip_count', 0) for r in successful)
                total_st_flags = sum(r.get('st_flags_count', 0) for r in successful)
                
                print(f"Total records processed: {total_records:,}")
                print(f"Total Golden Addresses added: {total_golden_address:,}")
                print(f"Total Golden Cities added: {total_golden_city:,}")
                print(f"Total Golden States added: {total_golden_state:,}")
                print(f"Total Golden Zips added: {total_golden_zip:,}")
                print(f"Total records with ST flags: {total_st_flags:,}")
            
            if failed:
                print(f"\\n[FAILED] Failed regions:")
                for result in failed:
                    print(f"  - {result.get('region_key', 'Unknown')}: {result.get('error', 'Unknown error')}")
    
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\\n[ERROR] Application error: {e}")
        exit(1)

if __name__ == "__main__":
    main()