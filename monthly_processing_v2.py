"""
Multi-Region Monthly Real Estate Direct Mail Processing Script

This enhanced version supports processing multiple regions with individual configurations,
standardized file naming, and organized output structure.

Usage:
    python monthly_processing_v2.py --region roanoke_city_va
    python monthly_processing_v2.py --all-regions  
    python monthly_processing_v2.py --list-regions
"""

import logging
import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import re

from multi_region_config import MultiRegionConfigManager
from enhanced_property_processor import EnhancedPropertyProcessor, DistressFlagManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Console output
    ]
)
logger = logging.getLogger(__name__)

# Constants
NICHE_ONLY_PRIORITY_ID = 99
VERY_OLD_DATE_STR = '1850-01-01'

def _detect_niche_type_from_filename(filename: str) -> str:
    """Detect niche type from filename"""
    filename_lower = filename.lower()
    
    if 'lien' in filename_lower:
        return 'Liens'
    elif 'foreclosure' in filename_lower or 'preforeclosure' in filename_lower:
        return 'PreForeclosure'
    elif 'bankrupt' in filename_lower:
        return 'Bankruptcy'
    elif 'landlord' in filename_lower or 'tired' in filename_lower:
        return 'Landlord'
    elif ('tax' in filename_lower and 'delinq' in filename_lower) or ('delinq' in filename_lower):
        # Distinguish between current city tax delinquencies and historical vendor data
        if 'current' in filename_lower or filename_lower.startswith(('roanoke_', 'lynchburg_', 'norfolk_')):
            return 'CurrentTax'  # Higher priority - direct from locality
        else:
            return 'TaxHistory'  # Lower priority - historical vendor data
    elif 'probate' in filename_lower:
        return 'Probate'
    elif 'interfamily' in filename_lower or 'family' in filename_lower:
        return 'InterFamily'
    elif 'cash' in filename_lower and 'buyer' in filename_lower:
        return 'CashBuyer'
    elif 'vacant' in filename_lower:
        return 'Vacant'
    elif 'code' in filename_lower and 'enforcement' in filename_lower:
        return 'CodeEnforcement'
    elif 'inherited' in filename_lower or 'inherit' in filename_lower:
        return 'Inherited'
    else:
        return 'Other'

def _normalize_address(address_str) -> str:
    """Normalize address for matching"""
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

def _append_unique_records(main_df: pd.DataFrame, recent_sales_df: pd.DataFrame) -> tuple:
    """
    Append unique records from recent sales to main DataFrame.
    
    Returns:
        tuple: (combined_dataframe, records_added_count)
    """
    if recent_sales_df.empty:
        return main_df, 0
    
    # Normalize addresses for matching
    main_df_temp = main_df.copy()
    recent_sales_temp = recent_sales_df.copy()
    
    main_df_temp['_NormalizedAddress'] = main_df_temp['Address'].apply(_normalize_address)
    recent_sales_temp['_NormalizedAddress'] = recent_sales_temp['Address'].apply(_normalize_address)
    
    # Filter out records with empty addresses
    recent_sales_clean = recent_sales_temp[recent_sales_temp['_NormalizedAddress'] != ''].copy()
    
    # Find records in recent sales that don't exist in main
    existing_addresses = set(main_df_temp['_NormalizedAddress'].unique())
    new_records = recent_sales_clean[~recent_sales_clean['_NormalizedAddress'].isin(existing_addresses)].copy()
    
    records_added = 0
    if len(new_records) > 0:
        # Remove temporary column before concatenation
        new_records.drop(columns=['_NormalizedAddress'], inplace=True, errors='ignore')
        
        # Concatenate new records to main DataFrame
        main_df = pd.concat([main_df, new_records], ignore_index=True, sort=False)
        records_added = len(new_records)
    
    # Clean up temporary column from main DataFrame
    if '_NormalizedAddress' in main_df.columns:
        main_df.drop(columns=['_NormalizedAddress'], inplace=True, errors='ignore')
    
    return main_df, records_added

def _cleanup_fips_mismatches(region_dir: Path, expected_fips: str, fips_mismatches: List[Dict]) -> bool:
    """
    Clean files by removing records that don't match the expected FIPS code.
    
    Args:
        region_dir: Path to the region directory
        expected_fips: The correct FIPS code for this region
        fips_mismatches: List of files with FIPS mismatches
        
    Returns:
        bool: True if cleanup succeeded, False otherwise
    """
    try:
        for mismatch in fips_mismatches:
            file_path = region_dir / mismatch['file']
            backup_path = region_dir / f"{mismatch['file']}.backup"
            
            print(f"  Processing: {mismatch['file']}")
            
            try:
                # Read the original file
                df = pd.read_excel(file_path)
                original_count = len(df)
                
                # Create backup
                df.to_excel(backup_path, index=False)
                print(f"    Backup created: {backup_path.name}")
                
                # Filter to keep only expected FIPS
                if 'FIPS' not in df.columns:
                    print(f"    WARNING: No FIPS column found in {mismatch['file']}")
                    continue
                
                # Filter records
                filtered_df = df[df['FIPS'].astype(str) == str(expected_fips)].copy()
                filtered_count = len(filtered_df)
                removed_count = original_count - filtered_count
                
                if filtered_count == 0:
                    print(f"    ERROR: No records would remain after filtering {mismatch['file']}")
                    return False
                
                # Save cleaned file
                filtered_df.to_excel(file_path, index=False)
                print(f"    Cleaned: {original_count:,} -> {filtered_count:,} records ({removed_count:,} removed)")
                
            except Exception as file_error:
                print(f"    ERROR cleaning {mismatch['file']}: {file_error}")
                logger.error(f"Error cleaning {mismatch['file']}: {file_error}")
                return False
        
        return True
        
    except Exception as e:
        print(f"ERROR in cleanup process: {e}")
        logger.error(f"Error in FIPS cleanup process: {e}")
        return False

def _update_main_with_niche(main_df: pd.DataFrame, niche_df: pd.DataFrame, niche_type: str) -> tuple:
    """
    Update main region DataFrame with niche data using boolean flag architecture.
    
    Returns:
        tuple: (updated_main_df, updates_count, inserts_count)
    """
    updates_count = 0
    inserts_count = 0
    
    # Mapping from niche types to boolean flag column names
    niche_flag_columns = {
        'Liens': 'HasLiens',
        'PreForeclosure': 'HasForeclosure',
        'CodeEnforcement': 'HasCodeEnforcement', 
        'CurrentTax': 'HasCurrentTax',
        'TaxHistory': 'HasTaxHistory',
        'Bankruptcy': 'HasBankruptcy',
        'CashBuyer': 'HasCashBuyer',
        'InterFamily': 'HasInterFamily',
        'Landlord': 'HasLandlord',
        'Probate': 'HasProbate',
        'Inherited': 'HasInherited'
    }
    
    # Get the boolean flag column for this niche type
    flag_column = niche_flag_columns.get(niche_type)
    if not flag_column:
        logger.warning(f"Unknown niche type for boolean flags: {niche_type}")
        return main_df, 0, 0
    
    # Normalize addresses for matching
    main_df['_NormalizedAddress'] = main_df['Address'].apply(_normalize_address)
    niche_df['_NormalizedAddress'] = niche_df['Address'].apply(_normalize_address)
    
    # Create dictionary mapping addresses to main DataFrame indices for fast lookup
    main_address_map = main_df.groupby('_NormalizedAddress').groups
    
    # Separate niche records into updates and inserts
    niche_df_clean = niche_df[niche_df['_NormalizedAddress'] != ''].copy()
    
    # Vectorized matching - find which niche addresses exist in main
    existing_addresses = niche_df_clean['_NormalizedAddress'].isin(main_address_map.keys())
    
    # Process updates in bulk
    update_addresses = niche_df_clean[existing_addresses]['_NormalizedAddress'].unique()
    
    for address in update_addresses:
        if address in main_address_map:
            main_indices = main_address_map[address]
            
            # Update all main records with this address by setting boolean flag
            for main_idx in main_indices:
                # Set the boolean flag for this niche type
                if not main_df.loc[main_idx, flag_column]:  # Only count if not already set
                    main_df.loc[main_idx, flag_column] = True
                    updates_count += 1
    
    # Process inserts in bulk
    insert_records = niche_df_clean[~existing_addresses].copy()
    
    if len(insert_records) > 0:
        # Create new records DataFrame with boolean flag architecture
        # Only include columns that should be in the enhanced main DataFrame
        new_records = pd.DataFrame({
            'OwnerName': insert_records.get('Owner 1 Last Name', '').astype(str) + ' ' + insert_records.get('Owner 1 First Name', '').astype(str),
            'Address': insert_records.get('Address', ''),
            'Mailing Address': insert_records.get('Mailing Address', ''),
            'Last Sale Date': insert_records.get('Last Sale Date', ''),
            'Last Sale Amount': insert_records.get('Last Sale Amount', ''),
            'Owner 1 Last Name': insert_records.get('Owner 1 Last Name', ''),
            'Owner 1 First Name': insert_records.get('Owner 1 First Name', ''),
            'City': insert_records.get('City', ''),
            'State': insert_records.get('State', ''),
            'Zip': insert_records.get('Zip', ''),
            
            # Property classification  
            'PropertyCategory': 'DEVELOPED',  # Assume developed unless raw land detection done
            
            # Boolean distress flags (all default to False, then set specific one)
            'HasLiens': False,
            'HasForeclosure': False, 
            'HasCodeEnforcement': False,
            'HasCurrentTax': False,
            'HasTaxHistory': False,
            'HasBankruptcy': False,
            'HasCashBuyer': False,
            'HasInterFamily': False,
            'HasLandlord': False,
            'HasProbate': False,
            'HasInherited': False,
            'HasSTBankruptcy': False,
            'HasSTForeclosure': False,
            'HasSTLien': False,
            'HasSTJudgment': False,
            'HasSTQuitclaim': False,
            'HasSTDeceased': False,
            
            # Single priority system (niche-only records get default)
            'PriorityCode': 'DEFAULT',  # Default priority for niche-only records
            'PriorityId': NICHE_ONLY_PRIORITY_ID,
            'PriorityName': f'{niche_type} List Only',
            
            # Processing metadata
            '_NormalizedAddress': insert_records['_NormalizedAddress']
        })
        
        # Set the specific boolean flag for this niche type
        new_records[flag_column] = True
        
        # Concatenate new records to main DataFrame with proper error handling
        try:
            # Validate column compatibility before concatenation
            main_cols = set(main_df.columns)
            new_cols = set(new_records.columns)
            
            # Add any missing columns from main DataFrame to new_records with default values
            missing_cols = main_cols - new_cols - {'_NormalizedAddress'}  # Exclude temp column
            if missing_cols:
                for col in missing_cols:
                    if col.startswith('Has') or col in ['IsTrust', 'IsChurch', 'IsBusiness', 'IsOwnerOccupied', 'OwnerGrantorMatch']:
                        new_records[col] = False
                    elif col == 'ParsedSaleDate':
                        new_records[col] = pd.to_datetime(VERY_OLD_DATE_STR)
                    elif col == 'ParsedSaleAmount':
                        new_records[col] = None
                    else:
                        # For other columns, try to get from original niche data or use empty/default values
                        if col in insert_records.columns:
                            new_records[col] = insert_records[col]
                        else:
                            # Default to empty string for string columns, None for others
                            new_records[col] = '' if new_records.dtypes.get(col, 'object') == 'object' else None
            
            # Remove columns that don't exist in main DataFrame from new_records
            extra_cols = new_cols - main_cols
            if extra_cols:
                logger.debug(f"Removing columns not in main DataFrame: {extra_cols}")
                new_records = new_records.drop(columns=list(extra_cols))
            
            # Now check for any remaining incompatibilities
            new_cols = set(new_records.columns)
            if not new_cols.issubset(main_cols):
                remaining_cols = new_cols - main_cols
                logger.warning(f"New records still have columns not in main DataFrame: {remaining_cols}")
            
            # Perform concatenation with memory and index safety
            main_df = pd.concat([main_df, new_records], ignore_index=True, sort=False)
            inserts_count = len(insert_records)
            
        except pd.errors.OutOfMemoryError:
            logger.error(f"Out of memory during concatenation of {len(insert_records)} records")
            raise MemoryError(f"Insufficient memory to add {len(insert_records)} niche records")
        except Exception as concat_error:
            logger.error(f"Failed to concatenate niche records: {concat_error}")
            raise ValueError(f"Data structure mismatch during concatenation: {concat_error}")
    
    # Clean up temporary column from both DataFrames
    if '_NormalizedAddress' in main_df.columns:
        main_df.drop(columns=['_NormalizedAddress'], inplace=True)
    if '_NormalizedAddress' in niche_df.columns:
        niche_df.drop(columns=['_NormalizedAddress'], inplace=True)
    
    return main_df, updates_count, inserts_count

def process_region(region_key: str, config_manager: MultiRegionConfigManager, auto_clean_fips: bool = False) -> Dict:
    """
    Process a single region's files.
    
    Args:
        region_key: Region identifier (e.g., 'roanoke_city_va')
        config_manager: Configuration manager instance
        
    Returns:
        Dictionary with processing results
    """
    print("=" * 70)
    print(f"PROCESSING REGION: {region_key.upper()}")
    print("=" * 70)
    
    # Get region configuration
    config = config_manager.get_region_config(region_key)
    region_dir = config_manager.get_region_directory(region_key)
    output_dir = config_manager.create_output_directory(region_key)
    region_code = config.region_code.lower()  # Define once for consistent use
    
    # Set up region-specific logging with region name
    log_file = output_dir / f"{region_code}_processing_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    try:
        print(f"Region: {config.region_name}")
        print(f"Market Type: {config.market_type}")
        print(f"ABS1 Date Cutoff: {config.region_input_date1.strftime('%Y-%m-%d')}")
        print(f"BUY Date Cutoff: {config.region_input_date2.strftime('%Y-%m-%d')}")
        print(f"Amount Thresholds: ${config.region_input_amount1:,} / ${config.region_input_amount2:,}")
        print(f"Files Directory: {region_dir}")
        print(f"Output Directory: {output_dir}")
        print()
        
        # Validate region files
        validation = config_manager.validate_region_files(region_key)
        if not validation['valid']:
            print("ERROR: Region validation failed!")
            print(f"  Has Config: {validation['has_config']}")
            print(f"  Has Main File: {validation['has_main_file']}")
            print(f"  Has Excel Files: {validation['has_excel_files']}")
            print(f"  Total Files: {validation['total_files']}")
            return {'success': False, 'error': 'Region validation failed'}
        
        print(f"Region validation passed - found {validation['total_files']} Excel files")
        
        # Validate FIPS codes in all files
        print("Validating FIPS codes...")
        fips_validation = config_manager.validate_fips_codes(region_key)
        
        if not fips_validation['all_valid']:
            print("ERROR: FIPS validation failed!")
            print(f"  Expected FIPS: {fips_validation['region_fips']}")
            print(f"  Files checked: {fips_validation['files_checked']}")
            print(f"  Files valid: {fips_validation['files_valid']}")
            
            if fips_validation['missing_fips_column']:
                print(f"  Missing FIPS column in: {', '.join(fips_validation['missing_fips_column'])}")
            
            if fips_validation['fips_mismatches']:
                print("  FIPS code mismatches:")
                for mismatch in fips_validation['fips_mismatches']:
                    print(f"    {mismatch['file']}: expected {mismatch['expected']}, found {mismatch['found']}")
            
            # Offer cleanup option
            print("\nWould you like to automatically clean the files to remove records with incorrect FIPS codes?")
            print("This will:")
            print("  - Keep only records matching the expected FIPS code")
            print("  - Create backup files with '.backup' extension")
            print("  - Update the original files in place")
            
            if auto_clean_fips:
                print("Auto-clean enabled - cleaning files automatically...")
                response = 'yes'
            else:
                response = input("\nClean files automatically? (y/n): ").lower().strip()
            
            if response in ['y', 'yes']:
                print("\nCleaning files...")
                cleanup_success = _cleanup_fips_mismatches(region_dir, fips_validation['region_fips'], fips_validation['fips_mismatches'])
                
                if cleanup_success:
                    print("Files cleaned successfully! Re-validating...")
                    # Re-validate after cleanup
                    fips_validation = config_manager.validate_fips_codes(region_key)
                    
                    if fips_validation['all_valid']:
                        print(f"SUCCESS: All files now match FIPS {fips_validation['region_fips']}")
                    else:
                        print("ERROR: Cleanup failed - files still contain mismatched FIPS codes")
                        return {'success': False, 'error': 'FIPS cleanup failed'}
                else:
                    print("ERROR: File cleanup failed")
                    return {'success': False, 'error': 'FIPS cleanup failed'}
            else:
                print("Cleanup cancelled by user")
                return {'success': False, 'error': 'FIPS validation failed - files contain wrong region data'}
        
        print(f"FIPS validation passed - all {fips_validation['files_checked']} files match region {fips_validation['region_fips']}")
        
        # Find Excel files
        excel_files = list(region_dir.glob("*.xlsx"))
        
        # Find main region file (largest or specifically named)
        main_file = None
        for excel_file in excel_files:
            if 'main_region' in excel_file.name.lower():
                main_file = excel_file
                break
        
        if main_file is None:
            # Fall back to largest file
            main_file = max(excel_files, key=lambda f: f.stat().st_size)
        
        # Find recent sales files
        recent_sales_files = [f for f in excel_files if 'recent' in f.name.lower() and 'sales' in f.name.lower()]
        
        # 1. MERGE RECENT SALES WITH MAIN FILE (if any)
        if recent_sales_files:
            print("\\nSTEP 1: Merging Recent Sales with Main File")
            print("-" * 50)
            
            # Load main file
            print(f"Loading main file: {main_file.name} ({main_file.stat().st_size:,} bytes)")
            main_df = pd.read_excel(main_file)
            print(f"Main file loaded: {len(main_df):,} records")
            
            total_added = 0
            for recent_file in recent_sales_files:
                try:
                    print(f"Processing recent sales: {recent_file.name}")
                    recent_df = pd.read_excel(recent_file)
                    
                    if recent_df.empty:
                        print(f"   WARNING: Empty recent sales file: {recent_file.name}")
                        continue
                    
                    print(f"   Loaded {len(recent_df):,} recent sales records")
                    
                    # Merge unique records
                    main_df, added_count = _append_unique_records(main_df, recent_df)
                    total_added += added_count
                    
                    print(f"   SUCCESS: {added_count:,} unique records added from recent sales")
                    
                except Exception as e:
                    print(f"   ERROR: Failed to process recent sales file {recent_file.name}: {e}")
                    logger.error(f"Failed to process recent sales file {recent_file.name}: {e}")
            
            print(f"\\nMERGE SUMMARY:")
            print(f"   Original main file records: {len(main_df) - total_added:,}")
            print(f"   Recent sales records added: {total_added:,}")
            print(f"   Combined dataset size: {len(main_df):,}")
            
            # Create enhanced property processor with region-specific settings
            processor_config = {
                'region_input_date1': config.region_input_date1,
                'region_input_date2': config.region_input_date2,
                'region_input_amount1': config.region_input_amount1,
                'region_input_amount2': config.region_input_amount2
            }
            processor = EnhancedPropertyProcessor(processor_config)
            
            # Process the combined dataset
            print("\\nSTEP 2: Processing Combined Dataset")
            print("-" * 50)
            
            # Save combined dataset to temporary file for processing
            temp_combined_file = region_dir / "temp_combined_main.xlsx"
            try:
                main_df.to_excel(temp_combined_file, index=False)
                main_result = processor.process_excel_file(str(temp_combined_file))
                # Clean up temporary file
                temp_combined_file.unlink()
            except Exception as e:
                # Clean up temporary file if it exists
                if temp_combined_file.exists():
                    temp_combined_file.unlink()
                raise e
            
        else:
            # No recent sales files, process main file normally
            print("\\nSTEP 1: Processing Main Region File")
            print("-" * 50)
            print(f"Processing main file: {main_file.name} ({main_file.stat().st_size:,} bytes)")
            
            # Create enhanced property processor with region-specific settings
            processor_config = {
                'region_input_date1': config.region_input_date1,
                'region_input_date2': config.region_input_date2,
                'region_input_amount1': config.region_input_amount1,
                'region_input_amount2': config.region_input_amount2
            }
            processor = EnhancedPropertyProcessor(processor_config)
            
            # Process main file
            main_result = processor.process_excel_file(str(main_file))
        
        print(f"SUCCESS: Main region processed - {len(main_result):,} records")
        
        # 2. PROCESS NICHE LISTS
        if recent_sales_files:
            print("\\nSTEP 3: Processing Niche Lists (Updating Combined Dataset)")
        else:
            print("\\nSTEP 2: Processing Niche Lists (Updating Main Region)")
        print("-" * 50)
        
        niche_files = [f for f in excel_files if f != main_file and f not in recent_sales_files]
        total_updates = 0
        total_inserts = 0
        
        if niche_files:
            for niche_file in niche_files:
                try:
                    print(f"Processing niche: {niche_file.name}")
                    
                    # Validate niche file
                    if not niche_file.exists() or niche_file.stat().st_size == 0:
                        print(f"   WARNING: Skipping empty or missing file: {niche_file.name}")
                        continue
                    
                    # Determine niche type from filename
                    niche_type = _detect_niche_type_from_filename(str(niche_file.name))
                    
                    # Read niche file with validation and memory optimization
                    try:
                        niche_df = pd.read_excel(niche_file, dtype={'FIPS': 'category'})
                        
                        # Optimize memory usage for niche files with safety limits
                        protected_columns = {'Owner 1 Last Name', 'Owner 1 First Name', 'Address', 'Mailing Address'}
                        string_columns = niche_df.select_dtypes(include=['object']).columns
                        
                        # Apply safe dtype optimization with limits
                        for col in string_columns:
                            if col not in protected_columns:
                                try:
                                    unique_ratio = niche_df[col].nunique() / len(niche_df)
                                    max_categories = niche_df[col].nunique()
                                    
                                    # Safety checks before category conversion
                                    if (unique_ratio < 0.5 and 
                                        max_categories < 10000 and  # Prevent excessive category creation
                                        niche_df[col].memory_usage(deep=True) > 1024 * 1024):  # Only optimize if >1MB
                                        
                                        niche_df[col] = niche_df[col].astype('category')
                                        logger.debug(f"Converted column '{col}' to category (unique_ratio={unique_ratio:.3f}, categories={max_categories})")
                                    
                                except Exception as dtype_error:
                                    logger.warning(f"Failed to optimize column '{col}': {dtype_error}")
                                    # Continue without optimization for this column
                                
                    except Exception as read_error:
                        print(f"   ERROR: Cannot read {niche_file.name}: {read_error}")
                        logger.error(f"Cannot read {niche_file.name}: {read_error}")
                        continue
                    
                    if niche_df.empty:
                        print(f"   WARNING: Empty niche file: {niche_file.name}")
                        continue
                        
                    print(f"   Loaded {len(niche_df):,} niche records")
                    
                    # Update main region with niche data
                    try:
                        main_result, updates, inserts = _update_main_with_niche(main_result, niche_df, niche_type)
                        
                        total_updates += updates
                        total_inserts += inserts
                        
                        print(f"   SUCCESS: {niche_type}: {updates:,} updated, {inserts:,} inserted")
                    except Exception as update_error:
                        print(f"   ERROR: Failed to process {niche_type} data: {update_error}")
                        logger.error(f"Failed to process {niche_type} data from {niche_file.name}: {update_error}")
                    
                except Exception as e:
                    print(f"   ERROR: Unexpected error processing {niche_file.name}: {e}")
                    logger.error(f"Unexpected error processing {niche_file.name}: {e}")
                    
            print(f"\\nNICHE PROCESSING SUMMARY:")
            print(f"   Total Updated Records: {total_updates:,}")
            print(f"   Total Inserted Records: {total_inserts:,}")
            print(f"   Final Record Count: {len(main_result):,}")
            
        else:
            print("No niche files found")
        
        # 3. SAVE RESULTS
        print("\\nSTEP 3: Saving Results")
        print("-" * 50)
        
        # Save enhanced main region file with region name
        try:
            main_output = output_dir / f"{region_code}_main_region_enhanced_{datetime.now().strftime('%Y%m%d')}.xlsx"
            main_result.to_excel(main_output, index=False)
            print(f"Enhanced main region saved: {main_output.name}")
        except Exception as e:
            error_msg = f"Failed to save main output file: {e}"
            print(f"ERROR: {error_msg}")
            logger.error(error_msg)
            return {'success': False, 'error': error_msg}
        
        # Save optional summary report with region name
        summary_output = output_dir / f"{region_code}_processing_summary_{datetime.now().strftime('%Y%m%d')}.xlsx"
        
        # Generate summary report
        summary_data = {
            'region_name': config.region_name,
            'region_code': config.region_code,
            'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'total_records': len(main_result),
            'original_records': len(main_result) - total_inserts,
            'updated_records': total_updates,
            'inserted_records': total_inserts,
            'niche_files_processed': len(niche_files)
        }
        
        # Priority distribution
        priority_dist = main_result['PriorityCode'].value_counts().head(10)
        
        # Create summary report DataFrame
        priority_data = []
        for priority, count in priority_dist.items():
            pct = (count / len(main_result)) * 100
            priority_data.append({
                'Priority_Code': priority,
                'Count': count,
                'Percentage': round(pct, 1)
            })
        
        # Save summary report  
        try:
            with pd.ExcelWriter(summary_output) as writer:
                # Summary sheet
                pd.DataFrame([summary_data]).to_excel(writer, sheet_name='Summary', index=False)
                # Priority distribution sheet
                pd.DataFrame(priority_data).to_excel(writer, sheet_name='Priority_Distribution', index=False)
            
            print(f"Summary report saved: {summary_output.name}")
        except Exception as e:
            print(f"Warning: Could not save summary report: {e}")
        
        print("\\nFINAL SUMMARY")
        print("=" * 70)
        print(f"Region: {config.region_name}")
        print(f"Total Records: {len(main_result):,}")
        print(f"Updated with Niche Data: {total_updates:,}")
        print(f"New from Niche Lists: {total_inserts:,}")
        
        print(f"\\nTOP PRIORITY CODES:")
        for priority, count in priority_dist.items():
            pct = (count / len(main_result)) * 100
            print(f"   {priority}: {count:,} ({pct:.1f}%)")
        
        print(f"\\nOutput saved to: {output_dir}")
        print("=" * 70)
        
        # Clean up logging handler
        logger.removeHandler(file_handler)
        file_handler.close()
        
        return {
            'success': True,
            'region_name': config.region_name,
            'total_records': len(main_result),
            'updated_records': total_updates,
            'inserted_records': total_inserts,
            'output_file': str(main_output)
        }
        
    except Exception as e:
        logger.error(f"Processing failed for {region_key}: {e}")
        print(f"\\nERROR: Processing failed - {e}")
        
        # Clean up logging handler
        logger.removeHandler(file_handler)
        file_handler.close()
        
        return {'success': False, 'error': str(e)}

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Multi-Region Real Estate Direct Mail Processor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python monthly_processing_v2.py --region roanoke_city_va
  python monthly_processing_v2.py --all-regions  
  python monthly_processing_v2.py --list-regions
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--region", help="Process specific region (e.g., roanoke_city_va)")
    group.add_argument("--all-regions", action="store_true", help="Process all regions")
    group.add_argument("--list-regions", action="store_true", help="List available regions")
    
    parser.add_argument("--auto-clean-fips", action="store_true", help="Automatically clean files with FIPS mismatches")
    
    args = parser.parse_args()
    
    try:
        # Initialize configuration manager
        config_manager = MultiRegionConfigManager()
        
        if args.list_regions:
            print("\\n=== AVAILABLE REGIONS ===")
            print(f"{'CODE':<6} | {'REGION NAME':<25} | {'MARKET TYPE':<18} | {'DESCRIPTION'}")
            print("-" * 80)
            
            for region in config_manager.list_regions():
                print(f"{region['code']:<6} | {region['name']:<25} | {region['market_type']:<18} | {region['description']}")
            
            print(f"\\nTotal regions configured: {len(config_manager.configs)}")
            
        elif args.region:
            # Process single region
            result = process_region(args.region, config_manager, args.auto_clean_fips)
            
            if result['success']:
                print("\\n[SUCCESS] Processing completed successfully!")
            else:
                print(f"\\n[ERROR] Processing failed: {result.get('error', 'Unknown error')}")
                exit(1)
                
        elif args.all_regions:
            # Process all regions
            print("\\n[BATCH] PROCESSING ALL REGIONS")
            print("=" * 70)
            
            results = []
            for region_key in config_manager.configs.keys():
                print(f"\\nStarting {region_key}...")
                result = process_region(region_key, config_manager, args.auto_clean_fips)
                results.append(result)
            
            # Summary of all regions
            print("\\n\\n[SUMMARY] BATCH PROCESSING SUMMARY")
            print("=" * 70)
            
            successful = [r for r in results if r.get('success', False)]
            failed = [r for r in results if not r.get('success', False)]
            
            print(f"Successfully processed: {len(successful)} regions")
            print(f"Failed: {len(failed)} regions")
            
            if successful:
                total_records = sum(r.get('total_records', 0) for r in successful)
                print(f"Total records processed: {total_records:,}")
            
            if failed:
                print(f"\\n[FAILED] Failed regions:")
                for result in failed:
                    print(f"  - {result.get('region_name', 'Unknown')}: {result.get('error', 'Unknown error')}")
    
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\\n[ERROR] Application error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
