"""
Monthly Real Estate Direct Mail Processing Script

This script processes your monthly property files with the correct region settings.
Customize the configuration at the top for your specific region and requirements.

Usage:
    python monthly_processing.py
"""

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from property_processor import PropertyProcessor
from niche_processor import NicheListProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'monthly_processing_{datetime.now().strftime("%Y%m")}.log'),
        logging.StreamHandler()
    ]
)

# =============================================================================
# CONFIGURATION - CUSTOMIZE THESE FOR YOUR REGION
# =============================================================================

# Region-specific date and amount criteria
# These were stored in your SQL Region table
REGION_CONFIG = {
    'region_name': 'Roanoke City, VA',
    
    # Date criteria (IMPORTANT - these control which properties go to which lists)
    'region_input_date1': datetime(2009, 1, 1),    # ABS1: Properties sold before this = old sales  
    'region_input_date2': datetime(2019, 1, 1),    # BUY1/BUY2: Properties sold after this = recent buyers
    
    # Amount thresholds
    'region_input_amount1': 75000,                  # Low threshold (TRS1, OON1)
    'region_input_amount2': 200000,                 # High threshold (BUY1, BUY2 - cash buyers)
}

# File paths
EXCEL_DIR = "Excel files"
OUTPUT_DIR = f"output/monthly_{datetime.now().strftime('%Y_%m')}"

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
    elif 'tax' in filename_lower and 'delinq' in filename_lower:
        return 'Tax'
    elif 'probate' in filename_lower:
        return 'Probate'
    elif 'interfamily' in filename_lower or 'family' in filename_lower:
        return 'InterFamily'
    elif 'cash' in filename_lower and 'buyer' in filename_lower:
        return 'CashBuyer'
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
    import re
    addr = re.sub(r'\s+', ' ', addr)
    
    return addr

def _update_main_with_niche(main_df: pd.DataFrame, niche_df: pd.DataFrame, niche_type: str) -> tuple:
    """
    Update main region DataFrame with niche data.
    
    Returns:
        tuple: (updates_count, inserts_count)
    """
    updates_count = 0
    inserts_count = 0
    
    # Normalize addresses for matching
    main_df['_NormalizedAddress'] = main_df['Address'].apply(_normalize_address)
    niche_df['_NormalizedAddress'] = niche_df['Address'].apply(_normalize_address)
    
    # Create a set of main region addresses for fast lookup
    main_addresses = set(main_df['_NormalizedAddress'])
    
    for idx, niche_row in niche_df.iterrows():
        niche_address = niche_row['_NormalizedAddress']
        
        if niche_address == '':
            continue  # Skip blank addresses
            
        if niche_address in main_addresses:
            # UPDATE: Append niche type to existing priority code
            mask = main_df['_NormalizedAddress'] == niche_address
            
            # Update priority codes for all matching addresses
            for main_idx in main_df[mask].index:
                current_priority = main_df.loc[main_idx, 'PriorityCode']
                
                # Append niche type if not already present
                if niche_type not in current_priority:
                    main_df.loc[main_idx, 'PriorityCode'] = f"{niche_type}-{current_priority}"
                    main_df.loc[main_idx, 'PriorityName'] = f"{niche_type} Enhanced - {main_df.loc[main_idx, 'PriorityName']}"
                    updates_count += 1
        else:
            # INSERT: Add new record with niche type as priority
            new_record = {
                'OwnerName': niche_row.get('Owner 1 Last Name', '') + ' ' + niche_row.get('Owner 1 First Name', ''),
                'Address': niche_row.get('Address', ''),
                'Mailing Address': niche_row.get('Mailing Address', ''),
                'Last Sale Date': niche_row.get('Last Sale Date', ''),
                'Last Sale Amount': niche_row.get('Last Sale Amount', ''),
                'Owner 1 Last Name': niche_row.get('Owner 1 Last Name', ''),
                'Owner 1 First Name': niche_row.get('Owner 1 First Name', ''),
                'City': niche_row.get('City', ''),
                'State': niche_row.get('State', ''),
                'Zip': niche_row.get('Zip', ''),
                
                # Classification flags (new records default to False)
                'IsTrust': False,
                'IsChurch': False,
                'IsBusiness': False,
                'IsOwnerOccupied': False,
                'OwnerGrantorMatch': False,
                
                # Priority information 
                'PriorityId': 99,  # Special ID for niche-only records
                'PriorityCode': niche_type,
                'PriorityName': f'{niche_type} List Only',
                
                # Processing metadata
                'ParsedSaleDate': pd.to_datetime('1850-01-01'),  # Default very old date
                'ParsedSaleAmount': None,
                '_NormalizedAddress': niche_address
            }
            
            # Add the new record to main DataFrame using loc
            new_idx = len(main_df)
            for key, value in new_record.items():
                main_df.loc[new_idx, key] = value
            inserts_count += 1
    
    # Clean up temporary column
    main_df.drop(columns=['_NormalizedAddress'], inplace=True)
    
    return updates_count, inserts_count

def main():
    """Main monthly processing workflow"""
    print("="*60)
    print("MONTHLY REAL ESTATE DIRECT MAIL PROCESSING")
    print("="*60)
    print(f"Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Region: {REGION_CONFIG['region_name']}")
    print(f"ABS1 Date Cutoff: {REGION_CONFIG['region_input_date1'].strftime('%Y-%m-%d')}")
    print(f"BUY1/BUY2 Date Cutoff: {REGION_CONFIG['region_input_date2'].strftime('%Y-%m-%d')}")
    print(f"Amount Thresholds: ${REGION_CONFIG['region_input_amount1']:,} / ${REGION_CONFIG['region_input_amount2']:,}")
    print()
    
    # Create output directory
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. PROCESS MAIN REGION FILE
        print("STEP 1: Processing Main Region File")
        print("-" * 40)
        
        processor = PropertyProcessor(
            region_input_date1=REGION_CONFIG['region_input_date1'],
            region_input_date2=REGION_CONFIG['region_input_date2'],
            region_input_amount1=REGION_CONFIG['region_input_amount1'],
            region_input_amount2=REGION_CONFIG['region_input_amount2']
        )
        
        # Find main region file (largest Excel file)
        excel_files = list(Path(EXCEL_DIR).glob("*.xlsx"))
        if not excel_files:
            print(f"ERROR: No Excel files found in {EXCEL_DIR}")
            return
        
        main_file = max(excel_files, key=lambda f: f.stat().st_size)
        print(f"Processing: {main_file.name} ({main_file.stat().st_size:,} bytes)")
        
        # Process main file
        main_result = processor.process_excel_file(str(main_file))
        
        # Save main results
        main_output = Path(OUTPUT_DIR) / f"main_region_processed_{datetime.now().strftime('%Y%m%d')}.xlsx"
        main_result.to_excel(main_output, index=False)
        
        print(f"SUCCESS: Main region processed: {len(main_result):,} records")
        print(f"   Saved to: {main_output}")
        
        # 2. PROCESS NICHE LISTS - UPDATE/INSERT INTO MAIN REGION
        print("\nSTEP 2: Processing Niche Lists (Updating Main Region)")
        print("-" * 40)
        
        # Process each niche file (excluding main file)
        niche_files = [f for f in excel_files if f != main_file]
        
        total_updates = 0
        total_inserts = 0
        
        if niche_files:
            for niche_file in niche_files:
                try:
                    print(f"Processing niche: {niche_file.name}")
                    
                    # Determine niche type from filename
                    niche_type = _detect_niche_type_from_filename(str(niche_file.name))
                    
                    # Read niche file
                    niche_df = pd.read_excel(niche_file)
                    print(f"   Loaded {len(niche_df):,} niche records")
                    
                    # Update main region with niche data
                    updates, inserts = _update_main_with_niche(main_result, niche_df, niche_type)
                    
                    total_updates += updates
                    total_inserts += inserts
                    
                    print(f"   SUCCESS: {niche_type}: {updates:,} updated, {inserts:,} inserted")
                    
                except Exception as e:
                    print(f"   ERROR: Error processing {niche_file.name}: {e}")
                    
            print(f"\nNICHE PROCESSING SUMMARY:")
            print(f"   Total Updated Records: {total_updates:,}")
            print(f"   Total Inserted Records: {total_inserts:,}")
            print(f"   Final Main Region Records: {len(main_result):,}")
            
        else:
            print("No niche files found (only main region file)")
            
        # Save updated main region results
        main_output = Path(OUTPUT_DIR) / f"main_region_with_niches_{datetime.now().strftime('%Y%m%d')}.xlsx"
        main_result.to_excel(main_output, index=False)
        print(f"\nFinal enhanced main region saved to: {main_output}")
        
        # 3. GENERATE SUMMARY REPORT
        print("\nSTEP 3: Generating Summary Report")
        print("-" * 40)
        
        # Main region summary
        print(f"MAIN REGION SUMMARY ({REGION_CONFIG['region_name']})")
        print(f"   Total Records: {len(main_result):,}")
        print(f"   Owner Occupied: {main_result['IsOwnerOccupied'].sum():,} ({main_result['IsOwnerOccupied'].mean()*100:.1f}%)")
        print(f"   Trusts: {main_result['IsTrust'].sum():,}")
        print(f"   Churches: {main_result['IsChurch'].sum():,}")
        print(f"   Businesses: {main_result['IsBusiness'].sum():,}")
        
        # Priority distribution
        print(f"\nPRIORITY DISTRIBUTION:")
        priority_dist = main_result['PriorityName'].value_counts().head(8)
        for priority, count in priority_dist.items():
            pct = (count / len(main_result)) * 100
            print(f"   {priority}: {count:,} ({pct:.1f}%)")
        
        # Data quality
        print(f"\nDATA QUALITY:")
        valid_dates = main_result['ParsedSaleDate'].notna().sum()
        valid_amounts = main_result['ParsedSaleAmount'].notna().sum()
        print(f"   Valid Sale Dates: {valid_dates:,} ({valid_dates/len(main_result)*100:.1f}%)")
        print(f"   Valid Sale Amounts: {valid_amounts:,} ({valid_amounts/len(main_result)*100:.1f}%)")
        
        # Niche integration summary (data now integrated into main region)
        
        # 4. FINAL SUMMARY
        total_all_records = len(main_result)
        
        print(f"\nPROCESSING COMPLETE!")
        print("="*60)
        print(f"TOTAL RECORDS PROCESSED: {total_all_records:,}")
        print(f"OUTPUT DIRECTORY: {OUTPUT_DIR}")
        print(f"LOG FILE: monthly_processing_{datetime.now().strftime('%Y%m')}.log")
        print(f"PROCESSING TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        # Show key files created
        print(f"\nFILES CREATED:")
        for file_path in Path(OUTPUT_DIR).glob("*.xlsx"):
            print(f"   - {file_path.name}")
        
        print(f"\nMonthly processing completed successfully!")
        
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        print(f"\nERROR: Processing failed - {e}")
        raise

def validate_configuration():
    """Validate the configuration before processing"""
    print("VALIDATING CONFIGURATION...")
    
    # Check date logic
    if REGION_CONFIG['region_input_date1'] >= REGION_CONFIG['region_input_date2']:
        print("WARNING: region_input_date1 should be older than region_input_date2")
    
    # Check amount logic
    if REGION_CONFIG['region_input_amount1'] >= REGION_CONFIG['region_input_amount2']:
        print("WARNING: region_input_amount1 should be less than region_input_amount2")
    
    # Check directory exists
    if not Path(EXCEL_DIR).exists():
        print(f"ERROR: Excel directory not found: {EXCEL_DIR}")
        return False
    
    # Check for Excel files
    excel_files = list(Path(EXCEL_DIR).glob("*.xlsx"))
    if not excel_files:
        print(f"ERROR: No Excel files found in {EXCEL_DIR}")
        return False
    
    print(f"Configuration valid - found {len(excel_files)} Excel files")
    return True

if __name__ == "__main__":
    # Validate configuration first
    if not validate_configuration():
        exit(1)
    
    # Run main processing
    main()