"""
Shared GIS utilities for government data processing
"""
import pandas as pd
from pathlib import Path


def load_gis_data(gis_file_path: Path) -> pd.DataFrame:
    """Load and prepare GIS parcel data for augmentation"""
    if not gis_file_path.exists():
        raise FileNotFoundError(f"GIS file not found: {gis_file_path}")
    
    gis_df = pd.read_csv(gis_file_path)
    
    # Standardize key columns for matching
    gis_df['_ParcelKey'] = gis_df['TAXID'].astype(str)
    
    print(f"Loaded GIS data: {len(gis_df):,} parcels")
    return gis_df


def extract_gis_data(gis_row) -> dict:
    """Extract relevant data from GIS record for niche format"""
    
    # Parse sale dates (handle various formats)
    def parse_sale_date(date_str):
        if pd.isna(date_str) or str(date_str) in ['1776/07/04 00:00:01+00', '1900/01/01 00:00:01+00']:
            return ""
        try:
            # Handle various date formats from GIS
            date_clean = str(date_str).split(' ')[0]  # Remove time component
            if '/' in date_clean and not date_clean.startswith('1776') and not date_clean.startswith('1900'):
                return date_clean
        except:
            pass
        return ""
    
    # Parse sale amounts
    def parse_sale_amount(amount):
        if pd.isna(amount) or amount == 0.0:
            return ""
        return f"${amount:,.0f}"
    
    # Extract mailing zip (handle different formats)
    def format_zip(zip_val):
        if pd.isna(zip_val):
            return ""
        zip_str = str(zip_val).strip()
        if len(zip_str) == 5 and zip_str.isdigit():
            return zip_str
        elif len(zip_str) == 4 and zip_str.isdigit():
            return f"0{zip_str}"  # Add leading zero for 4-digit zips
        return zip_str
    
    return {
        'City': 'ROANOKE',  # All Roanoke City parcels
        'State': 'VA',
        'Zip': '',  # GIS doesn't have property zip consistently
        'Last Sale Date': parse_sale_date(gis_row.get('SALEDATE1', '')),
        'Last Sale Amount': parse_sale_amount(gis_row.get('SALEAMT1', 0)),
        'Mailing Address': str(gis_row.get('OWNERADDR1', '')).strip(),
        'Mailing Unit #': '',  # Would need parsing from OWNERADDR1
        'Mailing City': str(gis_row.get('MAILCITY', '')).strip(),
        'Mailing State': str(gis_row.get('MAILSTATE', '')).strip(),
        'Mailing Zip': format_zip(gis_row.get('MAINZIPCOD', '')),
        'Mailing Zip+4': '',  # Not available in this GIS data
        # Additional GIS data for analysis
        'Property Type': str(gis_row.get('PROPERTYDE', '')).strip(),
        'Total Assessed Value': gis_row.get('TOTALVAL1', ''),
        'Land Value': gis_row.get('LANDVAL1', ''),
        'Building Value': gis_row.get('DWELLINGVA', ''),
        'Square Feet': gis_row.get('SQFT', ''),
        'Acres': gis_row.get('ACRES', ''),
        'Zone Description': str(gis_row.get('ZONEDESC', '')).strip(),
        'Legal Description': str(gis_row.get('LEGALDESC', '')).strip()[:100],  # Truncate long descriptions
    }


def normalize_address_for_matching(addr: str) -> str:
    """Normalize address for consistent matching between datasets"""
    if not isinstance(addr, str):
        return ""
    # Convert to uppercase and strip whitespace
    addr = addr.strip().upper()
    # Standardize common abbreviations
    addr = addr.replace(' AVENUE ', ' AVE ')
    addr = addr.replace(' STREET ', ' ST ')
    addr = addr.replace(' ROAD ', ' RD ')
    addr = addr.replace(' DRIVE ', ' DR ')
    addr = addr.replace(' COURT ', ' CT ')
    addr = addr.replace(' PLACE ', ' PL ')
    return addr


def augment_with_gis(record: dict, parcel_id: str, gis_data: pd.DataFrame) -> dict:
    """
    Augment a single record with GIS data using hybrid matching strategy:
    1. Primary: Parcel ID matching (most accurate)
    2. Backup: Address matching (handles parcel ID format mismatches)
    
    Args:
        record: Base record dictionary to augment
        parcel_id: Parcel ID to match against GIS data
        gis_data: GIS DataFrame with _ParcelKey column and LOCADDR column
        
    Returns:
        Updated record with GIS data and Data_Source field
    """
    gis_match = None
    match_method = ""
    
    if gis_data is not None:
        # Strategy 1: Try parcel ID matching first (most accurate)
        if parcel_id:
            gis_match = gis_data[gis_data['_ParcelKey'] == parcel_id]
            if len(gis_match) > 0:
                match_method = "Parcel_ID"
            else:
                # Strategy 1b: Try parcel ID without suffix (handle A/B/C suffixes)
                base_parcel = parcel_id.rstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
                if base_parcel != parcel_id:
                    gis_match = gis_data[gis_data['_ParcelKey'] == base_parcel]
                    if len(gis_match) > 0:
                        match_method = "Parcel_ID_Base"
        
        # Strategy 2: Fallback to address matching if parcel ID failed
        if (gis_match is None or len(gis_match) == 0) and record.get('Address'):
            normalized_input_addr = normalize_address_for_matching(record['Address'])
            
            # Create normalized address column if it doesn't exist
            if '_NormalizedAddr' not in gis_data.columns:
                gis_data['_NormalizedAddr'] = gis_data['LOCADDR'].apply(normalize_address_for_matching)
            
            # Find address matches
            gis_match = gis_data[gis_data['_NormalizedAddr'] == normalized_input_addr]
            if len(gis_match) > 0:
                match_method = "Address"
    
    # Apply GIS augmentation if match found
    if gis_match is not None and len(gis_match) > 0:
        gis_row = gis_match.iloc[0]
        gis_fields = extract_gis_data(gis_row)
        record.update(gis_fields)
        record['Data_Source'] = f'GIS_Augmented_{match_method}'
        
        # Take first match if multiple (should be rare with normalized addresses)
        if len(gis_match) > 1:
            record['Data_Source'] += f'_MultiMatch({len(gis_match)})'
    else:
        record['Data_Source'] = 'Government_Data_Only'
    
    return record