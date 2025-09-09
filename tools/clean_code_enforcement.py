import argparse
import re
from pathlib import Path
import pandas as pd


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


def parse_owner(name: str) -> tuple[str, str]:
    if not isinstance(name, str):
        return "", ""
    name = name.strip()
    if not name:
        return "", ""
    # Handle formats like "LAST FIRST" or "LAST, FIRST" 
    if "," in name:
        last, first = name.split(",", 1)
        return last.strip(), first.strip()
    # Handle formats like "LAST FIRST MIDDLE" - take last word as last name
    parts = name.split()
    if len(parts) >= 2:
        return parts[-1], " ".join(parts[:-1])
    return name, ""


def normalize_address(addr: str) -> str:
    if not isinstance(addr, str):
        return ""
    return addr.strip().upper()


def clean_code_enforcement_excel(input_path: Path, gis_data: pd.DataFrame = None) -> pd.DataFrame:
    # Read Excel file with headers in first row
    df = pd.read_excel(input_path, dtype=str)
    
    # Verify expected columns exist
    expected_cols = ["CASE NO", "PARCEL NO", "SITE ADDRESS", "OWNER NAME"]
    missing_cols = [col for col in expected_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")
    
    records = []
    
    for _, row in df.iterrows():
        case_no = str(row.get("CASE NO", "") or "").strip()
        parcel_id = str(row.get("PARCEL NO", "") or "").strip()
        address = str(row.get("SITE ADDRESS", "") or "").strip()
        owner = str(row.get("OWNER NAME", "") or "").strip()
        case_type = str(row.get("CASE TYPE", "") or "").strip()
        status = str(row.get("STATUS", "") or "").strip()
        
        # Skip empty rows
        if not address and not parcel_id:
            continue
        
        # Parse owner name
        last, first = parse_owner(owner)
        
        # Create standardized record for niche file format
        record = {
            "Parcel ID": parcel_id,
            "Current Owner": owner,
            "Owner 1 Last Name": last,
            "Owner 1 First Name": first,
            "Address": normalize_address(address),
            "City": "",  # Will be augmented from GIS
            "State": "",  # Will be augmented from GIS
            "Zip": "",   # Will be augmented from GIS
            "Last Sale Date": "",  # Will be augmented from GIS
            "Last Sale Amount": "",  # Will be augmented from GIS
            "Mailing Address": "",  # Will be augmented from GIS
            "Mailing Unit #": "",
            "Mailing City": "",
            "Mailing State": "",
            "Mailing Zip": "",
            "Mailing Zip+4": "",
            # Code enforcement specific fields
            "Case Number": case_no,
            "Case Type": case_type,
            "Status": status,
        }
        
        # Augment with GIS data if available
        if gis_data is not None and parcel_id:
            gis_match = gis_data[gis_data['_ParcelKey'] == parcel_id]
            if len(gis_match) > 0:
                gis_row = gis_match.iloc[0]
                gis_fields = extract_gis_data(gis_row)
                record.update(gis_fields)
                record['Data_Source'] = 'GIS_Augmented'
            else:
                record['Data_Source'] = 'Code_Enforcement_Only'
        else:
            record['Data_Source'] = 'Code_Enforcement_Only'
        
        records.append(record)
    
    if not records:
        return pd.DataFrame(columns=[
            "Owner 1 Last Name", "Owner 1 First Name", "Address", "City", "State", "Zip",
            "Mailing Address", "Mailing Unit #", "Mailing City", "Mailing State", 
            "Mailing Zip", "Mailing Zip+4", "Last Sale Date", "Last Sale Amount",
            "Parcel ID", "Current Owner", "Case Number", "Case Type", "Status"
        ])
    
    df_clean = pd.DataFrame(records)
    
    # Deduplicate on Address to avoid duplicate updates (keep first occurrence)
    df_clean = df_clean.drop_duplicates(subset=["Address"]).reset_index(drop=True)
    
    return df_clean


def main():
    parser = argparse.ArgumentParser(description="Clean Code Enforcement data into niche format")
    parser.add_argument("--input", required=True, help="Path to code enforcement Excel file")
    parser.add_argument("--region", required=True, help="Region key (e.g., roanoke_city_va)")
    parser.add_argument("--date", help="YYYYMMDD for output filename; if omitted, attempts to infer from input name")
    parser.add_argument("--gis-file", help="Path to GIS parcel data CSV file for augmentation")
    parser.add_argument("--no-gis", action="store_true", help="Skip GIS augmentation even if file exists")
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")
    
    # Load GIS data for augmentation
    gis_data = None
    if not args.no_gis:
        # Try specified GIS file or default location
        gis_file = Path(args.gis_file) if args.gis_file else Path("ParcelsRoanokeCity.csv")
        
        if gis_file.exists():
            try:
                gis_data = load_gis_data(gis_file)
                print(f"GIS augmentation enabled with {len(gis_data):,} parcels")
            except Exception as e:
                print(f"Warning: Could not load GIS data: {e}")
                print("Proceeding without GIS augmentation...")
        else:
            print(f"GIS file not found at {gis_file}. Proceeding without augmentation.")
    else:
        print("GIS augmentation disabled by --no-gis flag")
    
    # Infer date from filename like "2-25-2025 to 6-25-2025"
    output_date = args.date
    if not output_date:
        date_match = re.search(r"(\d{1,2})[-_/](\d{1,2})[-_/](\d{2,4})", input_path.name)
        if date_match:
            mm, dd, yyyy = date_match.groups()
            yyyy = ("20" + yyyy) if len(yyyy) == 2 else yyyy
            output_date = f"{yyyy}{int(mm):02d}{int(dd):02d}"
        else:
            output_date = "unknown"
    
    # Clean the data with optional GIS augmentation
    print(f"\nProcessing code enforcement data...")
    df_clean = clean_code_enforcement_excel(input_path, gis_data)
    
    # Report augmentation statistics
    if gis_data is not None:
        gis_augmented = df_clean['Data_Source'].str.contains('GIS_Augmented', na=False).sum()
        print(f"GIS augmentation: {gis_augmented}/{len(df_clean)} records enhanced ({gis_augmented/len(df_clean)*100:.1f}%)")
    
    # Save to region directory
    region_dir = Path("regions") / args.region
    region_dir.mkdir(parents=True, exist_ok=True)
    output_path = region_dir / f"{args.region}_code_enforcement_{output_date}.xlsx"
    df_clean.to_excel(output_path, index=False)
    
    print(f"\nProcessed {len(df_clean)} unique code enforcement records")
    print(f"Saved augmented niche file: {output_path}")
    
    # Show sample of augmented data
    if gis_data is not None and gis_augmented > 0:
        print(f"\nSample of GIS-augmented records:")
        augmented_sample = df_clean[df_clean['Data_Source'] == 'GIS_Augmented'].head(3)
        for i, (_, row) in enumerate(augmented_sample.iterrows()):
            print(f"  {i+1}. {row['Address']} | Owner: {row['Current Owner'][:30]} | Mail: {row['Mailing Address'][:30]}")
            print(f"     Sale: {row['Last Sale Date']} {row['Last Sale Amount']} | Assessed: ${row.get('Total Assessed Value', 'N/A')}")


if __name__ == "__main__":
    main()