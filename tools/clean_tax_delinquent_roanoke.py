"""
Roanoke City Tax Delinquent Data Cleaner

Handles Roanoke City's simple tabular format:
Row 1: Account Number | Parcel Id | Account Name | Parcel Address | Amount Due
"""

import argparse
import re
from pathlib import Path
import pandas as pd
from gis_utils import load_gis_data, augment_with_gis


def parse_owner(name: str) -> tuple[str, str]:
    """Parse owner name into last, first components"""
    if not isinstance(name, str):
        return "", ""
    name = name.strip()
    if not name:
        return "", ""
    
    # Handle formats like "LAST, FIRST" or "LAST FIRST"
    if "," in name:
        last, first = name.split(",", 1)
        return last.strip(), first.strip()
    
    # Handle "LAST FIRST MIDDLE" - take last word as last name
    parts = name.split()
    if len(parts) >= 2:
        return parts[-1], " ".join(parts[:-1])
    
    return name, ""


def normalize_address(addr: str) -> str:
    """Normalize address for consistent matching"""
    if not isinstance(addr, str):
        return ""
    return addr.strip().upper()


def clean_roanoke_tax_delinquent(input_path: Path, gis_data: pd.DataFrame = None) -> pd.DataFrame:
    """
    Clean Roanoke City tax delinquent data into standard niche format.
    
    Roanoke format: Simple table with headers in row 1
    - Account Number | Parcel Id | Account Name | Parcel Address | Amount Due
    
    Returns:
        pd.DataFrame: Standard niche format ready for main processor
    """
    # Read with row 1 as headers (skip the title row 0)
    df = pd.read_excel(input_path, header=1, dtype=str)
    
    print(f"Columns found: {list(df.columns)}")
    
    # Verify expected columns exist
    expected_cols = ["Account Number", "Parcel Id", "Account Name", "Parcel Address", "Amount Due"]
    missing_cols = [col for col in expected_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")
    
    records = []
    
    for _, row in df.iterrows():
        account_no = str(row.get("Account Number", "") or "").strip()
        parcel_id = str(row.get("Parcel Id", "") or "").strip()
        account_name = str(row.get("Account Name", "") or "").strip()
        address = str(row.get("Parcel Address", "") or "").strip()
        amount_due = str(row.get("Amount Due", "") or "").strip()
        
        # Skip empty rows
        if not address and not parcel_id:
            continue
        
        # Parse owner name
        last, first = parse_owner(account_name)
        
        # Create standardized record for niche file format
        record = {
            "Parcel ID": parcel_id,
            "Current Owner": account_name,
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
            # Tax delinquent specific fields
            "Account Number": account_no,
            "Amount Due": amount_due,
        }
        
        # Augment with GIS data using shared utility
        record = augment_with_gis(record, parcel_id, gis_data)
        
        records.append(record)
    
    if not records:
        return pd.DataFrame(columns=[
            "Owner 1 Last Name", "Owner 1 First Name", "Address", "City", "State", "Zip",
            "Mailing Address", "Mailing Unit #", "Mailing City", "Mailing State", 
            "Mailing Zip", "Mailing Zip+4", "Last Sale Date", "Last Sale Amount",
            "Parcel ID", "Current Owner", "Account Number", "Amount Due"
        ])
    
    df_clean = pd.DataFrame(records)
    
    # Deduplicate on Address to avoid duplicate updates (keep first occurrence)
    df_clean = df_clean.drop_duplicates(subset=["Address"]).reset_index(drop=True)
    
    return df_clean


def main():
    parser = argparse.ArgumentParser(description="Clean Roanoke City Tax Delinquent data into niche format")
    parser.add_argument("--input", required=True, help="Path to tax delinquent Excel file")
    parser.add_argument("--region", required=True, help="Region key (should be roanoke_city_va)")
    parser.add_argument("--date", help="YYYYMMDD for output filename; if omitted, attempts to infer from input name")
    parser.add_argument("--gis-file", help="Path to GIS parcel data CSV file for augmentation")
    parser.add_argument("--no-gis", action="store_true", help="Skip GIS augmentation even if file exists")
    
    args = parser.parse_args()
    
    if args.region != "roanoke_city_va":
        raise SystemExit(f"This cleaner is for roanoke_city_va only, got: {args.region}")
    
    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")
    
    # Infer date from filename
    output_date = args.date
    if not output_date:
        date_match = re.search(r"(\d{1,2})[-_/](\d{1,2})[-_/](\d{2,4})", input_path.name)
        if date_match:
            mm, dd, yyyy = date_match.groups()
            yyyy = ("20" + yyyy) if len(yyyy) == 2 else yyyy
            output_date = f"{yyyy}{int(mm):02d}{int(dd):02d}"
        else:
            output_date = "unknown"
    
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
    
    # Clean the data with optional GIS augmentation
    print(f"\nProcessing Roanoke City tax delinquent data...")
    df_clean = clean_roanoke_tax_delinquent(input_path, gis_data)
    
    # Report augmentation statistics
    if gis_data is not None:
        gis_augmented = df_clean['Data_Source'].str.contains('GIS_Augmented', na=False).sum()
        print(f"GIS augmentation: {gis_augmented}/{len(df_clean)} records enhanced ({gis_augmented/len(df_clean)*100:.1f}%)")
    
    # Save to region directory
    region_dir = Path("regions") / args.region
    region_dir.mkdir(parents=True, exist_ok=True)
    output_path = region_dir / f"{args.region}_tax_delinquent_{output_date}.xlsx"
    df_clean.to_excel(output_path, index=False)
    
    print(f"\nProcessed {len(df_clean)} unique tax delinquent records")
    print(f"Saved niche file: {output_path}")
    
    # Show sample of augmented data
    if gis_data is not None and df_clean['Data_Source'].str.contains('GIS_Augmented', na=False).sum() > 0:
        print(f"\nSample of GIS-augmented records:")
        augmented_sample = df_clean[df_clean['Data_Source'] == 'GIS_Augmented'].head(3)
        for i, (_, row) in enumerate(augmented_sample.iterrows()):
            print(f"  {i+1}. {row['Address']} | Owner: {row['Current Owner'][:30]} | Due: ${row['Amount Due']}")
            print(f"     Mail: {row['Mailing Address'][:30]} | Sale: {row['Last Sale Date']} {row['Last Sale Amount']}")
    else:
        print(f"\nSample records:")
        for i, (_, row) in enumerate(df_clean.head(3).iterrows()):
            print(f"  {i+1}. {row['Address']} | Owner: {row['Current Owner'][:30]} | Due: ${row['Amount Due']}")


if __name__ == "__main__":
    main()