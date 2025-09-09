import argparse
import re
from pathlib import Path
import pandas as pd


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


def clean_code_enforcement_excel(input_path: Path) -> pd.DataFrame:
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
            "City": "",  # Property city not available in code enforcement data
            "State": "",  # Property state not available
            "Zip": "",   # Property zip not available
            "Last Sale Date": "",  # Not available in code enforcement data
            "Last Sale Amount": "",  # Not available in code enforcement data
            "Mailing Address": "",  # Not available in code enforcement data
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
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")
    
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
    
    # Clean the data
    df_clean = clean_code_enforcement_excel(input_path)
    
    # Save to region directory
    region_dir = Path("regions") / args.region
    region_dir.mkdir(parents=True, exist_ok=True)
    output_path = region_dir / f"{args.region}_code_enforcement_{output_date}.xlsx"
    df_clean.to_excel(output_path, index=False)
    
    print(f"Processed {len(df_clean)} unique code enforcement records")
    print(f"Saved cleaned niche file: {output_path}")


if __name__ == "__main__":
    main()