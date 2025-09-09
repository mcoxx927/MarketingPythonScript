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
    # Expect formats like "LAST, FIRST ..."; fall back gracefully
    if "," in name:
        last, first = name.split(",", 1)
        return last.strip(), first.strip()
    parts = name.split()
    if len(parts) >= 2:
        return parts[-1], " ".join(parts[:-1])
    return name, ""


money_re = re.compile(r"\$[\d,]+(?:\.\d{2})?")


def extract_money_values(block: str) -> dict:
    if not isinstance(block, str):
        return {"Tax Amount": "", "Penalty Amount": "", "Interest Amount": "", "Total Amount": ""}
    vals = money_re.findall(block)
    # Normalize to plain numbers (keep $ prefixes for readability if preferred)
    result = {"Tax Amount": "", "Penalty Amount": "", "Interest Amount": "", "Total Amount": ""}
    if not vals:
        return result
    # Assign by position with last one as total
    result["Tax Amount"] = vals[0] if len(vals) >= 1 else ""
    result["Penalty Amount"] = vals[1] if len(vals) >= 2 else ""
    result["Interest Amount"] = vals[2] if len(vals) >= 3 else ""
    result["Total Amount"] = vals[-1]
    return result


def split_address_block(block: str) -> tuple[str, str]:
    """Return (location_address, mailing_address_full)."""
    if not isinstance(block, str):
        return "", ""
    # Split on newlines, drop empty and separator lines
    lines = [ln.strip() for ln in re.split(r"\r?\n+", block) if ln and set(ln.strip()) != {"_"}]
    if not lines:
        return "", ""
    location = lines[0].strip()
    mailing = ""
    # Pick the first subsequent line that looks like a full mailing address (has commas)
    for ln in lines[1:]:
        if "," in ln:
            mailing = ln.strip()
            break
    return location, mailing


def parse_mailing_address(addr: str) -> dict:
    """Parse full mailing address into components for niche file format."""
    if not isinstance(addr, str):
        return {
            "Mailing Address": "",
            "Mailing Unit #": "",
            "Mailing City": "",
            "Mailing State": "",
            "Mailing Zip": "",
            "Mailing Zip+4": ""
        }
    
    addr = addr.strip()
    if not addr:
        return {
            "Mailing Address": "",
            "Mailing Unit #": "",
            "Mailing City": "",
            "Mailing State": "",
            "Mailing Zip": "",
            "Mailing Zip+4": ""
        }
    
    # Split by commas to separate components
    parts = [p.strip() for p in addr.split(",")]
    
    mailing_address = ""
    mailing_unit = ""
    mailing_city = ""
    mailing_state = ""
    mailing_zip = ""
    mailing_zip4 = ""
    
    if len(parts) >= 3:
        # Extract street address (first part)
        street_part = parts[0]
        
        # Check if street address contains unit number (common patterns: #, APT, UNIT, STE, LOT)
        unit_patterns = [r'\s+#\s*(\w+)', r'\s+APT\s+(\w+)', r'\s+UNIT\s+(\w+)', r'\s+STE\s+(\w+)', r'\s+LOT\s+(\w+)']
        for pattern in unit_patterns:
            match = re.search(pattern, street_part, re.IGNORECASE)
            if match:
                mailing_unit = match.group(1)
                street_part = re.sub(pattern, '', street_part, flags=re.IGNORECASE).strip()
                break
        
        mailing_address = street_part
        mailing_city = parts[1].strip()  # Second part is city
        
        # Third part should be state, fourth part should be zip
        if len(parts) >= 4:
            mailing_state = parts[2].strip()
            zip_part = parts[3].strip()
            
            # Parse zip (e.g., "24501" or "24501-1234")
            zip_match = re.match(r'(\d{5})(?:-(\d{4}))?', zip_part)
            if zip_match:
                mailing_zip = zip_match.group(1)
                mailing_zip4 = zip_match.group(2) or ""
        elif len(parts) == 3:
            # Format might be "street, city, state zip"
            state_zip = parts[2].strip()
            state_zip_match = re.match(r'([A-Z]{2})\s+(\d{5})(?:-(\d{4}))?', state_zip)
            if state_zip_match:
                mailing_state = state_zip_match.group(1)
                mailing_zip = state_zip_match.group(2)
                mailing_zip4 = state_zip_match.group(3) or ""
    
    return {
        "Mailing Address": mailing_address,
        "Mailing Unit #": mailing_unit,
        "Mailing City": mailing_city,
        "Mailing State": mailing_state,
        "Mailing Zip": mailing_zip,
        "Mailing Zip+4": mailing_zip4
    }


def clean_delinquent_excel(input_path: Path) -> pd.DataFrame:
    # Read without assuming headers; the file uses a report layout
    raw = pd.read_excel(input_path, header=None, dtype=str)
    # Find header row (contains "Parcel ID" and "Current Owner")
    header_idx = None
    for i in range(min(len(raw), 50)):  # search first 50 rows for headers
        row_vals = [str(v) if v is not None else "" for v in raw.iloc[i].tolist()]
        if any("parcel id" in v.lower() for v in row_vals) and any("current owner" in v.lower() for v in row_vals):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Could not locate header row containing 'Parcel ID' and 'Current Owner'.")

    data = raw.iloc[header_idx + 1 :].reset_index(drop=True)

    # Column positions inferred from observed layout
    # c1: Parcel ID, c3: Current Owner, c4: Location/Mailing block, c5: Amounts block
    c1, c3, c4, c5 = 1, 3, 4, 5

    # Keep rows that look like real data (Parcel ID present)
    data = data[data[c1].notna()]

    records = []
    for _, row in data.iterrows():
        parcel_id = str(row.get(c1, "") or "").strip()
        owner = str(row.get(c3, "") or "").strip()
        address_block = row.get(c4, "") or ""
        money_block = row.get(c5, "") or ""

        location_addr, mailing_full = split_address_block(address_block)
        money_vals = extract_money_values(money_block)
        last, first = parse_owner(owner)
        mailing_components = parse_mailing_address(mailing_full)

        # Construct the normalized record expected by monthly_processing_v2 niche updater
        rec = {
            "Parcel ID": parcel_id,
            "Current Owner": owner,
            "Owner 1 Last Name": last,
            "Owner 1 First Name": first,
            "Address": location_addr,
            "City": "",  # Property city (not available in tax delinquent data)
            "State": "",  # Property state (not available in tax delinquent data)
            "Zip": "",   # Property zip (not available in tax delinquent data)
            "Last Sale Date": "",  # Not available in tax delinquent data
            "Last Sale Amount": "",  # Not available in tax delinquent data
        }
        # Add all mailing address components
        rec.update(mailing_components)
        rec.update(money_vals)
        # Skip empty rows if no address
        if rec["Address"]:
            records.append(rec)

    if not records:
        return pd.DataFrame(columns=[
            "Owner 1 Last Name", "Owner 1 First Name", "Address", "City", "State", "Zip",
            "Mailing Address", "Mailing Unit #", "Mailing City", "Mailing State", 
            "Mailing Zip", "Mailing Zip+4", "Last Sale Date", "Last Sale Amount",
            "Parcel ID", "Current Owner", "Tax Amount", "Penalty Amount", 
            "Interest Amount", "Total Amount"
        ])

    df = pd.DataFrame.from_records(records)
    # Deduplicate on Address to avoid dup updates
    df = df.drop_duplicates(subset=["Address"])\
           .reset_index(drop=True)
    return df


def main():
    p = argparse.ArgumentParser(description="Clean Real Estate Delinquent List into niche format")
    p.add_argument("--input", required=True, help="Path to raw delinquent Excel report")
    p.add_argument("--region", required=True, help="Region key (e.g., lynchburg_city_va)")
    p.add_argument("--date", help="YYYYMMDD for output filename; if omitted, attempts to infer from input name")
    args = p.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"Input file not found: {in_path}")

    # Infer date from filename like 9-2-2025
    out_date = args.date
    if not out_date:
        m = re.search(r"(\d{1,2})[-_/](\d{1,2})[-_/](\d{2,4})", in_path.name)
        if m:
            mm, dd, yyyy = m.groups()
            yyyy = ("20" + yyyy) if len(yyyy) == 2 else yyyy
            out_date = f"{yyyy}{int(mm):02d}{int(dd):02d}"
        else:
            out_date = "unknown"

    # Clean
    df = clean_delinquent_excel(in_path)

    # Save under region folder
    region_dir = Path("regions") / args.region
    region_dir.mkdir(parents=True, exist_ok=True)
    out_path = region_dir / f"{args.region}_tax_delinquent_{out_date}.xlsx"
    df.to_excel(out_path, index=False)
    print(f"Saved cleaned niche file: {out_path}")


if __name__ == "__main__":
    main()

