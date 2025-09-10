"""
Tax Delinquent Data Cleaner Router

Auto-detects region and routes to appropriate region-specific cleaner.
Each region may have completely different data formats from their tax systems.
"""

import argparse
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Clean Tax Delinquent data (auto-detects region format)")
    parser.add_argument("--input", required=True, help="Path to tax delinquent Excel file")
    parser.add_argument("--region", required=True, help="Region key (e.g., roanoke_city_va, lynchburg_city_va)")
    parser.add_argument("--date", help="YYYYMMDD for output filename; if omitted, attempts to infer from input name")
    parser.add_argument("--gis-file", help="Path to GIS parcel data CSV file for augmentation")
    parser.add_argument("--no-gis", action="store_true", help="Skip GIS augmentation even if file exists")
    
    args = parser.parse_args()
    
    # Route to region-specific cleaner
    if args.region == "roanoke_city_va":
        print(f"Using Roanoke City tax delinquent cleaner (simple table format)")
        from clean_tax_delinquent_roanoke import main as roanoke_main
        # Replace sys.argv to pass args to region-specific cleaner
        sys.argv = [
            "clean_tax_delinquent_roanoke.py",
            "--input", args.input,
            "--region", args.region
        ]
        if args.date:
            sys.argv.extend(["--date", args.date])
        if args.gis_file:
            sys.argv.extend(["--gis-file", args.gis_file])
        if args.no_gis:
            sys.argv.append("--no-gis")
        roanoke_main()
        
    elif args.region == "lynchburg_city_va":
        print(f"Using Lynchburg City tax delinquent cleaner (complex report format)")
        from clean_tax_delinquent_lynchburg import main as lynchburg_main
        # Replace sys.argv to pass args to region-specific cleaner  
        sys.argv = [
            "clean_tax_delinquent_lynchburg.py",
            "--input", args.input,
            "--region", args.region
        ]
        if args.date:
            sys.argv.extend(["--date", args.date])
        if args.gis_file:
            sys.argv.extend(["--gis-file", args.gis_file])
        if args.no_gis:
            sys.argv.append("--no-gis")
        lynchburg_main()
        
    else:
        print(f"ERROR: No tax delinquent cleaner available for region '{args.region}'")
        print("Available regions:")
        print("  - roanoke_city_va: Simple table format")
        print("  - lynchburg_city_va: Complex report format")
        print()
        print("To add support for a new region, create:")
        print(f"  tools/clean_tax_delinquent_{args.region}.py")
        sys.exit(1)


if __name__ == "__main__":
    main()