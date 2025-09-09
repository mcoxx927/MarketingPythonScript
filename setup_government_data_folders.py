#!/usr/bin/env python3
"""
Setup script for government data folder structure.
Creates the recommended folder hierarchy for government data processing.
"""

import os
from pathlib import Path
import json


def create_government_data_structure(base_path: str = "."):
    """Create the government data folder structure"""
    
    base_path = Path(base_path)
    
    print("Setting up Government Data Processing Structure...")
    
    # Get existing regions
    regions_path = base_path / "regions"
    if not regions_path.exists():
        print("ERROR: No regions folder found. Please run this from the MarketingPythonScript directory.")
        return
    
    # Get list of existing regions
    regions = [d.name for d in regions_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
    
    if not regions:
        print("ERROR: No regions found in regions/ folder.")
        return
    
    print(f"Found {len(regions)} existing regions: {', '.join(regions)}")
    
    # Create government_data folder structure
    gov_data_path = base_path / "government_data"
    gov_data_path.mkdir(exist_ok=True)
    
    for region in regions:
        region_gov_path = gov_data_path / region
        
        # Create region subfolders
        (region_gov_path / "raw").mkdir(parents=True, exist_ok=True)
        (region_gov_path / "gis").mkdir(parents=True, exist_ok=True)
        (region_gov_path / "processed").mkdir(parents=True, exist_ok=True)
        
        # Create README files
        create_region_readme(region_gov_path, region)
        
        print(f"SUCCESS: Created government data structure for {region}")
    
    # Create tools configuration
    create_tools_config(base_path)
    
    # Create example batch scripts
    create_batch_scripts(base_path)
    
    print("\nGovernment Data Structure Setup Complete!")
    print("\nNext Steps:")
    print("1. Place raw government files in government_data/{region}/raw/")
    print("2. Place GIS/bulk data in government_data/{region}/gis/") 
    print("3. Process with: python tools/clean_code_enforcement.py --input [...] --region [...]")
    print("4. Run monthly processing: python monthly_processing_v2.py --region [...]")
    print("\nSee DIRECT_MAIL_PROCESSING_GUIDE.md for complete instructions")


def create_region_readme(region_path: Path, region: str):
    """Create README file for region government data folder"""
    
    readme_content = f"""# Government Data for {region.upper()}

## Folder Structure

### raw/
Place raw government data files here before processing:
- Code enforcement reports
- Tax delinquent reports  
- Building permits
- Housing violations
- Other municipal data

### gis/
Place GIS and bulk property data here:
- Parcel data (CSV format)
- Property assessments
- Zoning information
- Other spatial data

### processed/
Archive of processed government data (auto-generated):
- Cleaned government files
- Processing logs
- Historical versions

## Processing Commands

### Code Enforcement (with GIS)
```bash
python tools/clean_code_enforcement.py \\
  --input "government_data/{region}/raw/Code_Enforcement_Cases.xlsx" \\
  --region {region} \\
  --gis-file "government_data/{region}/gis/Parcels{region.title().replace('_', '')}.csv"
```

### Tax Delinquent
```bash  
python tools/clean_tax_delinquent.py \\
  --input "government_data/{region}/raw/Tax_Delinquent_Report.xlsx" \\
  --region {region}
```

### Integration with Monthly Processing
```bash
# After processing government data, run monthly processing
python monthly_processing_v2.py --region {region}
```

## File Naming Conventions

### Input Files (raw/)
- `Code_Enforcement_Cases_YYYY-MM-DD.xlsx`
- `Tax_Delinquent_Report_YYYY-MM-DD.xlsx` 
- `Building_Permits_QN_YYYY.xlsx`
- `Housing_Violations_YYYY-MM-DD.xlsx`

### GIS Files (gis/)
- `Parcels{region.title().replace('_', '')}.csv` (main parcel data)
- `PropertyAssessments.csv`
- `ZoningData.csv`

### Output Files (auto-generated in regions/{region}/)
- `{region}_code_enforcement_YYYYMMDD.xlsx`
- `{region}_tax_delinquent_YYYYMMDD.xlsx`

## Data Quality Notes

### GIS-Augmented Processing
When GIS data is available, government files get enhanced with:
- Complete mailing addresses (100% coverage)
- Property details (sale dates, assessed values)
- Ready for direct mail campaigns
- No skip trace required

### Direct Government Sources
- Authoritative distress indicators
- Current violation/delinquency status
- May need skip trace for mailing addresses
- Higher priority than vendor data
"""
    
    readme_path = region_path / "README.md"
    with open(readme_path, 'w') as f:
        f.write(readme_content)


def create_tools_config(base_path: Path):
    """Create government data tools configuration"""
    
    config = {
        "data_sources": {
            "code_enforcement": {
                "description": "Municipal code enforcement violations and citations",
                "format_type": "tabular",
                "detection_keywords": ["code", "enforcement", "violation", "citation"],
                "required_columns": ["CASE NO", "PARCEL NO", "SITE ADDRESS", "OWNER NAME"],
                "gis_augmentation": True,
                "priority": "high"
            },
            "tax_delinquent": {
                "description": "Properties with delinquent tax payments", 
                "format_type": "report_layout",
                "detection_keywords": ["tax", "delinquent", "delinq", "unpaid"],
                "priority": "highest"
            },
            "building_permits": {
                "description": "Building permits and construction projects",
                "format_type": "tabular",
                "detection_keywords": ["permit", "building", "construction"],
                "gis_augmentation": True,
                "priority": "medium"
            },
            "housing_violations": {
                "description": "Housing code violations and maintenance issues",
                "format_type": "tabular", 
                "detection_keywords": ["housing", "violation", "maintenance", "habitability"],
                "gis_augmentation": True,
                "priority": "high"
            }
        },
        "gis_integration": {
            "enabled": True,
            "default_gis_file": "ParcelsRoanokeCity.csv",
            "parcel_id_column": "TAXID",
            "required_columns": ["TAXID", "LOCADDR", "OWNER", "OWNERADDR1", "MAILCITY", "MAILSTATE", "MAINZIPCOD"],
            "augmentation_fields": [
                "City", "State", "Zip", "Last Sale Date", "Last Sale Amount",
                "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
                "Property Type", "Total Assessed Value", "Square Feet", "Acres"
            ]
        },
        "processing_settings": {
            "auto_detect_data_type": True,
            "require_parcel_id": False,
            "address_normalization": True,
            "duplicate_handling": "keep_first",
            "output_format": "excel"
        }
    }
    
    config_path = base_path / "tools" / "government_data_config.json" 
    config_path.parent.mkdir(exist_ok=True)
    
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"SUCCESS: Created tools configuration: {config_path}")


def create_batch_scripts(base_path: Path):
    """Create batch processing scripts"""
    
    # Monthly government data processing script
    monthly_script = '''#!/bin/bash
# monthly_government_data.sh
# Process all government data files for all regions

echo "Monthly Government Data Processing - $(date)"

# Function to process government data for a region
process_region_government_data() {
    local region=$1
    echo "Processing government data for $region..."
    
    # Code enforcement files
    for file in government_data/$region/raw/*[Cc]ode*[Ee]nforcement*.xlsx; do
        if [ -f "$file" ]; then
            echo "  Processing code enforcement: $(basename "$file")"
            python tools/clean_code_enforcement.py --input "$file" --region "$region"
        fi
    done
    
    # Tax delinquent files
    for file in government_data/$region/raw/*[Tt]ax*[Dd]elinq*.xlsx; do
        if [ -f "$file" ]; then
            echo "  Processing tax delinquent: $(basename "$file")"
            python tools/clean_tax_delinquent.py --input "$file" --region "$region"
        fi
    done
    
    # Building permits files (future)
    for file in government_data/$region/raw/*[Pp]ermit*.xlsx; do
        if [ -f "$file" ]; then
            echo "  Processing building permits: $(basename "$file")"
            # python tools/clean_permits.py --input "$file" --region "$region"
            echo "    Building permits processor not yet implemented"
        fi
    done
}

# Process all regions with government data
for region_dir in government_data/*/; do
    if [ -d "$region_dir" ]; then
        region=$(basename "$region_dir")
        if [ -d "government_data/$region/raw" ] && [ "$(ls -A government_data/$region/raw 2>/dev/null)" ]; then
            process_region_government_data "$region"
        else
            echo "📭 No government data files found for $region"
        fi
    fi
done

echo "🏃‍♂️ Running monthly processing with government data..."
python monthly_processing_v2.py --all-regions

echo "✅ Monthly government data processing complete - $(date)"
'''
    
    script_path = base_path / "scripts" / "monthly_government_data.sh"
    script_path.parent.mkdir(exist_ok=True)
    
    with open(script_path, 'w') as f:
        f.write(monthly_script)
    
    # Make executable on Unix systems
    try:
        script_path.chmod(0o755)
    except:
        pass  # Windows doesn't support chmod
    
    print(f"SUCCESS: Created batch script: {script_path}")
    
    # Windows batch file
    windows_script = '''@echo off
REM monthly_government_data.bat
REM Process all government data files for all regions

echo Monthly Government Data Processing - %date% %time%

REM Process regions (simplified Windows version)
echo Processing government data files...

REM You'll need to customize this for your specific regions and files
python tools/clean_code_enforcement.py --input "government_data/roanoke_city_va/raw/Code_Enforcement_Cases.xlsx" --region roanoke_city_va
python tools/clean_tax_delinquent.py --input "government_data/roanoke_city_va/raw/Tax_Delinquent_Report.xlsx" --region roanoke_city_va

echo Running monthly processing with government data...
python monthly_processing_v2.py --all-regions

echo SUCCESS: Monthly government data processing complete - %date% %time%
pause
'''
    
    windows_script_path = base_path / "scripts" / "monthly_government_data.bat"
    with open(windows_script_path, 'w') as f:
        f.write(windows_script)
    
    print(f"SUCCESS: Created Windows batch file: {windows_script_path}")


if __name__ == "__main__":
    create_government_data_structure()