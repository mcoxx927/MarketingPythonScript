# Multi-Region Real Estate Direct Mail Processing System

## Overview

This system processes real estate direct mail data for multiple regions simultaneously, with complete workflow from initial processing through skip trace integration. Each region has its own configuration, file organization, and output structure. It supports 10+ regions with market-specific thresholds and standardized workflows including skip trace enhancement.

## 🎯 Key Features

✅ **Multi-Region Support** - Process 10+ regions with individual configurations  
✅ **No Region Mix-ups** - Physical file separation prevents processing wrong files  
✅ **Market-Specific Settings** - Custom date/amount thresholds per region  
✅ **Standardized Workflow** - Consistent processing across all regions  
✅ **Batch Processing** - Process all regions with one command  
✅ **Enhanced Priority Codes** - Niche indicators append to existing priorities  
✅ **Skip Trace Integration** - Golden Address and distress flag enhancement  
✅ **Complete Direct Mail Cycle** - From initial processing to skip trace enhancement  
✅ **Organized Output** - Region-specific output folders with timestamps  

## 📁 Directory Structure

```
MarketingPythonScript/
├── regions/                           # Region configurations and files
│   ├── roanoke_city_va/
│   │   ├── config.json               # Region-specific settings
│   │   ├── README.md                 # Region instructions
│   │   ├── main_region.xlsx          # Main property file (~21k records)
│   │   ├── liens.xlsx                # Niche list files
│   │   ├── foreclosure.xlsx
│   │   ├── bankruptcy.xlsx
│   │   └── ... (other niche files)
│   ├── virginia_beach_va/
│   │   ├── config.json
│   │   ├── main_region.xlsx
│   │   └── ... (niche files)
│   └── ... (8 more regions)
├── output/                           # Processing results  
│   ├── roanoke_city_va/
│   │   └── 2025_09/
│   │       ├── main_region_enhanced_20250903.xlsx
│   │       └── processing_20250903_1430.log
│   └── ... (region-specific outputs)
├── monthly_processing_v2.py          # Multi-region processor
├── skip_trace_processor.py           # Skip trace integration processor
├── multi_region_config.py            # Configuration system
└── ... (existing processing modules)
```

## 🚀 Quick Start

### 1. List Available Regions
```bash
python monthly_processing_v2.py --list-regions
```

### 2. Process Single Region
```bash
python monthly_processing_v2.py --region roanoke_city_va
```

### 3. Process All Regions
```bash
python monthly_processing_v2.py --all-regions
```

## 🏛️ Government Data Integration

### Overview
The system now supports direct integration of government data sources with GIS augmentation for complete property information. Government data provides highly targeted distress indicators directly from municipal sources.

### Supported Government Data Types

**Tier 1: GIS-Augmented (Best Quality)**
- **Code Enforcement** - Violations, citations, compliance issues
- **Building Permits** - Construction activity, property improvements
- **Housing Violations** - Property maintenance issues

**Tier 2: Direct Government (High Quality)**  
- **Current Tax Delinquent** - Active tax delinquencies from city/county
- **Current Violations** - Recent violations from departments

**Tier 3: Vendor Data (Standard Quality)**
- **Historical Tax Data** - Historical tax issues from data providers
- **Bulk Distress Data** - Aggregated distress indicators

### Government Data Processing Workflow

#### Step 1: Collect Raw Government Data
```bash
# Create staging area (one-time setup)
mkdir -p government_data/roanoke_city_va/raw
mkdir -p government_data/roanoke_city_va/gis

# Place raw government files
government_data/roanoke_city_va/raw/
├── Code Enforcement Cases Cited 2-25-2025 to 6-25-2025.xlsx
├── Real Estate Delinquent List - 9-2-2025.xlsx
└── Building Permits Q3 2025.xlsx

# Place GIS/bulk property data (periodic updates)
government_data/roanoke_city_va/gis/
└── ParcelsRoanokeCity.csv
```

#### Step 2: Process Government Data
```bash
# Code enforcement with GIS augmentation (RECOMMENDED)
python tools/clean_code_enforcement.py \
  --input "government_data/roanoke_city_va/raw/Code Enforcement Cases Cited 2-25-2025 to 6-25-2025.xlsx" \
  --region roanoke_city_va

# Tax delinquent reports
python tools/clean_tax_delinquent.py \
  --input "government_data/roanoke_city_va/raw/Real Estate Delinquent List - 9-2-2025.xlsx" \
  --region roanoke_city_va \
  --date 20250902
```

**Results:** Clean niche files automatically placed in region folders:
- `regions/roanoke_city_va/roanoke_city_va_code_enforcement_20250225.xlsx`
- `regions/roanoke_city_va/roanoke_city_va_tax_delinquent_20250902.xlsx`

#### Step 3: Benefits of GIS Augmentation

**Code Enforcement with GIS Enhancement:**
- ✅ **100% mailing addresses** from GIS property data
- ✅ **Complete property details** (sale dates, assessed values, square footage)
- ✅ **Ready for insertion** if no match in main region file
- ✅ **Immediate mail campaigns** possible

**Example GIS-enhanced record:**
```
Property: 1725 PADBURY AVE SE
Owner: COLMENAREJO ROBERTO MARIVELA  
Mailing: 2727 N 32ND ST #215, PHOENIX, AZ 85008
Sale: 2023/10/26, Assessed: $72,400
Case: INOPERABLE VEHICLE (COMPLIED)
```

#### Step 4: Integration with Monthly Processing
Government data files are automatically detected and processed:

```bash
# Process region (includes all government niche files)
python monthly_processing_v2.py --region roanoke_city_va
```

**Enhanced Priority Codes:**
- `CodeEnforcement-ABS1` - Code violation on absentee property
- `CurrentTax-Liens-BUY2` - Current tax delinquent with liens, recent buyer
- `STBankruptcy-CodeEnforcement-ABS1` - Skip trace + government + base priority

### Government Data Configuration

#### Auto-Detection Rules
Government data files are automatically detected by filename patterns:

```python
# In monthly_processing_v2.py
def _detect_niche_type_from_filename(filename: str) -> str:
    filename_lower = filename.lower()
    
    if 'code' in filename_lower and 'enforcement' in filename_lower:
        return 'CodeEnforcement'
    elif ('tax' in filename_lower and 'delinq' in filename_lower):
        if filename_lower.startswith(('roanoke_', 'lynchburg_', 'norfolk_')):
            return 'CurrentTax'  # Higher priority - direct from locality
        else:
            return 'TaxHistory'  # Lower priority - historical vendor data
    # ... other detection rules
```

#### Data Source Priorities
**CurrentTax** (Highest Priority)
- Direct from city/county tax systems
- Files starting with region name get `CurrentTax-` prefix
- Example: `CurrentTax-ABS1`, `CurrentTax-Liens-BUY2`

**CodeEnforcement** (High Priority)
- Municipal violations and citations
- GIS-augmented for complete property data
- Example: `CodeEnforcement-ABS1`, `CodeEnforcement-Landlord-OWN20`

**TaxHistory** (Standard Priority)
- Historical tax data from vendors
- Third-party files get `TaxHistory-` prefix
- Example: `TaxHistory-ABS1`, `TaxHistory-BUY2`

### Government Data Quality Metrics

#### GIS-Augmented Sources (Code Enforcement)
- **100%** complete mailing addresses
- **97%** sale dates available
- **69%** sale amounts available
- **100%** property details (assessed values, etc.)
- **Ready for direct mail** without skip trace

#### Direct Government Sources (Tax Delinquent)
- **100%** property addresses
- **Variable** mailing addresses (0-60% depending on source)
- **Authoritative** violation/delinquency data
- **May require** skip trace for mailing addresses

## 🔄 Complete Direct Mail Cycle

### Phase 1: Government Data Collection & Processing (As Needed)
Collect and process government data sources:

```bash
# Process code enforcement with GIS augmentation
python tools/clean_code_enforcement.py \
  --input "Code Enforcement Cases.xlsx" \
  --region roanoke_city_va

# Process tax delinquent reports  
python tools/clean_tax_delinquent.py \
  --input "Tax Delinquent Report.xlsx" \
  --region roanoke_city_va
```

### Phase 2: Initial Processing (Monthly)
Process property data and create enhanced files with priority codes:

```bash
# Process single region
python monthly_processing_v2.py --region roanoke_city_va

# Process all regions
python monthly_processing_v2.py --all-regions
```

**Output:** `{region_code}_main_region_enhanced_YYYYMMDD.xlsx` with priority codes like:
- `ABS1` - High-priority absentee owners
- `Liens-ABS1` - Absentee owners with liens
- `STBankruptcy-Liens-ABS1` - Multiple distress indicators

### Phase 2: Direct Mail Campaign (Weekly)
Use enhanced files for weekly direct mail campaigns:
- Mail to highest priority records first
- Cycle through priority levels weekly
- Track which records were mailed

### Phase 3: Skip Trace Integration (Weekly)
After mailing, integrate skip trace data back into enhanced files:

```bash
# Skip trace single region (auto-finds latest enhanced file)
python skip_trace_processor.py --region roanoke_city_va --skip-trace-file "weekly_skip_trace.xlsx"

# Skip trace all regions
python skip_trace_processor.py --all-regions --skip-trace-file "weekly_skip_trace.xlsx"

# Skip trace with specific enhanced file
python skip_trace_processor.py --region roanoke_city_va --enhanced-file "output/roanoke_city_va/2024_01/roa_main_region_enhanced_20240115.xlsx" --skip-trace-file "weekly_skip_trace.xlsx"
```

**Skip Trace Enhancements:**
- **Golden Address** - Enhanced mailing addresses from skip trace provider
- **Golden_Address_Differs** - Flag when Golden Address differs from original (for A/B testing)
- **Skip Trace Flags** - STBankruptcy, STForeclosure, STLien, STJudgment, STQuitclaim, STDeceased
- **Enhanced Priority Codes** - Combines existing priorities with skip trace flags

### Phase 4: Enhanced Records Ready
Updated enhanced files now contain:
- Original property data and priority codes
- Golden Address improvements
- Skip trace distress indicators
- Phone numbers (when available)
- Combined priority codes like `STLien-Bankruptcy-ABS1`

## ⚙️ Setting Up a New Region

### Step 1: Create Region Folder
```bash
mkdir "regions/new_region_name"
```

### Step 2: Create Configuration File
Create `regions/new_region_name/config.json`:

```json
{
  "region_name": "Your City Name, ST",
  "region_code": "CODE", 
  "fips_code": "12345",
  "region_input_date1": "2009-01-01",
  "region_input_date2": "2019-01-01",
  "region_input_amount1": 75000,
  "region_input_amount2": 200000,
  "market_type": "Market Classification",
  "description": "Brief market description",
  "notes": "Any special notes about this market"
}
```

**Configuration Parameters:**
- `fips_code`: FIPS code for the region - **CRITICAL**: Must match FIPS column in all Excel files
- `region_input_date1`: ABS1 cutoff - properties sold before this date get high priority
- `region_input_date2`: BUY1/BUY2 cutoff - recent buyers sold after this date  
- `region_input_amount1`: Low amount threshold for TRS1, OON1 classifications
- `region_input_amount2`: High amount threshold for cash buyer identification

### Step 3: Add Your Files
Place your Excel files in the region folder using these names:

**Required:**
- `main_region.xlsx` - Main property export file

**Optional Niche Files:**
- `liens.xlsx` - Properties with liens
- `foreclosure.xlsx` - Pre-foreclosure properties
- `bankruptcy.xlsx` - Properties with bankruptcy  
- `tax_delinquencies.xlsx` - Tax delinquent properties (vendor data)
- `current_tax_delinquent_YYYYMMDD.xlsx` - Current tax delinquent (city data)
- `landlords.xlsx` - Tired landlord properties
- `probate.xlsx` - Probate properties
- `cash_buyers.xlsx` - Cash buyer properties
- `interfamily.xlsx` - Inter-family transfers

### Step 4: Test Configuration
```bash
python multi_region_config.py
```

## 🏛️ Tax Delinquent Data Processing

### Overview
The system supports two types of tax delinquent data with different priority levels:

- **CurrentTax** - Direct from city/county (highest priority)
- **TaxHistory** - From vendor services (lower priority)

### Processing Raw Tax Delinquent Reports

#### Clean City Tax Delinquent Files
For raw tax delinquent reports directly from localities:

```bash
# Clean a city tax delinquent report
python tools/clean_tax_delinquent.py --input "Real Estate Delinquent List - 9-2-2025.xlsx" --region lynchburg_city_va --date 20250902

# Auto-detect date from filename
python tools/clean_tax_delinquent.py --input "Tax_Report_9-2-2025.xlsx" --region roanoke_city_va
```

**Input:** Raw city tax delinquent report with columns like:
- Parcel ID, Current Owner, Location/Mailing Address, Tax amounts

**Output:** Clean niche file with standardized columns:
- `regions/lynchburg_city_va/lynchburg_city_va_tax_delinquent_20250902.xlsx`

#### Processing Results
The cleaned file will have proper mailing address format matching other niche files:
- **Mailing Address** - Street address only
- **Mailing Unit #** - Apartment/unit number 
- **Mailing City** - City name
- **Mailing State** - State code
- **Mailing Zip** - 5-digit ZIP
- **Mailing Zip+4** - 4-digit extension

### Tax Data Priority Levels

#### CurrentTax (Highest Priority)
- **Source:** Direct from city/county billing systems
- **Detection:** Files starting with region name (e.g., `lynchburg_city_va_tax_delinquent_*.xlsx`)
- **Priority Codes:** `CurrentTax-ABS1`, `CurrentTax-OWN20`
- **Contains:** Actual tax amounts owed, current delinquencies

#### TaxHistory (Lower Priority)  
- **Source:** Vendor services (BiggerPockets, etc.)
- **Detection:** Third-party files (e.g., `Property Export...Tax Delinquencies.xlsx`)
- **Priority Codes:** `TaxHistory-ABS1`, `TaxHistory-OWN20`
- **Contains:** Historical tax issues, rich property data

### Tax Data Workflow

#### Step 1: Clean Raw City Data (if applicable)
```bash
# Only needed for raw city reports
python tools/clean_tax_delinquent.py --input "city_tax_report.xlsx" --region your_region --date YYYYMMDD
```

#### Step 2: Place Files in Region Folder
```bash
# City data (gets CurrentTax priority)
regions/lynchburg_city_va/lynchburg_city_va_tax_delinquent_20250902.xlsx

# Vendor data (gets TaxHistory priority)  
regions/lynchburg_city_va/Property Export Lynchburg+City+Co%2C+VA+Tax+Delinquencies.xlsx
```

#### Step 3: Process with Monthly System
```bash
# Both files will be processed with different priority levels
python monthly_processing_v2.py --region lynchburg_city_va
```

#### Step 4: Review Priority Codes
**CurrentTax examples:**
- `CurrentTax-ABS1` - Current delinquent, old absentee owner
- `CurrentTax-Liens-ABS1` - Current delinquent with liens

**TaxHistory examples:**  
- `TaxHistory-ABS1` - Historical tax issues, absentee owner
- `TaxHistory-Landlord-OWN20` - Historical tax issues, tired landlord

## 📊 Skip Trace Data Requirements

### Required Skip Trace File Columns
Your skip trace provider file must contain these columns:

**Required:**
- `Property FIPS` - FIPS code matching your region configuration
- `Property Address` - Property address for matching
- `Golden Address` - Enhanced mailing address from skip trace provider

**Optional (for distress flags):**
- `Property APN` - Assessor's Parcel Number (improves matching accuracy)
- `Owner Bankruptcy` - True/False for bankruptcy flag
- `Owner Foreclosure` - True/False for foreclosure flag  
- `Lien` - True/False for lien flag
- `Judgment` - True/False for judgment flag
- `Quitclaim` - True/False for quitclaim flag
- `Owner Is Deceased` - True/False for deceased owner flag

### Skip Trace Matching Strategy
The system uses a hybrid matching approach:

1. **Primary Match:** Property APN + Property FIPS (most accurate)
2. **Fallback Match:** Normalized address matching using Property Address
3. **FIPS Filtering:** Only processes records matching the region's FIPS code

### Skip Trace Flag Values
The system recognizes these values as "True":
- `True`, `true`, `TRUE`
- `Yes`, `yes`, `YES` 
- `1`
- `y`, `Y`

All other values (False, No, 0, empty, etc.) are treated as False.

## 📊 How Processing Works

### Main Region Processing
1. **Loads region configuration** from `config.json`
2. **Processes main_region.xlsx** with region-specific thresholds
3. **Applies property classification** (trusts, churches, businesses)
4. **Assigns priority codes** (ABS1, OWN1, BUY1, etc.) based on region criteria

### Niche List Integration  
1. **For each niche file** in the region folder:
   - **If address exists in main region**: Appends niche indicator to priority code
     - Example: `ABS1` → `Liens-ABS1` → `Tax-Liens-ABS1`
   - **If address NOT in main region**: Inserts new record with niche name as priority
     - Example: New record with priority `Liens`

### Enhanced Priority Examples
- `Liens-ABS1` - Property has liens AND is an old absentee sale
- `Tax-Landlord-OWN20` - Tax delinquent, tired landlord, very old owner-occupied
- `PreForeclosure-BUY2` - Pre-foreclosure property that was a recent high-value buyer

## 🎛️ Region Configuration Guide

### Market-Specific Thresholds

**Rural/Small City Markets** (like Roanoke):
```json
{
  "region_input_amount1": 75000,
  "region_input_amount2": 200000
}
```

**Coastal/Resort Markets** (like Virginia Beach):
```json
{
  "region_input_amount1": 150000,
  "region_input_amount2": 400000
}
```

**Metro/High-Value Markets** (like Alexandria):
```json
{
  "region_input_amount1": 200000,
  "region_input_amount2": 600000
}
```

### Date Criteria Guidelines

**Conservative (Longer Holding Periods):**
- `region_input_date1`: 10-15 years ago
- `region_input_date2`: 5-6 years ago

**Aggressive (Shorter Holding Periods):**
- `region_input_date1`: 5-8 years ago  
- `region_input_date2`: 2-3 years ago

## 📋 Complete Monthly Processing Workflow

### 1. Monthly Initial Processing

#### Prepare Files
```bash
# For each region you're processing:
# 1. Export data from your data source
# 2. Name files according to conventions
# 3. Place in appropriate region folder
```

#### Validate Setup
```bash
# Check region configurations
python multi_region_config.py

# List available regions
python monthly_processing_v2.py --list-regions
```

#### Process Regions
```bash
# Process single region
python monthly_processing_v2.py --region roanoke_city_va

# Or process all regions at once
python monthly_processing_v2.py --all-regions
```

#### Review Initial Results
- Check `output/region_name/YYYY_MM/` for enhanced files
- Review processing logs for any issues
- Validate enhanced priority codes and niche integration

### 2. Weekly Direct Mail Campaigns
- Use enhanced files for weekly mailings
- Mail highest priority records first
- Track which records were sent mail
- Collect mailing data for skip trace

### 3. Weekly Skip Trace Integration

#### Receive Skip Trace Data
- Get skip trace results from your provider
- Ensure file contains required columns (FIPS, Address, Golden Address)
- Place skip trace file in accessible location

#### Process Skip Trace Integration
```bash
# Auto-find latest enhanced files and integrate skip trace
python skip_trace_processor.py --all-regions --skip-trace-file "weekly_skip_trace_YYYYMMDD.xlsx"

# Process specific region
python skip_trace_processor.py --region roanoke_city_va --skip-trace-file "weekly_skip_trace_YYYYMMDD.xlsx"

# Use specific enhanced file
python skip_trace_processor.py --region roanoke_city_va --enhanced-file "output/roanoke_city_va/2024_01/roa_main_region_enhanced_20240115.xlsx" --skip-trace-file "skip_trace.xlsx"
```

#### Review Skip Trace Results
- Check enhanced files were updated in-place
- Review Golden Address differ counts for A/B testing
- Validate skip trace flags were properly applied
- Check enhanced priority codes include ST flags

### 4. Continue Direct Mail Cycle
- Use skip trace enhanced files for subsequent mailings
- Leverage Golden Addresses for improved deliverability
- Use ST flags for targeted campaigns
- Repeat weekly skip trace integration as needed

## 🔍 Output Files

### Main Output File
**`{region_code}_main_region_enhanced_YYYYMMDD.xlsx`** - Complete enhanced dataset containing:

**Example filenames:**
- `roak_main_region_enhanced_20250903.xlsx` (Roanoke)
- `vbch_main_region_enhanced_20250903.xlsx` (Virginia Beach)
- `rich_main_region_enhanced_20250903.xlsx` (Richmond)

**Original Columns:** All columns from your source data  
**Added Columns (Initial Processing):**
- `IsTrust`, `IsChurch`, `IsBusiness` - Property classifications
- `IsOwnerOccupied` - Owner occupancy determination
- `PriorityId`, `PriorityCode`, `PriorityName` - Priority assignments
- `ParsedSaleDate`, `ParsedSaleAmount` - Cleaned data fields

**Added Columns (Skip Trace Enhancement):**
- `Golden_Address` - Enhanced mailing address from skip trace
- `Golden_Address_Differs` - Boolean flag when Golden Address ≠ original address
- `ST_Flags` - Comma-separated skip trace flags (STBankruptcy, STLien, STDeceased, etc.)

**Enhanced Priority Codes Evolution:**
- **Initial:** `ABS1`, `OWN20`, `BUY2`, etc.
- **With Niche:** `Liens-ABS1`, `Tax-Landlord-OWN20`, `PreForeclosure-BUY2`
- **With Skip Trace:** `STBankruptcy-Liens-ABS1`, `STDeceased-Tax-Landlord-OWN20`

### Processing Log
**`{region_code}_processing_YYYYMMDD_HHMM.log`** - Detailed processing log with:

**Example filenames:**
- `roak_processing_20250903_1430.log`
- `vbch_processing_20250903_1430.log`

**Log Contents:**
- Configuration loaded
- Files processed
- Error messages
- Update/insert counts

## 🔧 Troubleshooting

### Common Issues

**"Region validation failed"**
- Ensure `config.json` exists and is valid JSON
- Verify at least one `.xlsx` file exists in region folder
- Check file permissions

**"FIPS validation failed"**
- **CRITICAL ERROR**: Excel files contain wrong region data
- Check that all Excel files have FIPS column
- Verify FIPS codes in files match the config.json setting
- This prevents processing wrong region data by mistake

**"No Excel files found"**  
- Verify files have `.xlsx` extension
- Check file naming matches conventions
- Ensure files aren't corrupted

**"Address matching issues"**
- Address normalization handles common variations
- Check for unusual address formats in your data
- Review processing logs for specific issues

### Skip Trace Specific Issues

**"No enhanced files found"**
- Run initial processing first: `python monthly_processing_v2.py --region region_name`
- Verify enhanced files exist in `output/region_name/YYYY_MM/`
- Check enhanced file naming matches pattern `*_main_region_enhanced_*.xlsx`

**"Skip trace file missing required columns"**
- Verify skip trace file contains: Property FIPS, Property Address, Golden Address
- Check column names match exactly (case sensitive)
- Ensure Property FIPS column contains numeric values

**"No skip trace matches found"**
- Check Property FIPS codes in skip trace file match region configurations
- Verify Property Address in skip trace file match format in enhanced files
- Review matching logic: Property APN+Property FIPS primary, Property Address fallback

**"Golden Address differs count is 0"**
- Check if Golden Address column contains different addresses
- Verify Golden Address isn't identical to Mailing Address
- Review address comparison logic in processing logs

**"Skip trace flags not applied"**
- Ensure flag columns use recognized True values: True, Yes, 1, Y
- Check for extra spaces or unusual formatting in flag columns
- Verify flag column names match: Owner Bankruptcy, Lien, etc.

### Government Data Processing Issues

#### Code Enforcement Issues

**"GIS file not found"**
- Verify GIS file exists: `ParcelsRoanokeCity.csv` in working directory
- Use `--gis-file` parameter to specify different location
- Use `--no-gis` flag to process without GIS augmentation
- Check file permissions and accessibility

**"0% GIS augmentation"**
- Check parcel ID format mismatch (e.g., `123-4567` vs `1234567`)
- Verify TAXID column exists in GIS file
- Ensure parcel IDs in code enforcement file match GIS format
- Check for data type issues (numeric vs string)

**"Missing expected columns"**
- Code enforcement file must contain: CASE NO, PARCEL NO, SITE ADDRESS, OWNER NAME
- Check for column name variations (case sensitive)
- Verify file is from code enforcement department (not other violation types)

**"Permission denied writing output"**
- Output file may be open in Excel
- Check region folder write permissions
- Use different output date with `--date` parameter

#### Tax Delinquent Cleaner Issues

**"Could not locate header row"**
- Raw city tax reports have varying formats
- Tool looks for "Parcel ID" and "Current Owner" in headers
- Check if your file has different column names
- File may already be clean (use directly without cleaning)

**"Mailing address parsing errors"**
- Tool expects format: "Street, City, State, Zip"
- Check for unusual address formatting in source data
- Verify addresses contain commas separating components
- Some addresses may not parse unit numbers correctly

**"File integration issues"**
- Ensure cleaned files have required columns: Address, Mailing Address, Owner names
- Check that Last Sale Date/Amount columns were added (may be empty)
- Verify file is saved in correct region folder with proper naming

#### GIS Data Issues

**"GIS file format problems"**
- Verify CSV file is properly formatted
- Check for required columns: TAXID, LOCADDR, OWNER, OWNERADDR1, MAILCITY, etc.
- Ensure no header row corruption or encoding issues
- File size should be reasonable (not truncated)

**"Parcel ID format mismatches"**
- GIS uses different parcel formats than code enforcement
- Check for leading zeros, dashes, or other formatting differences
- Example: GIS `123-4567` vs Code Enforcement `1234567`
- May need parcel format standardization

**"Incomplete GIS data"**
- Some parcels may have missing mailing addresses in GIS
- Check completeness of OWNERADDR1, MAILCITY, MAILSTATE columns
- Consider supplementing with additional data sources
- Use GIS data validation before processing

#### Monthly Processing Integration

**"Government niche files not detected"**
- Check filename patterns for auto-detection
- Verify files are in correct region folder
- Check that _detect_niche_type_from_filename() includes your file type
- Manually verify file naming conventions

**"Priority codes not appending correctly"**
- Check address normalization between main region and government data
- Verify parcel ID matching if applicable
- Review processing logs for matching statistics
- Check for duplicate addresses causing conflicts

**"Government data inserting instead of updating"**
- Address matching may be failing
- Check address format differences between sources
- Review normalized address output in logs
- Consider manual address standardization

### Validation Commands
```bash
# Test configuration loading
python multi_region_config.py

# Test tax delinquent cleaner
python tools/clean_tax_delinquent.py --input "test_tax_report.xlsx" --region test_region --date 20250902

# Validate specific region initial processing
python monthly_processing_v2.py --region test_region

# Test skip trace processing with small file
python skip_trace_processor.py --region test_region --skip-trace-file "test_skip_trace.xlsx"
```

### Debug Mode
Add debug logging to see detailed processing:
```bash
# Edit monthly_processing_v2.py, change:
logging.basicConfig(level=logging.DEBUG)
```

## 📊 Data Quality & Validation

### Automatic Validations
- **FIPS Code Validation**: Ensures all files match the expected region FIPS code - **PREVENTS REGION MIX-UPS**
- **Date parsing**: Blank dates treated as "very old" for high priority
- **Amount parsing**: Handles currency formatting and blank values  
- **Address normalization**: Standardizes addresses for matching
- **Duplicate detection**: Identifies potential duplicate records

### Quality Reports
- Processing logs show data quality metrics
- Update/insert counts verify niche integration
- Priority distribution shows classification results

## 🔄 Migration from Single-Region System

### Step 1: Backup Current System
```bash
# Create backup of existing files
cp -r "Excel files" "Excel files_backup"
cp -r "output" "output_backup"
```

### Step 2: Set Up Roanoke Region
```bash
# Already configured - just migrate files:
cp "Excel files/Property Export Roanoke*.xlsx" "regions/roanoke_city_va/main_region.xlsx"
cp "Excel files/*Liens*.xlsx" "regions/roanoke_city_va/liens.xlsx"
# ... copy other niche files with standardized names
```

### Step 3: Test Migration
```bash
python monthly_processing_v2.py --region roanoke_city_va
```

### Step 4: Compare Results
- Compare output with previous processing runs
- Verify record counts and priority distributions
- Check enhanced priority codes are working

## 🎯 Advanced Usage

### Custom Niche Types
Add new niche type detection in `monthly_processing_v2.py`:
```python
def _detect_niche_type_from_filename(filename: str) -> str:
    # Add your custom logic
    if 'your_custom_keyword' in filename_lower:
        return 'YourNicheType'
```

### Batch Automation
Create scripts to automate monthly processing:
```bash
#!/bin/bash
# monthly_batch.sh
python monthly_processing_v2.py --all-regions
echo "Processing complete: $(date)"
```

### Custom Reports
Extend the system with custom reporting:
```python
# Add to monthly_processing_v2.py
def generate_custom_report(results):
    # Your custom reporting logic
    pass
```

## 📞 Support

### Getting Help
1. **Check processing logs** in `output/region_name/YYYY_MM/`
2. **Review configuration** with `python multi_region_config.py`
3. **Validate region files** exist and are properly named
4. **Test with single region** before batch processing

### Best Practices
- **Always backup** your source files before processing
- **Test new regions** individually before adding to batch
- **Review output files** before using for mail campaigns
- **Keep configurations** consistent across similar markets
- **Monitor processing logs** for data quality issues

---

**Questions?** The modular Python design makes it easy to extend and customize for your specific needs.

**Performance Issues?** Process regions separately or contact for optimization guidance.

**Need New Features?** The system is designed for easy enhancement and customization.