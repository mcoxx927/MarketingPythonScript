# Multi-Region Real Estate Processing System

## Overview

This system processes real estate direct mail data for multiple regions simultaneously, with each region having its own configuration, file organization, and output structure. It supports 10+ regions with market-specific thresholds and standardized workflows.

## 🎯 Key Features

✅ **Multi-Region Support** - Process 10+ regions with individual configurations  
✅ **No Region Mix-ups** - Physical file separation prevents processing wrong files  
✅ **Market-Specific Settings** - Custom date/amount thresholds per region  
✅ **Standardized Workflow** - Consistent processing across all regions  
✅ **Batch Processing** - Process all regions with one command  
✅ **Enhanced Priority Codes** - Niche indicators append to existing priorities  
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
- `tax_delinquencies.xlsx` - Tax delinquent properties
- `landlords.xlsx` - Tired landlord properties
- `probate.xlsx` - Probate properties
- `cash_buyers.xlsx` - Cash buyer properties
- `interfamily.xlsx` - Inter-family transfers

### Step 4: Test Configuration
```bash
python multi_region_config.py
```

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

## 📋 Monthly Processing Workflow

### 1. Prepare Files (Monthly)
```bash
# For each region you're processing:
# 1. Export data from your data source
# 2. Name files according to conventions
# 3. Place in appropriate region folder
```

### 2. Validate Setup
```bash
# Check region configurations
python multi_region_config.py

# List available regions
python monthly_processing_v2.py --list-regions
```

### 3. Process Regions
```bash
# Process single region
python monthly_processing_v2.py --region roanoke_city_va

# Or process all regions at once
python monthly_processing_v2.py --all-regions
```

### 4. Review Results
- Check `output/region_name/YYYY_MM/` for results
- Review processing logs for any issues
- Validate enhanced priority codes

## 🔍 Output Files

### Main Output File
**`main_region_enhanced_YYYYMMDD.xlsx`** - Complete enhanced dataset containing:

**Original Columns:** All columns from your source data  
**Added Columns:**
- `IsTrust`, `IsChurch`, `IsBusiness` - Property classifications
- `IsOwnerOccupied` - Owner occupancy determination
- `PriorityId`, `PriorityCode`, `PriorityName` - Priority assignments
- `ParsedSaleDate`, `ParsedSaleAmount` - Cleaned data fields

**Enhanced Priority Codes:**
- Original: `ABS1`, `OWN20`, `BUY2`, etc.
- Enhanced: `Liens-ABS1`, `Tax-Landlord-OWN20`, `PreForeclosure-BUY2`

### Processing Log
**`processing_YYYYMMDD_HHMM.log`** - Detailed processing log with:
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

**"No Excel files found"**  
- Verify files have `.xlsx` extension
- Check file naming matches conventions
- Ensure files aren't corrupted

**"Address matching issues"**
- Address normalization handles common variations
- Check for unusual address formats in your data
- Review processing logs for specific issues

### Validation Commands
```bash
# Test configuration loading
python multi_region_config.py

# Validate specific region
python monthly_processing_v2.py --region test_region
```

### Debug Mode
Add debug logging to see detailed processing:
```bash
# Edit monthly_processing_v2.py, change:
logging.basicConfig(level=logging.DEBUG)
```

## 📊 Data Quality & Validation

### Automatic Validations
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