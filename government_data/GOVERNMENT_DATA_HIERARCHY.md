# Government Data Processing Hierarchy

## 📁 Complete Folder Structure

```
MarketingPythonScript/
├── regions/                           # Region configurations and files
│   ├── roanoke_city_va/
│   │   ├── config.json               # Region-specific settings
│   │   ├── README.md                 # Region instructions
│   │   ├── main_region.xlsx          # Main property file (~21k records)
│   │   ├── liens.xlsx                # Traditional niche lists
│   │   ├── foreclosure.xlsx
│   │   ├── bankruptcy.xlsx
│   │   ├── roanoke_city_va_code_enforcement_YYYYMMDD.xlsx    # GIS-augmented
│   │   ├── roanoke_city_va_tax_delinquent_YYYYMMDD.xlsx     # City direct
│   │   └── vendor_tax_delinquencies.xlsx                    # Vendor data
│   └── ... (other regions)
├── government_data/                   # Raw government data staging
│   ├── roanoke_city_va/
│   │   ├── raw/                      # Raw government files (before processing)
│   │   │   ├── Code Enforcement Cases Cited 2-25-2025 to 6-25-2025.xlsx
│   │   │   ├── Real Estate Delinquent List - 9-2-2025.xlsx
│   │   │   └── Building Permits Q3 2025.xlsx
│   │   ├── gis/                      # GIS/bulk property data
│   │   │   ├── ParcelsRoanokeCity.csv
│   │   │   ├── PropertyAssessments.csv
│   │   │   └── ZoningData.csv
│   │   └── processed/                # Cleaned government data (archive)
│   │       ├── code_enforcement_20250225_processed.xlsx
│   │       └── tax_delinquent_20250902_processed.xlsx
│   └── ... (other regions)
├── tools/                            # Government data processing tools
│   ├── government_data_standardizer.py      # Framework (future)
│   ├── government_data_standardizer.py            # Code enforcement + GIS
│   └── legacy/ (archived old tools)             # Tax delinquent reports
│   ├── clean_permits.py                    # Building permits (future)
│   ├── clean_violations.py                 # Housing violations (future)
│   └── government_data_config.json         # Data source configurations
├── output/                           # Enhanced processing results  
│   ├── roanoke_city_va/
│   │   └── 2025_09/
│   │       ├── roak_main_region_enhanced_20250903.xlsx  # With govt flags
│   │       └── processing_20250903_1430.log
│   └── ... (other regions)
├── monthly_processing_v2.py          # Multi-region processor (updated)
├── skip_trace_processor.py           # Skip trace integration
└── DIRECT_MAIL_PROCESSING_GUIDE.md   # Complete documentation
```

## 🔄 Government Data Processing Flow

### Phase 1: Data Collection & Staging
```bash
# Collect raw government data
government_data/roanoke_city_va/raw/
├── Code Enforcement Cases Cited 2-25-2025 to 6-25-2025.xlsx    # From city
├── Real Estate Delinquent List - 9-2-2025.xlsx                 # From assessor  
└── Building Permits Q3 2025.xlsx                               # From permits dept

# Collect GIS/bulk data (one-time or periodic updates)
government_data/roanoke_city_va/gis/
└── ParcelsRoanokeCity.csv                                       # From city GIS
```

### Phase 2: Data Processing & Augmentation
```bash
# Process code enforcement with GIS augmentation
python tools/government_data_standardizer.py --type code_enforcement \
  --input "government_data/roanoke_city_va/raw/Code Enforcement Cases Cited 2-25-2025 to 6-25-2025.xlsx" \
  --region roanoke_city_va \
  --gis-file "government_data/roanoke_city_va/gis/ParcelsRoanokeCity.csv"

# Process tax delinquent reports  
python tools/government_data_standardizer.py --type tax_delinquent \
  --input "government_data/roanoke_city_va/raw/Real Estate Delinquent List - 9-2-2025.xlsx" \
  --region roanoke_city_va

# Results automatically placed in regions/ folder:
# regions/roanoke_city_va/roanoke_city_va_code_enforcement_20250225.xlsx
# regions/roanoke_city_va/roanoke_city_va_tax_delinquent_20250902.xlsx
```

### Phase 3: Monthly Processing Integration
```bash
# Process region with all niche lists (including government data)
python monthly_processing_v2.py --region roanoke_city_va

# Government data gets integrated with priority codes:
# - "CodeEnforcement-ABS1" - Code violation on absentee property
# - "CurrentTax-Liens-BUY2" - Current tax delinquent with liens, recent buyer
```

### Phase 4: Skip Trace Enhancement  
```bash
# Skip trace includes government-flagged properties
python skip_trace_processor.py \
  --region roanoke_city_va \
  --skip-trace-file "weekly_skip_trace.xlsx"
```

## 🎯 Data Source Types & Priorities

### Tier 1: GIS-Augmented (Highest Quality)
- **Code Enforcement** → GIS-augmented with complete mailing addresses
- **Building Permits** → GIS-augmented with property details
- **Complete data for insertions** when no main file match

### Tier 2: Direct Government (High Quality)  
- **Current Tax Delinquent** → Direct from city/county
- **Current Violations** → Direct from departments
- **Reliable but may need address enhancement**

### Tier 3: Vendor Data (Standard Quality)
- **Historical tax data** → From BiggerPockets, etc.
- **Bulk distress data** → From data providers
- **Standard niche processing**

## 📋 Processing Commands by Data Type

### Code Enforcement (with GIS)
```bash
# Full augmentation (recommended)
python tools/government_data_standardizer.py --type code_enforcement \
  --input "raw_code_enforcement.xlsx" \
  --region roanoke_city_va

# Without GIS (basic processing)
python tools/government_data_standardizer.py --type code_enforcement \
  --input "raw_code_enforcement.xlsx" \
  --region roanoke_city_va \
  --no-gis
```

### Tax Delinquent (city reports)
```bash
# Clean raw city tax report
python tools/government_data_standardizer.py --type tax_delinquent \
  --input "Real Estate Delinquent List.xlsx" \
  --region lynchburg_city_va \
  --date 20250902
```

### Building Permits (future)
```bash
# Process building permits
python tools/clean_permits.py \
  --input "Building Permits Q3.xlsx" \
  --region roanoke_city_va
```

## 🔍 Quality Metrics by Source

### GIS-Augmented Sources
- ✅ **100%** mailing addresses
- ✅ **95%+** property details (sale dates, values)
- ✅ **Ready for insertion** if no main file match
- ✅ **Immediate mail campaigns** possible

### Direct Government Sources  
- ✅ **100%** property addresses
- ⚠️ **Variable** mailing addresses (0-60%)
- ✅ **Authoritative** violation/delinquency data
- ⚠️ **May need** skip trace enhancement

### Vendor Sources
- ✅ **Rich** property data
- ⚠️ **Historical** rather than current
- ✅ **Good** for bulk processing
- ⚠️ **Lower priority** than direct sources

## 🚀 Automation Opportunities

### Monthly Batch Processing
```bash
#!/bin/bash
# government_data_monthly.sh

# Process all raw government data files
for region in roanoke_city_va lynchburg_city_va virginia_beach_va; do
  echo "Processing government data for $region..."
  
  # Code enforcement
  if [ -f "government_data/$region/raw/"*code*enforcement*.xlsx ]; then
    python tools/government_data_standardizer.py --type code_enforcement \
      --input government_data/$region/raw/*code*enforcement*.xlsx \
      --region $region
  fi
  
  # Tax delinquent  
  if [ -f "government_data/$region/raw/"*tax*delinq*.xlsx ]; then
    python tools/government_data_standardizer.py --type tax_delinquent \
      --input government_data/$region/raw/*tax*delinq*.xlsx \
      --region $region
  fi
done

# Run monthly processing with all government data
python monthly_processing_v2.py --all-regions
```

### GIS Data Updates
```bash
#!/bin/bash  
# update_gis_data.sh

# Download/update GIS data (quarterly)
for region in roanoke_city_va virginia_beach_va; do
  echo "Checking for GIS updates for $region..."
  # Your GIS download logic here
done
```

## 📊 Integration with Main Processing

### Priority Code Evolution
**Original:** `ABS1` (Absentee seller)
**+ Government:** `CodeEnforcement-ABS1` (Code violation + absentee)
**+ Skip Trace:** `STLien-CodeEnforcement-ABS1` (Skip trace lien + violation + absentee)

### Database Integration (Future SQL Migration)
```sql
-- Enhanced Location table includes government data flags
ALTER TABLE Location ADD 
    HasCodeEnforcement BIT DEFAULT 0,
    HasCurrentTaxDelinquent BIT DEFAULT 0,
    HasBuildingViolations BIT DEFAULT 0,
    
    -- Government data details
    CodeEnforcementCaseTypes NVARCHAR(200),
    TaxDelinquentAmount DECIMAL(10,2),
    LastViolationDate DATETIME,
    
    -- Data source tracking
    GovernmentDataSource NVARCHAR(50),  -- "GIS_Augmented", "City_Direct", "Vendor"
    GovernmentDataDate DATETIME;
```

This hierarchy provides complete separation between raw government data collection, processing, and integration into your existing marketing pipeline.