# Government Data for NEWPORT_NEWS_VA

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
python tools/clean_code_enforcement.py \
  --input "government_data/newport_news_va/raw/Code_Enforcement_Cases.xlsx" \
  --region newport_news_va \
  --gis-file "government_data/newport_news_va/gis/ParcelsNewportNewsVa.csv"
```

### Tax Delinquent
```bash  
python tools/clean_tax_delinquent.py \
  --input "government_data/newport_news_va/raw/Tax_Delinquent_Report.xlsx" \
  --region newport_news_va
```

### Integration with Monthly Processing
```bash
# After processing government data, run monthly processing
python monthly_processing_v2.py --region newport_news_va
```

## File Naming Conventions

### Input Files (raw/)
- `Code_Enforcement_Cases_YYYY-MM-DD.xlsx`
- `Tax_Delinquent_Report_YYYY-MM-DD.xlsx` 
- `Building_Permits_QN_YYYY.xlsx`
- `Housing_Violations_YYYY-MM-DD.xlsx`

### GIS Files (gis/)
- `ParcelsNewportNewsVa.csv` (main parcel data)
- `PropertyAssessments.csv`
- `ZoningData.csv`

### Output Files (auto-generated in regions/newport_news_va/)
- `newport_news_va_code_enforcement_YYYYMMDD.xlsx`
- `newport_news_va_tax_delinquent_YYYYMMDD.xlsx`

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
