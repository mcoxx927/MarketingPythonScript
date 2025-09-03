# Real Estate Direct Mail Processing System

A Python-based replacement for SQL stored procedures that processes real estate property data and assigns priority scores for direct mail marketing campaigns.

## 🎯 What This System Does

This system takes raw property data from Excel files and transforms it into prioritized mailing lists by:

- **Classifying Properties**: Identifies trusts, churches, businesses, and owner-occupied properties
- **Scoring Priorities**: Assigns priority levels (1-13) based on likelihood of selling
- **Adding Distress Indicators**: Flags properties with liens, foreclosure, bankruptcy, high equity, etc.
- **Data Quality Checks**: Validates data integrity and generates quality reports
- **Generating Reports**: Creates comprehensive Excel reports for campaign planning

## 📁 Project Structure

```
MarketingPythonScript/
├── Excel files/                          # Input Excel files directory
│   ├── Property Export Roanoke+City_2C+VA.xlsx      # Main region file (21k+ records)
│   ├── *Liens*.xlsx                      # Niche lists (hundreds to thousands)
│   ├── *PreForeclosure*.xlsx
│   └── ...
├── output/                               # Generated output files
├── property_processor.py                # Core property classification & scoring
├── niche_processor.py                   # Niche list processing & distress indicators  
├── main_processor.py                    # Main orchestrator script
├── analyze_excel_structure.py           # Data analysis utilities
├── test_processor.py                    # Testing script
└── README.md                           # This file
```

## 🚀 Quick Start

### Prerequisites
```bash
pip install pandas openpyxl numpy
```

### Basic Usage

**Process Everything (Main Region + All Niche Lists):**
```bash
python main_processor.py
```

**Process Only Main Region (21k records):**
```bash
python main_processor.py --main-only
```

**Process Only Niche Lists:**
```bash
python main_processor.py --niches-only
```

**Custom Directories:**
```bash
python main_processor.py --excel-dir "my_excel_files" --output-dir "my_output"
```

## 📊 Business Logic

### Property Classification

**Trust Detection:**
- Keywords: 'trust', 'estate', 'living', 'revocable', etc.
- Priority: 5 (TRS2 - Trust)

**Church Detection:**  
- Keywords: 'church', 'baptist', 'ministry', 'holy', etc.
- Priority: 10 (CHURCH - Church)

**Business Detection:**
- Keywords: 'llc', 'inc', 'corp', 'company', 'holding', etc.
- Higher complexity scoring based on occupancy

### Priority Scoring (1 = Highest, 13 = Lowest)

| Priority | Code | Description | Criteria |
|----------|------|-------------|----------|
| 1 | OIN1 | Owner-Occupant List 1 | Owner occupied + grantor match |
| 2 | OWN1 | Owner-Occupant List 3 | Owner occupied + 13+ year old sale |
| 3 | OON1 | Owner-Occupant List 4 | Owner occupied + low sale amount |
| 4 | BUY2 | Owner-Occupant List 5 | Owner occupied + recent high buyer |
| 5 | TRS2 | Trust | Trust properties |
| 6 | INH1 | Absentee List 1 | Absentee + grantor match |
| 7 | ABS1 | Absentee List 3 | Absentee + old sale date |
| 8 | TRS1 | Absentee List 4 | Absentee + low sale amount |
| 9 | BUY1 | Absentee List 5 | Absentee + recent high buyer |
| 10 | CHURCH | Church | Church properties |
| 11 | DEFAULT | Default | Unclassified properties |
| 13 | OWN20 | Owner-Occupant List 20 | Very old owner occupied (20+ years) |

### Distress Indicators

**Enhanced Priority Codes** combine base priorities with distress indicators:

- **HE**: High Equity (Loan-to-Value ≤ 50%)
- **Liens**: Has liens or judgments
- **Bankruptcy**: Has bankruptcy date
- **Divorce**: Has divorce date  
- **PreFor**: Pre-foreclosure proceedings
- **F&C**: Free & Clear (no loans, has equity)
- **Vacant**: Property marked as vacant
- **NCOA_Moves**: NCOA indicates owner moved
- **NCOA_Drops**: NCOA delivery issues

Example Enhanced Codes:
- `HE-Liens-PreFor-ABS1`: High equity, liens, pre-foreclosure, absentee property
- `F&C-Vacant-TRS2`: Free & clear, vacant trust property

## 📈 Performance Benefits vs SQL

| Aspect | SQL Stored Procedures | Python System |
|--------|----------------------|---------------|
| **Processing Time** | 1 hour for 200k records, frequent breaks | Stable processing, better error handling |
| **Maintainability** | Complex SQL, hard to debug | Clean Python, easy to modify |
| **Debugging** | Limited SQL debugging tools | Full Python debugging & logging |
| **Flexibility** | Fixed SQL logic | Easy to add new rules/indicators |
| **Testing** | Difficult to unit test | Comprehensive testing possible |
| **Documentation** | Scattered SQL comments | Clear Python documentation |

## 📋 Output Files

The system generates several output files in the `output/` directory:

### Main Files
- `main_region_processed.xlsx`: Processed main region data (21k+ records)
- `niche_*_processed.xlsx`: Individual niche list results
- `final_processing_report.xlsx`: Summary of all processing results
- `data_quality_report.xlsx`: Data quality metrics and issues
- `processing.log`: Detailed processing log

### Key Output Columns Added
- `IsTrust`, `IsChurch`, `IsBusiness`: Classification flags
- `IsOwnerOccupied`: Owner occupancy determination
- `PriorityId`, `PriorityCode`, `PriorityName`: Priority assignments
- `DistressScore`: Number of distress indicators (0-4+)
- `DistressIndicators`: Comma-separated list of indicators
- `EnhancedPriorityCode`: Combined priority + distress code

## 🔧 Advanced Configuration

### Custom Priority Rules

To modify priority scoring rules, edit the `PropertyPriorityScorer` class in `property_processor.py`:

```python
# Example: Change high equity threshold
def _is_high_equity(self, loan_to_value):
    return loan_to_value <= 40  # Changed from 50%
```

### Adding New Distress Indicators

Add new indicators in `niche_processor.py`:

```python
# In DistressIndicatorEngine.__init__()
self.indicators['NEW_INDICATOR'] = 'Description'

# Add logic in add_distress_indicators()
new_mask = (df['SomeColumn'] == 'SomeValue')
df.loc[new_mask, 'Has_NEW_INDICATOR'] = True
```

### Custom Niche Types

Register new niche types in `niche_processor.py`:

```python
# In NicheListProcessor.__init__()
self.niche_configs['NewNiche'] = NicheListConfig(
    file_pattern='*NewNiche*',
    niche_type='NewNiche',
    base_priority_code='NewNiche',
    distress_indicators=['HE', 'Liens']
)
```

## 🧪 Testing & Validation

### Run Tests
```bash
# Test with small sample
python test_processor.py

# Test individual components
python property_processor.py
python niche_processor.py
```

### Validate Against SQL Results
1. Run both systems on the same data
2. Compare priority distributions
3. Spot-check individual classifications
4. Review data quality reports

## 🐛 Troubleshooting

### Common Issues

**Unicode Errors:** 
- Ensure your terminal supports UTF-8
- Run with `python -X utf8 main_processor.py`

**Memory Issues with Large Files:**
- Process main region separately: `--main-only`
- Increase system memory or use chunked processing

**File Not Found:**
- Check Excel files are in `Excel files/` directory
- Verify file names match expected patterns

**Classification Issues:**
- Review `property_processor.py` keyword lists
- Check test results with `test_processor.py`
- Examine processing logs for details

### Getting Help
1. Check `processing.log` for detailed error messages
2. Review data quality reports for issues
3. Run test scripts to isolate problems
4. Compare small samples with SQL results

## 🔄 Migration from SQL

### Recommended Migration Steps

1. **Parallel Testing**: Run Python system alongside SQL for several cycles
2. **Validation**: Compare results and tune business rules  
3. **Performance Testing**: Verify processing times meet requirements
4. **Training**: Ensure team understands new system
5. **Gradual Migration**: Start with niche lists, then main region
6. **SQL Retirement**: Disable stored procedures once confident

### SQL vs Python Mapping

| SQL Component | Python Equivalent |
|---------------|-------------------|
| `ProcessUploadLog_Newtest` | `main_processor.py --main-only` |
| `GetFinalizedNicheList` | `main_processor.py --niches-only` |
| Trust/Church/Business logic | `PropertyClassifier` class |
| Priority scoring rules | `PropertyPriorityScorer` class |
| Distress indicators | `DistressIndicatorEngine` class |

## 🎯 Next Steps

1. **Test with your full 200k main region file**
2. **Validate business rules against known good results**  
3. **Tune parameters for your specific market**
4. **Add any missing niche list types**
5. **Implement address validation integration**
6. **Consider adding automated scheduling**

---

**Questions?** Review the code comments and logs, or test with small samples first.

**Performance Issues?** Try processing files separately or in smaller batches.

**Need New Features?** The modular Python design makes it easy to extend.