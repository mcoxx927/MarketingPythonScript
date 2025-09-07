# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Process single region:**
```bash
python monthly_processing_v2.py --region roanoke_city_va
```

**Process all regions:**
```bash
python monthly_processing_v2.py --all-regions
```

**List available regions:**
```bash
python monthly_processing_v2.py --list-regions
```

**Test region configuration:**
```bash
python multi_region_config.py
```

**Analyze Excel file structure:**
```bash
python analyze_excel_structure.py
```

**Process with legacy single-region system:**
```bash
python main_processor.py
```

**Integrate skip trace data into enhanced files:**
```bash
python skip_trace_processor.py --region roanoke_city_va --skip-trace-file "skip_trace_data.xlsx"
```

**Process skip trace for all regions:**
```bash
python skip_trace_processor.py --all-regions --skip-trace-file "skip_trace_data.xlsx"
```

**Skip trace with specific enhanced file:**
```bash
python skip_trace_processor.py --region roanoke_city_va --enhanced-file "output/roanoke_city_va/2024_01/roa_main_region_enhanced_20240115.xlsx" --skip-trace-file "skip_trace_data.xlsx"
```

## Architecture Overview

This is a multi-region real estate direct mail processing system that replaces SQL stored procedures with Python. The system processes property data and assigns priority scores for marketing campaigns.

### Core Components

**Main Processing Pipeline:**
- `monthly_processing_v2.py` - Multi-region orchestrator with standardized file naming
- `skip_trace_processor.py` - Skip trace data integration with Golden Address and distress flags
- `multi_region_config.py` - Configuration management for 11 regions 
- `property_processor.py` - Core property classification and priority scoring
- `main_processor.py` - Legacy single-region processor (still maintained)

**Configuration System:**
- Each region has its own `regions/{region_key}/config.json` with market-specific thresholds
- Supports different date cutoffs and amount thresholds per market (rural vs metropolitan)
- FIPS code validation prevents processing wrong region data

**Data Flow:**
1. **Main Region Processing** - Processes main property file (~21k records) using PropertyClassifier and PropertyPriorityScorer classes
2. **Niche List Integration** - Overlays distress indicators (liens, foreclosure, bankruptcy) onto existing priorities
3. **Enhanced Priority Codes** - Creates combined codes like "Liens-ABS1" or "Tax-PreForeclosure-BUY2"
4. **Skip Trace Enhancement** - Post-processing integration with enhanced city-aware matching:
   - Hybrid strategy: APN+FIPS (most accurate), Address+City (prevents cross-city false matches)
   - Integrates Golden Address and skip trace distress flags (STBankruptcy, STForeclosure, STLien, STJudgment, STQuitclaim, STDeceased)
   - Typical performance: ~2,600 APN matches + ~1,500 city-validated address matches per region

### Business Logic Architecture

**Property Classification** (`PropertyClassifier` class):
- Trust detection via keyword matching (trust, estate, living, revocable)
- Church detection (church, baptist, ministry, holy)
- Business entity detection (llc, inc, corp, company)
- Owner occupancy determination via address matching

**Priority Scoring** (`PropertyPriorityScorer` class):
- 13 priority levels (1=highest, 13=lowest)
- Based on owner occupancy, sale date, sale amount, and property type
- Market-specific thresholds from region configuration

**Niche Enhancement:**
- Address-based matching between main region and niche files
- Updates existing records with compound priority codes
- Inserts niche-only records not found in main region

**Skip Trace Integration** (`skip_trace_processor.py`):
- Hybrid matching strategy: APN+FIPS (primary), Address+City (fallback)
- City-aware matching prevents cross-city false matches in multi-region skip trace files
- Golden Address integration with difference detection for A/B testing
- Skip trace flag detection: STDeceased, STBankruptcy, STForeclosure, STLien, STJudgment, STQuitclaim
- Priority code enhancement with skip trace flags (e.g., "STBankruptcy-ABS1")

### File Structure

**Input Files:**
- `regions/{region_key}/main_region.xlsx` - Primary property data
- `regions/{region_key}/liens.xlsx` - Properties with liens
- `regions/{region_key}/foreclosure.xlsx` - Pre-foreclosure properties
- Additional niche files detected by filename patterns

**Output Files:**
- `output/{region_key}/YYYY_MM/{region_code}_main_region_enhanced_YYYYMMDD.xlsx`
- `output/{region_key}/YYYY_MM/{region_code}_processing_YYYYMMDD_HHMM.log`
- `output/{region_key}/YYYY_MM/{region_code}_processing_summary_YYYYMMDD.xlsx`

### Region Configuration

Each region config contains:
- Market-specific date cutoffs for ABS1 and BUY priority classifications
- Amount thresholds for price-based classifications  
- FIPS code for data validation
- Market type classification (Rural/Small City, Coastal/Resort, Metro/High-Value)

Example thresholds by market type:
- **Rural markets** (Roanoke): $75k/$200k thresholds, longer holding periods
- **Coastal markets** (Virginia Beach): $150k/$400k thresholds  
- **Metro markets** (Alexandria): $200k/$600k thresholds, shorter holding periods

### Data Validation

**Automatic validations:**
- FIPS code matching between files and region configuration
- Date parsing with defaults for blank values
- Address normalization for matching
- File existence and structure validation

### Migration from SQL

This system replaced SQL stored procedures with these mappings:
- `ProcessUploadLog_Newtest` → `main_processor.py --main-only`
- `GetFinalizedNicheList` → `main_processor.py --niches-only` 
- Trust/Church/Business logic → `PropertyClassifier` class
- Priority scoring rules → `PropertyPriorityScorer` class
- Distress indicators → Niche file integration in `monthly_processing_v2.py`

The Python implementation provides better error handling, debugging capabilities, and maintainability compared to the original SQL system.