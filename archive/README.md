# Archive Directory

This directory contains files from the development and testing phase of the multi-region processing system.

## Contents

### Test/Development Python Files
- `test_*.py` - Test scripts used during development
- `check_*.py` - Utility scripts for checking system behavior

### Old Output Files  
- `*.xlsx` - Output files from single-region system testing
- These were generated before the multi-region system was implemented

### Purpose
These files are archived for reference but are not needed for production use of the multi-region system.

### Cleanup Date
Files archived on: 2025-09-03

## Current System
The production system now uses:
- `monthly_processing_v2.py` - Multi-region processor
- `multi_region_config.py` - Region configuration system  
- `regions/` directory structure for organized file management
- `output/region_name/YYYY_MM/` for organized output