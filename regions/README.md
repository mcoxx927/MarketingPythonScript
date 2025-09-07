# Roanoke City, VA - Region Files

## File Structure

Place your monthly Excel files in this directory using the following naming convention:

### Required Files:
- **`main_region.xlsx`** - Main property export file (~21k+ records)
  
### Optional Niche Files:
- **`liens.xlsx`** - Properties with liens
- **`foreclosure.xlsx`** - Pre-foreclosure properties  
- **`bankruptcy.xlsx`** - Properties with bankruptcy
- **`tax_delinquencies.xlsx`** - Tax delinquent properties
- **`landlords.xlsx`** - Tired landlord properties
- **`probate.xlsx`** - Probate properties
- **`cash_buyers.xlsx`** - Cash buyer properties
- **`interfamily.xlsx`** - Inter-family transfers

## Processing

To process this region:
```bash
python monthly_processing_v2.py --region roanoke_city_va
```

## Configuration

The `config.json` file contains region-specific settings:
- Date cutoffs for ABS1 and BUY1/BUY2 classifications
- Amount thresholds for priority scoring
- Market-specific parameters

## Output

Results will be saved to: `output/roanoke_city_va/YYYY_MM/`