"""
Enhanced Property Processor with Boolean Flag Architecture

This module extends the existing property processor to support:
1. Boolean distress indicator flags (HasLiens, HasCodeEnforcement, etc.)
2. Raw land detection and separate priority codes
3. Clean base priority codes separate from distress flags
4. Database-ready output format aligned with SQL migration plan

Architecture:
- BasePriorityCode: ABS1, BUY2, OWN1 (developed) or LAND1, LAND2 (raw land)
- Boolean flags: HasLiens, HasForeclosure, HasCodeEnforcement, etc.
- PropertyCategory: "DEVELOPED" or "RAW_LAND" 
- Legacy PriorityCode: Compound string for Excel readability
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pathlib import Path

# Import existing classes
from property_processor import PropertyClassifier, PropertyPriorityScorer, PropertyClassification, PropertyPriority

logger = logging.getLogger(__name__)


@dataclass
class EnhancedPropertyRecord:
    """Enhanced property record with boolean distress flags"""
    # Base classification (mutually exclusive)
    base_priority_code: str      # ABS1, BUY2, LAND1, etc.
    base_priority_id: int        # 1-13 for sorting
    property_category: str       # "DEVELOPED" or "RAW_LAND"
    
    # Boolean distress flags (combinable)
    has_liens: bool = False
    has_foreclosure: bool = False
    has_code_enforcement: bool = False
    has_current_tax: bool = False
    has_tax_history: bool = False
    has_bankruptcy: bool = False
    has_st_bankruptcy: bool = False
    has_st_foreclosure: bool = False
    has_st_lien: bool = False
    has_st_judgment: bool = False
    has_st_quitclaim: bool = False
    has_st_deceased: bool = False
    has_cash_buyer: bool = False
    has_inter_family: bool = False
    has_landlord: bool = False
    has_probate: bool = False
    has_inherited: bool = False
    
    # Derived fields
    legacy_priority_code: str = ""    # Compound code for Excel
    legacy_priority_name: str = ""    # Human readable description


class RawLandDetector:
    """Detects raw land properties from address and GIS data"""
    
    @staticmethod
    def is_raw_land_by_address(address: str) -> bool:
        """Detect raw land by lack of street number in address"""
        if not address or pd.isna(address):
            return False
        
        address = str(address).strip()
        if not address:
            return False
        
        # Check if first word/token contains a digit (street number)
        first_token = address.split()[0] if address.split() else ""
        return not any(char.isdigit() for char in first_token)
    
    @staticmethod 
    def is_raw_land_by_gis(gis_row: pd.Series) -> bool:
        """Detect raw land using GIS property type data"""
        if gis_row is None:
            return False
        if gis_row.empty:
            return False
        
        # Check GIS property description
        prop_type = str(gis_row.get('PROPERTYDE', '')).upper()
        land_keywords = ['VACANT', 'LAND', 'LOT', 'UNDEVELOPED', 'RAW']
        
        return any(keyword in prop_type for keyword in land_keywords)
    
    @classmethod
    def categorize_property(cls, address: str, gis_row: pd.Series = None) -> str:
        """Categorize property as DEVELOPED or RAW_LAND"""
        # Primary detection: address analysis
        if cls.is_raw_land_by_address(address):
            return "RAW_LAND"
        
        # Secondary detection: GIS data if available
        if gis_row is not None and cls.is_raw_land_by_gis(gis_row):
            return "RAW_LAND"
        
        return "DEVELOPED"


class EnhancedPropertyPriorityScorer:
    """Enhanced priority scorer supporting raw land and boolean flags"""
    
    def __init__(self, region_config: Dict[str, Any]):
        self.region_config = region_config
        self.region_input_date1 = pd.to_datetime(region_config['region_input_date1'])
        self.region_input_date2 = pd.to_datetime(region_config['region_input_date2'])
        self.region_input_amount1 = region_config['region_input_amount1']
        self.region_input_amount2 = region_config['region_input_amount2']
        
        # Create legacy scorer once during initialization to avoid repeated logging
        self.legacy_scorer = PropertyPriorityScorer(
            region_input_date1=self.region_input_date1,
            region_input_date2=self.region_input_date2,
            region_input_amount1=self.region_input_amount1,
            region_input_amount2=self.region_input_amount2
        )
        
        # Raw land uses PropertyCategory for separation - no special priority codes needed
        
        # Standard developed property priorities (existing logic)
        self.developed_priorities = {
            1: "OIN1 - Owner Occupied Grantor Match",
            2: "OWN1 - Owner Occupied Old Property",
            3: "OON1 - Owner Occupied Low Value",
            4: "BUY2 - Recent Non-Cash Buyer",
            5: "TRS1 - Business Trust Property", 
            6: "INH1 - Absentee Grantor Match",
            7: "ABS1 - High Priority Absentee",
            8: "TRS1 - Absentee Low Value",
            9: "BUY1 - Recent Cash Buyer",
            10: "CHU1 - Church Property",
            11: "OWN11 - Standard Owner Occupied",
            12: "ABS12 - Standard Absentee", 
            13: "OWN20 - Very Old Owner Occupied"
        }
    
    def score_property(self, row: pd.Series, classification: PropertyClassification, 
                      property_category: str) -> EnhancedPropertyRecord:
        """Score property and return enhanced record with boolean flags"""
        
        if property_category == "RAW_LAND":
            return self._score_raw_land(row, classification)
        else:
            return self._score_developed_property(row, classification)
    
    def _score_raw_land(self, row: pd.Series, classification: PropertyClassification) -> EnhancedPropertyRecord:
        """Score raw land properties - use default priority, PropertyCategory handles separation"""
        
        # Raw land gets default priority - PropertyCategory="RAW_LAND" handles the separation
        return EnhancedPropertyRecord(
            base_priority_code="DEFAULT",
            base_priority_id=11,  # Standard default priority
            property_category="RAW_LAND"
        )
    
    def _score_developed_property(self, row: pd.Series, classification: PropertyClassification) -> EnhancedPropertyRecord:
        """Score developed properties with standard priority codes"""
        
        # Use the pre-created legacy scorer to avoid repeated initialization and logging
        legacy_priority = self.legacy_scorer.score_property(row, classification)
        
        # Extract base code (remove any existing compound parts)
        base_code = legacy_priority.priority_code.split('-')[-1] if '-' in legacy_priority.priority_code else legacy_priority.priority_code
        
        return EnhancedPropertyRecord(
            base_priority_code=base_code,
            base_priority_id=legacy_priority.priority_id,
            property_category="DEVELOPED"
        )
    
    def _parse_date(self, date_val) -> datetime:
        """Parse date value - reuse existing logic"""
        if pd.isna(date_val) or date_val == '' or date_val is None:
            return datetime(1850, 1, 1)  # Very old sentinel date
        
        try:
            parsed_date = pd.to_datetime(date_val)
            if parsed_date.year <= 1900:
                return datetime(1850, 1, 1)
            return parsed_date
        except:
            return datetime(1850, 1, 1)
    
    def _parse_amount(self, amount_val) -> Optional[float]:
        """Parse amount value - reuse existing logic"""
        if pd.isna(amount_val) or amount_val == '' or amount_val is None:
            return None
        
        try:
            if isinstance(amount_val, str):
                amount_val = amount_val.replace('$', '').replace(',', '')
            return float(amount_val)
        except:
            return None


class DistressFlagManager:
    """Manages distress indicator boolean flags"""
    
    def __init__(self):
        # Mapping of niche types to boolean flags
        self.niche_flag_mapping = {
            'Liens': 'has_liens',
            'PreForeclosure': 'has_foreclosure',
            'CodeEnforcement': 'has_code_enforcement', 
            'CurrentTax': 'has_current_tax',
            'TaxHistory': 'has_tax_history',
            'Bankruptcy': 'has_bankruptcy',
            'CashBuyer': 'has_cash_buyer',
            'InterFamily': 'has_inter_family',
            'Landlord': 'has_landlord',
            'Probate': 'has_probate'
        }
        
        # Skip trace flag mapping
        self.skip_trace_flag_mapping = {
            'STBankruptcy': 'has_st_bankruptcy',
            'STForeclosure': 'has_st_foreclosure',
            'STLien': 'has_st_lien',
            'STJudgment': 'has_st_judgment', 
            'STQuitclaim': 'has_st_quitclaim',
            'STDeceased': 'has_st_deceased'
        }
    
    def apply_niche_flag(self, record: EnhancedPropertyRecord, niche_type: str) -> None:
        """Apply niche distress flag to record"""
        if niche_type in self.niche_flag_mapping:
            flag_name = self.niche_flag_mapping[niche_type]
            setattr(record, flag_name, True)
    
    def apply_skip_trace_flags(self, record: EnhancedPropertyRecord, st_flags: List[str]) -> None:
        """Apply skip trace distress flags to record"""
        for flag in st_flags:
            if flag in self.skip_trace_flag_mapping:
                flag_name = self.skip_trace_flag_mapping[flag]
                setattr(record, flag_name, True)
    
    def get_active_flags(self, record: EnhancedPropertyRecord) -> List[str]:
        """Get list of active distress flags for record"""
        active_flags = []
        
        # Check niche flags
        for niche_type, flag_name in self.niche_flag_mapping.items():
            if getattr(record, flag_name, False):
                active_flags.append(niche_type)
        
        # Check skip trace flags  
        for st_flag, flag_name in self.skip_trace_flag_mapping.items():
            if getattr(record, flag_name, False):
                active_flags.append(st_flag)
        
        return active_flags
    
    def generate_legacy_priority_code(self, record: EnhancedPropertyRecord) -> str:
        """Generate legacy compound priority code for Excel readability"""
        active_flags = self.get_active_flags(record)
        
        if active_flags:
            flag_string = "-".join(active_flags)
            return f"{flag_string}-{record.base_priority_code}"
        else:
            return record.base_priority_code


class EnhancedPropertyProcessor:
    """Main processor with boolean flag architecture"""
    
    def __init__(self, region_config: Dict[str, Any]):
        self.region_config = region_config
        self.classifier = PropertyClassifier()
        self.scorer = EnhancedPropertyPriorityScorer(region_config)
        self.flag_manager = DistressFlagManager()
    
    def process_property(self, row: pd.Series, gis_row: pd.Series = None) -> EnhancedPropertyRecord:
        """Process a single property record"""
        
        # Extract owner name and grantor name from the row
        owner_last = str(row.get('Owner 1 Last Name', ''))
        owner_first = str(row.get('Owner 1 First Name', ''))
        owner_name = f"{owner_first} {owner_last}".strip()
        grantor_name = str(row.get('Grantor', '')) if 'Grantor' in row else None
        
        # Classify property (trust, church, business detection)
        classification = self.classifier.classify_property(owner_name, grantor_name)
        
        # Override owner occupancy from pre-computed column in main file
        owner_occupied_value = row.get('Owner Occupied', 'No')
        classification.is_owner_occupied = (str(owner_occupied_value).lower() == 'yes')
        
        # Determine if raw land or developed
        address = str(row.get('Address', ''))
        property_category = RawLandDetector.categorize_property(address, gis_row)
        
        # Score property
        record = self.scorer.score_property(row, classification, property_category)
        
        return record
    
    def to_dataframe_record(self, enhanced_record: EnhancedPropertyRecord, 
                          original_row: pd.Series) -> Dict[str, Any]:
        """Convert enhanced record to dataframe row format"""
        
        # Generate legacy priority code
        legacy_code = self.flag_manager.generate_legacy_priority_code(enhanced_record)
        enhanced_record.legacy_priority_code = legacy_code
        
        # Start with original row data
        record = original_row.to_dict()
        
        # Add enhanced fields
        record.update({
            # Property classification
            'PropertyCategory': enhanced_record.property_category,
            
            # Boolean distress flags  
            'HasLiens': enhanced_record.has_liens,
            'HasForeclosure': enhanced_record.has_foreclosure,
            'HasCodeEnforcement': enhanced_record.has_code_enforcement,
            'HasCurrentTax': enhanced_record.has_current_tax,
            'HasTaxHistory': enhanced_record.has_tax_history,
            'HasBankruptcy': enhanced_record.has_bankruptcy,
            'HasCashBuyer': enhanced_record.has_cash_buyer,
            'HasInterFamily': enhanced_record.has_inter_family,
            'HasLandlord': enhanced_record.has_landlord,
            'HasProbate': enhanced_record.has_probate,
            'HasInherited': enhanced_record.has_inherited,
            'HasSTBankruptcy': enhanced_record.has_st_bankruptcy,
            'HasSTForeclosure': enhanced_record.has_st_foreclosure,
            'HasSTLien': enhanced_record.has_st_lien,
            'HasSTJudgment': enhanced_record.has_st_judgment,
            'HasSTQuitclaim': enhanced_record.has_st_quitclaim,
            'HasSTDeceased': enhanced_record.has_st_deceased,
            
            # Single priority system (no more duplicates)
            'PriorityCode': enhanced_record.base_priority_code,  # Clean base code (ABS1, BUY2, DEFAULT)
            'PriorityId': enhanced_record.base_priority_id,     # Numeric priority (1-13)
            'PriorityName': self._generate_priority_name(enhanced_record)
        })
        
        return record
    
    def _generate_priority_name(self, record: EnhancedPropertyRecord) -> str:
        """Generate human-readable priority name"""
        active_flags = self.flag_manager.get_active_flags(record)
        
        if record.property_category == "RAW_LAND":
            base_name = f"Raw Land Property"  # Simple name for raw land
        else:
            base_name = f"{record.base_priority_code} - Developed Property"
        
        if active_flags:
            flag_desc = " + ".join(active_flags)
            return f"{flag_desc} Enhanced - {base_name}"
        else:
            return base_name
    
    def process_excel_file(self, file_path: str) -> pd.DataFrame:
        """
        Process a single Excel file and return enhanced data with boolean flag architecture.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            DataFrame with boolean flag columns and separated raw land handling
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"[ENHANCED PROCESSING] Starting file: {Path(file_path).name}")
        
        try:
            # Validate file exists and is readable
            if not Path(file_path).exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Read Excel file 
            df = pd.read_excel(file_path, dtype={'FIPS': 'category'})
            logger.info(f"[ENHANCED PROCESSING] Loaded {len(df):,} records")
            
            # Validate required columns
            required_columns = ['Owner 1 Last Name', 'Owner 1 First Name', 'Address']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Process each row and collect enhanced records
            enhanced_records = []
            total_processed = 0
            
            for idx, row in df.iterrows():
                try:
                    # Process property with enhanced architecture
                    enhanced_record = self.process_property(row)
                    
                    # Convert to dataframe format
                    record_dict = self.to_dataframe_record(enhanced_record, row)
                    enhanced_records.append(record_dict)
                    
                    total_processed += 1
                    
                    # Progress logging every 5000 records
                    if total_processed % 5000 == 0:
                        logger.info(f"[ENHANCED PROCESSING] Processed {total_processed:,} records...")
                        
                except Exception as row_error:
                    logger.warning(f"[ENHANCED PROCESSING] Error processing row {idx}: {row_error}")
                    continue
            
            # Create enhanced DataFrame
            if not enhanced_records:
                logger.error("[ENHANCED PROCESSING] No records could be processed")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(enhanced_records)
            
            # Log processing summary
            developed_count = len(result_df[result_df['PropertyCategory'] == 'DEVELOPED'])
            raw_land_count = len(result_df[result_df['PropertyCategory'] == 'RAW_LAND'])
            
            logger.info(f"[ENHANCED PROCESSING] Complete:")
            logger.info(f"  Total processed: {len(result_df):,}")
            logger.info(f"  Developed properties: {developed_count:,} ({developed_count/len(result_df)*100:.1f}%)")
            logger.info(f"  Raw land parcels: {raw_land_count:,} ({raw_land_count/len(result_df)*100:.1f}%)")
            
            return result_df
            
        except Exception as e:
            logger.error(f"[ENHANCED PROCESSING] Failed to process {file_path}: {e}")
            raise