"""
Real Estate Direct Mail Property Processor

This module processes property data from Excel files and applies business rules
to classify properties and assign priority scores for direct mail campaigns.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PropertyClassification:
    """Holds classification results for a property"""
    is_trust: bool = False
    is_church: bool = False 
    is_business: bool = False
    is_owner_occupied: bool = False
    owner_grantor_match: bool = False

@dataclass  
class PropertyPriority:
    """Holds priority assignment results"""
    priority_id: int = 11  # Default priority
    priority_code: str = "DEFAULT"
    priority_name: str = "Default"

class PropertyClassifier:
    """
    Handles property classification based on owner name patterns.
    Based on SQL logic from stored procedures.
    """
    
    # Trust keywords from SQL
    TRUST_KEYWORDS = [
        'trus', 'estate', 'decl', 'supplemental', 'living', 'amend', 
        'life', 'TRS', 'execut', 'revoc', 'irrev'
    ]
    
    # Church keywords from SQL
    CHURCH_KEYWORDS = [
        'church', 'evangel', 'presbyterian', 'bible', 'episcopal', 'dioce',
        'protestant', 'trinity', 'holy', 'jerusalum', 'baptist', 'lutheran',
        'nazar', ' god ', 'convenant', 'ministry', ' christ '
    ]
    
    # Church ending patterns
    CHURCH_ENDINGS = [' christ', ' god']
    
    # Business keywords from SQL
    BUSINESS_KEYWORDS = [
        'roanoke', 'llc', 'housing', 'develop', 'author', 'planning',
        'district', 'commiss', 'partner', 'group', 'condo', 'city',
        'real', 'holding', 'company', ' inc ', ' co ', ' tc ',
        ' bank ', 'proprietor', 'propert', 'foundation', 'commonwealth',
        'clinic', ' office', 'limit', ' ltd', ' health', ' llp',
        ' assoc', ' corp', 'Virginia', 'North Carolina', 'Enterprises',
        'Attorney', 'Credit Union', 'Incorporated', 'Medical', 'Center'
    ]
    
    # Business ending patterns
    BUSINESS_ENDINGS = [' lc', ' inc', ' co', ' tc', ' bank', ' ltd', ' llp']
    
    def classify_property(self, owner_name: str, grantor_name: str = None) -> PropertyClassification:
        """
        Classify a property based on owner name patterns.
        
        Args:
            owner_name: Full owner name
            grantor_name: Grantor name for matching logic
            
        Returns:
            PropertyClassification object with classification results
        """
        if pd.isna(owner_name):
            owner_name = ""
        
        owner_name = str(owner_name).lower()
        classification = PropertyClassification()
        
        # Check for trust
        classification.is_trust = self._is_trust(owner_name)
        
        # Check for church (only if not a trust)
        if not classification.is_trust:
            classification.is_church = self._is_church(owner_name)
        
        # Check for business (only if not a church)
        if not classification.is_church:
            classification.is_business = self._is_business(owner_name, classification.is_trust)
            
        # Check grantor match if provided
        if grantor_name:
            classification.owner_grantor_match = self._check_grantor_match(owner_name, grantor_name)
            
        return classification
    
    def _is_trust(self, owner_name: str) -> bool:
        """Check if owner name indicates a trust"""
        return any(keyword in owner_name for keyword in self.TRUST_KEYWORDS)
    
    def _is_church(self, owner_name: str) -> bool:
        """Check if owner name indicates a church"""
        # Check contains patterns
        if any(keyword in owner_name for keyword in self.CHURCH_KEYWORDS):
            return True
        # Check ending patterns    
        return any(owner_name.endswith(ending) for ending in self.CHURCH_ENDINGS)
    
    def _is_business(self, owner_name: str, is_trust: bool) -> bool:
        """Check if owner name indicates a business"""
        # Check contains patterns
        if any(keyword in owner_name for keyword in self.BUSINESS_KEYWORDS):
            return True
        # Check ending patterns
        if any(owner_name.endswith(ending) for ending in self.BUSINESS_ENDINGS):
            return True
        # Special trust logic
        if is_trust and any(pattern in owner_name for pattern in [' the ', ' the', 'the ']):
            return True
        return False
    
    def _check_grantor_match(self, owner_name: str, grantor_name: str) -> bool:
        """
        Check if owner and grantor first words match but full names don't.
        Simplified version of the SQL logic.
        """
        if pd.isna(grantor_name):
            return False
            
        owner_words = owner_name.strip().split()
        grantor_words = str(grantor_name).lower().strip().split()
        
        if not owner_words or not grantor_words:
            return False
            
        # First words match but full names don't match
        return (owner_words[0] == grantor_words[0] and 
                owner_name != str(grantor_name).lower())

class PropertyPriorityScorer:
    """
    Assigns priority scores to properties based on business rules.
    Based on the complex priority logic from the SQL stored procedures.
    
    IMPORTANT DATE CRITERIA (from SQL Region table):
    - region_input_date1: Used for ABS1 (Absentee List 3) - properties with old sale dates
    - region_input_date2: Used for BUY1/BUY2 - properties with recent high-value buyers  
    - region_input_amount1: Low sale amount threshold (TRS1, OON1)
    - region_input_amount2: High sale amount threshold (BUY1, BUY2)
    """
    
    def __init__(self, region_input_date1=None, region_input_date2=None, 
                 region_input_amount1=75000, region_input_amount2=200000):
        """
        Initialize with region-specific parameters.
        These parameters were stored in the SQL Region table.
        
        Args:
            region_input_date1: Cutoff for ABS1 - properties older than this date
            region_input_date2: Cutoff for BUY1/BUY2 - properties newer than this date  
            region_input_amount1: Low amount threshold (typically $75k for Roanoke)
            region_input_amount2: High amount threshold (typically $200k+ for cash buyers)
        """
        # Set region-specific dates based on SQL stored procedure defaults
        if region_input_date1 is None:
            # ABS1 looks for properties sold before this date (15 years ago typical)
            region_input_date1 = datetime.now() - timedelta(days=365*15)  
        if region_input_date2 is None:
            # BUY1/BUY2 look for properties sold after this date (5 years ago typical)  
            region_input_date2 = datetime.now() - timedelta(days=365*5)   
            
        self.region_input_date1 = region_input_date1
        self.region_input_date2 = region_input_date2  
        self.region_input_amount1 = region_input_amount1
        self.region_input_amount2 = region_input_amount2
        
        logger.info(f"Priority Scorer Configuration:")
        logger.info(f"  ABS1 date cutoff (old sales): {region_input_date1.strftime('%Y-%m-%d')}")
        logger.info(f"  BUY1/BUY2 date cutoff (recent): {region_input_date2.strftime('%Y-%m-%d')}")
        logger.info(f"  Low amount threshold: ${region_input_amount1:,}")
        logger.info(f"  High amount threshold: ${region_input_amount2:,}")
        
        # Priority definitions from SQL
        self.priorities = {
            1: "OIN1 - Owner-Occupant List 1",      # Owner occupied + grantor match
            2: "OWN1 - Owner-Occupant List 3",      # Owner occupied + old sale date
            3: "OON1 - Owner-Occupant List 4",      # Owner occupied + low sale amount 
            4: "BUY2 - Owner-Occupant List 5",      # Owner occupied + recent high buyer
            5: "TRS2 - Trust",                      # Trusts
            6: "INH1 - Absentee List 1",           # Absentee + grantor match
            7: "ABS1 - Absentee List 3",           # Absentee + old sale date
            8: "TRS1 - Absentee List 4",           # Absentee + low sale amount
            9: "BUY1 - Absentee List 5",           # Absentee + recent high buyer
            10: "CHURCH - Church",                  # Churches
            11: "DEFAULT - Default",                # Default/unclassified
            13: "OWN20 - Owner-Occupant List 20"    # Very old owner occupied (20+ years)
        }
    
    def score_property(self, row: pd.Series, classification: PropertyClassification) -> PropertyPriority:
        """
        Score a property and assign priority based on business rules.
        
        Args:
            row: Property data row
            classification: PropertyClassification result
            
        Returns:
            PropertyPriority object with assigned priority
        """
        # Start with default priority
        priority_id = 11
        
        # Trust properties get priority 5
        if classification.is_trust:
            priority_id = 5
            
        # Church properties get priority 10    
        elif classification.is_church:
            priority_id = 10
            
        # Owner occupied properties
        elif classification.is_owner_occupied:
            priority_id = self._score_owner_occupied(row, classification)
            
        # Absentee (non-owner occupied) properties
        else:
            priority_id = self._score_absentee(row, classification)
        
        return PropertyPriority(
            priority_id=priority_id,
            priority_code=self.priorities[priority_id].split(' - ')[0],
            priority_name=self.priorities[priority_id]
        )
    
    def _score_owner_occupied(self, row: pd.Series, classification: PropertyClassification) -> int:
        """Score owner occupied properties"""
        # OIN1: Owner occupied + grantor match = Priority 1
        if classification.owner_grantor_match:
            return 1
            
        # Get sale information
        sale_date = self._parse_date(row.get('Last Sale Date'))
        sale_amount = self._parse_amount(row.get('Last Sale Amount'))
        
        # OWN20: Very old properties (20+ years) = Priority 13
        if sale_date <= datetime.now() - timedelta(days=365*20):
            return 13
            
        # OWN1: Properties with old sale dates (13+ years) = Priority 2  
        if sale_date <= datetime.now() - timedelta(days=365*13):
            return 2
            
        # OON1: Properties with low sale amounts = Priority 3
        if sale_amount and sale_amount <= self.region_input_amount1:
            return 3
            
        # BUY2: Recent high value buyers = Priority 4
        if (sale_amount and sale_amount >= self.region_input_amount2 and
            sale_date >= self.region_input_date2):
            return 4
            
        return 11  # Default
    
    def _score_absentee(self, row: pd.Series, classification: PropertyClassification) -> int:
        """Score absentee (non-owner occupied) properties"""
        # INH1: Absentee + grantor match = Priority 6
        if classification.owner_grantor_match:
            return 6
            
        # Get sale information
        sale_date = self._parse_date(row.get('Last Sale Date'))
        sale_amount = self._parse_amount(row.get('Last Sale Amount'))
        
        # ABS1: Absentee with old sale dates = Priority 7
        # Note: blank/1900 dates are parsed as 1850-01-01, so they qualify as "old"
        if sale_date <= self.region_input_date1:
            return 7
            
        # TRS1: Absentee with low sale amounts = Priority 8
        if sale_amount and sale_amount <= self.region_input_amount1:
            return 8
            
        # BUY1: Absentee recent high value buyers = Priority 9
        if (sale_amount and sale_amount >= self.region_input_amount2 and
            sale_date >= self.region_input_date2):
            return 9
            
        return 11  # Default
    
    def _parse_date(self, date_val) -> datetime:
        """
        Parse date value to datetime object with proper blank/invalid handling.
        
        IMPORTANT: Blank dates and 1900-01-01 dates should be treated as "very old" 
        dates that qualify for high-priority lists (ABS1, OWN1, etc.). We return
        a very old sentinel date (1850-01-01) for these cases so they pass
        "older than threshold" checks.
        """
        if pd.isna(date_val) or date_val == '' or date_val is None:
            # Return very old sentinel date for blank values
            return datetime(1850, 1, 1)
            
        try:
            parsed_date = pd.to_datetime(date_val)
            
            # Handle SQL sentinel dates (1900-01-01) as very old dates
            if parsed_date.year <= 1900:
                return datetime(1850, 1, 1)
                
            # Sanity check - future dates are probably errors
            if parsed_date > datetime.now():
                logger.debug(f"Future date detected: {parsed_date}, treating as very old")
                return datetime(1850, 1, 1)
                
            return parsed_date
            
        except (ValueError, TypeError):
            logger.debug(f"Could not parse date: {date_val}, treating as very old")
            return datetime(1850, 1, 1)
    
    def _parse_amount(self, amount_val) -> Optional[float]:
        """
        Parse amount value to float with proper blank/invalid handling.
        """
        if pd.isna(amount_val) or amount_val == '' or amount_val is None:
            return None
            
        try:
            # Handle string amounts with commas, dollar signs, etc.
            if isinstance(amount_val, str):
                cleaned = amount_val.replace(',', '').replace('$', '').strip()
                if not cleaned or cleaned.lower() in ['null', 'none', 'n/a']:
                    return None
                amount = float(cleaned)
            else:
                amount = float(amount_val)
            
            # Negative amounts are probably errors
            if amount < 0:
                return None
                
            return amount
            
        except (ValueError, TypeError):
            logger.debug(f"Could not parse amount: {amount_val}")
            return None

class PropertyProcessor:
    """
    Main property processing class that orchestrates the entire pipeline.
    """
    
    def __init__(self, region_input_date1=None, region_input_date2=None, 
                 region_input_amount1=75000, region_input_amount2=200000):
        """
        Initialize processor with region-specific parameters.
        
        Args:
            region_input_date1: Date cutoff for ABS1 (old sales)
            region_input_date2: Date cutoff for BUY1/BUY2 (recent buyers)  
            region_input_amount1: Low amount threshold
            region_input_amount2: High amount threshold
        """
        self.classifier = PropertyClassifier()
        self.scorer = PropertyPriorityScorer(
            region_input_date1=region_input_date1,
            region_input_date2=region_input_date2,
            region_input_amount1=region_input_amount1,
            region_input_amount2=region_input_amount2
        )
        
    def process_excel_file(self, file_path: str) -> pd.DataFrame:
        """
        Process a single Excel file and return enhanced data with classifications and priorities.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            DataFrame with additional columns for classification and priority
        """
        logger.info(f"Processing file: {file_path}")
        
        # Read Excel file
        df = pd.read_excel(file_path)
        logger.info(f"Loaded {len(df)} records")
        
        # Create owner name from first and last name
        df['OwnerName'] = (df['Owner 1 Last Name'].fillna('') + ' ' + 
                          df['Owner 1 First Name'].fillna('')).str.strip()
        
        # Initialize new columns
        df['IsTrust'] = False
        df['IsChurch'] = False
        df['IsBusiness'] = False  
        df['IsOwnerOccupied'] = False
        df['OwnerGrantorMatch'] = False
        df['PriorityId'] = 11
        df['PriorityCode'] = 'DEFAULT'
        df['PriorityName'] = 'Default'
        
        # Add date/amount tracking columns
        df['ParsedSaleDate'] = None
        df['ParsedSaleAmount'] = None
        df['DateParseIssues'] = ''
        
        # Process each row
        for idx, row in df.iterrows():
            if idx % 1000 == 0:  # Progress logging
                logger.info(f"Processed {idx} records...")
                
            # Classify property
            classification = self.classifier.classify_property(
                row['OwnerName'], 
                row.get('Grantor')  # This column might not exist in Excel
            )
            
            # Determine owner occupancy (simplified logic - compare addresses)
            classification.is_owner_occupied = self._check_owner_occupancy(row)
            
            # Parse and validate dates/amounts with tracking
            parsed_date = self.scorer._parse_date(row.get('Last Sale Date'))
            parsed_amount = self.scorer._parse_amount(row.get('Last Sale Amount'))
            
            # Track parsing issues for data quality reporting
            issues = []
            if pd.notna(row.get('Last Sale Date')) and parsed_date is None:
                issues.append('InvalidDate')
            if pd.notna(row.get('Last Sale Amount')) and parsed_amount is None:
                issues.append('InvalidAmount')
            
            # Score priority (using original row data - scorer handles parsing internally)
            priority = self.scorer.score_property(row, classification)
            
            # Update DataFrame
            df.at[idx, 'IsTrust'] = classification.is_trust
            df.at[idx, 'IsChurch'] = classification.is_church  
            df.at[idx, 'IsBusiness'] = classification.is_business
            df.at[idx, 'IsOwnerOccupied'] = classification.is_owner_occupied
            df.at[idx, 'OwnerGrantorMatch'] = classification.owner_grantor_match
            df.at[idx, 'PriorityId'] = priority.priority_id
            df.at[idx, 'PriorityCode'] = priority.priority_code
            df.at[idx, 'PriorityName'] = priority.priority_name
            
            # Update parsing tracking columns
            df.at[idx, 'ParsedSaleDate'] = parsed_date
            df.at[idx, 'ParsedSaleAmount'] = parsed_amount
            df.at[idx, 'DateParseIssues'] = ';'.join(issues)
        
        # Log data quality statistics
        parsing_issues = df['DateParseIssues'].str.len() > 0
        if parsing_issues.any():
            issue_count = parsing_issues.sum()
            logger.warning(f"Date/amount parsing issues found in {issue_count} records ({issue_count/len(df)*100:.1f}%)")
            
            # Break down by issue type
            date_issues = df['DateParseIssues'].str.contains('InvalidDate', na=False).sum()
            amount_issues = df['DateParseIssues'].str.contains('InvalidAmount', na=False).sum()
            
            if date_issues > 0:
                logger.warning(f"  Invalid dates: {date_issues} records")
            if amount_issues > 0:
                logger.warning(f"  Invalid amounts: {amount_issues} records")
        
        logger.info("Processing complete!")
        return df
    
    def _check_owner_occupancy(self, row: pd.Series) -> bool:
        """
        Simple owner occupancy check by comparing property and mailing addresses.
        This is a simplified version of the complex SQL logic.
        """
        prop_addr = str(row.get('Address', '')).strip().lower()
        mail_addr = str(row.get('Mailing Address', '')).strip().lower()
        
        if not prop_addr or not mail_addr:
            return False
            
        # Skip PO Box addresses
        if mail_addr.startswith('po ') or mail_addr.startswith('p o '):
            return False
            
        # Simple string comparison (SQL has more complex logic)
        return prop_addr == mail_addr
    
    def process_niche_files(self, niche_file_paths: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Process multiple niche list files.
        
        Args:
            niche_file_paths: List of paths to niche Excel files
            
        Returns:
            Dictionary mapping file names to processed DataFrames
        """
        results = {}
        
        for file_path in niche_file_paths:
            file_name = file_path.split('\\')[-1] if '\\' in file_path else file_path.split('/')[-1]
            results[file_name] = self.process_excel_file(file_path)
            
        return results


def create_monthly_processor():
    """
    Create a processor with typical Roanoke region settings.
    Customize these dates and amounts based on your market.
    """
    from datetime import datetime, timedelta
    
    # CUSTOMIZE THESE FOR YOUR REGION:
    # These were stored in your SQL Region table
    region_input_date1 = datetime(2009, 1, 1)    # ABS1: Properties sold before this date (old sales)
    region_input_date2 = datetime(2019, 1, 1)    # BUY1/BUY2: Properties sold after this date (recent)
    region_input_amount1 = 75000                  # Low sale threshold (TRS1, OON1)
    region_input_amount2 = 200000                 # High sale threshold (BUY1, BUY2, cash buyers)
    
    return PropertyProcessor(
        region_input_date1=region_input_date1,
        region_input_date2=region_input_date2, 
        region_input_amount1=region_input_amount1,
        region_input_amount2=region_input_amount2
    )

if __name__ == "__main__":
    # Example usage with region configuration
    print("=== MONTHLY PROCESSING EXAMPLE ===")
    
    # Create processor with region-specific settings
    processor = create_monthly_processor()
    
    # Process main region file
    main_file = "Excel files/Property Export Roanoke+City_2C+VA.xlsx"
    result_df = processor.process_excel_file(main_file)
    
    # Show results summary
    print("\n=== PROCESSING RESULTS SUMMARY ===")
    print(f"Total records processed: {len(result_df):,}")
    print(f"Trusts: {result_df['IsTrust'].sum():,}")
    print(f"Churches: {result_df['IsChurch'].sum():,}")
    print(f"Businesses: {result_df['IsBusiness'].sum():,}")
    print(f"Owner Occupied: {result_df['IsOwnerOccupied'].sum():,}")
    
    print("\nPriority Distribution:")
    priority_counts = result_df['PriorityName'].value_counts()
    for priority, count in priority_counts.head(10).items():
        print(f"  {priority}: {count:,}")
    
    # Show date parsing statistics
    print("\nData Quality:")
    total_with_dates = result_df['ParsedSaleDate'].notna().sum()
    total_with_amounts = result_df['ParsedSaleAmount'].notna().sum()
    print(f"  Records with valid sale dates: {total_with_dates:,} ({total_with_dates/len(result_df)*100:.1f}%)")
    print(f"  Records with valid sale amounts: {total_with_amounts:,} ({total_with_amounts/len(result_df)*100:.1f}%)")
    
    parsing_issues = result_df['DateParseIssues'].str.len() > 0
    if parsing_issues.any():
        print(f"  Records with parsing issues: {parsing_issues.sum():,}")
    
    # Save results with timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_file = f"processed_properties_{timestamp}.xlsx"
    result_df.to_excel(output_file, index=False)
    print(f"\nResults saved to: {output_file}")
    
    # Show some examples of each priority for validation
    print("\n=== PRIORITY EXAMPLES (for validation) ===")
    for priority_id in [1, 2, 7, 9]:  # Key priorities to check
        examples = result_df[result_df['PriorityId'] == priority_id].head(2)
        if len(examples) > 0:
            priority_name = examples.iloc[0]['PriorityName']
            print(f"\n{priority_name} (Priority {priority_id}):")
            for _, example in examples.iterrows():
                print(f"  - {example['OwnerName']} | Sale Date: {example.get('Last Sale Date')} | Amount: ${example.get('Last Sale Amount', 0):,}")