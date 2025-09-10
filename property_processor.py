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
from pathlib import Path
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for processing
DEFAULT_PRIORITY_ID = 11
DEFAULT_PRIORITY_CODE = 'DEFAULT'
NICHE_ONLY_PRIORITY_ID = 99
VERY_OLD_DATE = datetime(1850, 1, 1)
PROGRESS_LOG_INTERVAL = 5000

# Data validation constants
REQUIRED_COLUMNS = ['Owner 1 Last Name', 'Owner 1 First Name', 'Address']
RECOMMENDED_COLUMNS = ['Last Sale Date', 'Last Sale Amount', 'Mailing Address', 'FIPS']
MAX_REASONABLE_SALE_AMOUNT = 50_000_000  # $50M seems reasonable as upper bound
MIN_REASONABLE_SALE_DATE = datetime(1800, 1, 1)  # Sanity check for very old dates

@dataclass
class PropertyClassification:
    """Holds classification results for a property"""
    is_trust: bool = False
    is_church: bool = False 
    is_business: bool = False
    is_owner_occupied: bool = False
    owner_grantor_match: bool = False
    is_inherited: bool = False

@dataclass  
class PropertyPriority:
    """Holds priority assignment results"""
    priority_id: int = DEFAULT_PRIORITY_ID
    priority_code: str = DEFAULT_PRIORITY_CODE
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
        ' assoc', ' corp', 'virginia', 'north carolina', 'enterprises',
        'attorney', 'credit union', 'incorporated', 'medical', 'center'
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
        
        # Skip classification for simple personal names to prevent over-matching
        # e.g., "church barbara", "trussell janet", "upchurch david"
        if self._is_likely_personal_name(owner_name):
            # Only check grantor match for personal names
            if grantor_name:
                classification.owner_grantor_match = self._check_grantor_match(owner_name, grantor_name)
            return classification
        
        # Use priority-based classification to prevent multiple entity type matches
        classification = self._classify_with_priority(owner_name, classification)
            
        # Check grantor match if provided
        if grantor_name:
            classification.owner_grantor_match = self._check_grantor_match(owner_name, grantor_name)
            
        return classification
    
    def _is_likely_personal_name(self, owner_name: str) -> bool:
        """
        Check if owner name appears to be a simple personal name (Firstname Lastname).
        This prevents over-matching surnames like 'Church', 'Trussell' as business entities.
        Now more conservative - only blocks obvious personal names.
        """
        # Clean up the name
        name_cleaned = owner_name.strip()
        if not name_cleaned:
            return False
            
        # Split into words and filter out empty strings
        words = [word for word in name_cleaned.split() if word.strip()]
        
        # Only consider exactly 2 words for personal name detection
        if len(words) != 2:
            return False
        
        name_lower = name_cleaned.lower()
        
        # Strong business entity indicators - never classify as personal
        strong_business_indicators = ['llc', 'inc', 'corp', 'ltd', 'company', 'group', 
                                    'holdings', 'properties', 'ventures', 'authority', 
                                    'foundation', 'association', 'partnership', 'enterprises',
                                    'center', 'medical', 'hospital', 'clinic', 'services']
        
        if any(indicator in name_lower for indicator in strong_business_indicators):
            return False
            
        # Trust/Church entity phrases - never classify as personal
        entity_phrases = ['family trust', 'living trust', 'revocable trust', 'estate of',
                        'baptist church', 'methodist church', 'catholic church', 'first church', 
                        'church of', 'ministry of', 'diocese of']
        
        if any(phrase in name_lower for phrase in entity_phrases):
            return False
        
        # Business descriptive words - likely business entities
        business_descriptors = ['construction', 'development', 'management', 'consulting', 
                              'solutions', 'systems', 'technologies', 'industries', 'capital',
                              'investments', 'financial', 'insurance', 'real estate']
        
        if any(descriptor in name_lower for descriptor in business_descriptors):
            return False
            
        # Check if this appears to be an address rather than a person/entity name
        first_word, second_word = words[0].lower(), words[1].lower()
        
        # Address indicators - these should NOT be classified as personal names or entities
        address_suffixes = ['street', 'road', 'avenue', 'lane', 'drive', 'court', 'place',
                          'way', 'circle', 'boulevard', 'parkway', 'terrace', 'trail']
        
        if second_word in address_suffixes or first_word in address_suffixes:
            return False  # Not a personal name, and won't be classified as entity either
        
        # Known surname patterns that trigger keyword matches - these ARE personal names
        problematic_surnames = ['church', 'trussell', 'trussler', 'upchurch', 'churchwell']
        
        if second_word in problematic_surnames or first_word in problematic_surnames:
            return True
            
        # Default: if it's 2 words with no business indicators and no address indicators, likely personal
        return True
    
    def _classify_with_priority(self, owner_name: str, classification: PropertyClassification) -> PropertyClassification:
        """
        Classify entity using priority system to prevent multiple entity type matches.
        Priority order: Business Entity Suffixes > Trust Entities > Church Entities > Partial Matches
        """
        
        # PRIORITY 1: Strong Business Entity Indicators (highest priority)
        if self._has_strong_business_indicators(owner_name):
            classification.is_business = True
            return classification
        
        # PRIORITY 2: Strong Trust Entity Indicators  
        if self._has_strong_trust_indicators(owner_name):
            classification.is_trust = True
            return classification
            
        # PRIORITY 3: Strong Church Entity Indicators
        if self._has_strong_church_indicators(owner_name):
            classification.is_church = True
            return classification
        
        # PRIORITY 4: Weak/Partial Matches (lowest priority)
        # Only apply if no strong indicators found
        
        # Check for weak trust indicators
        if self._has_weak_trust_indicators(owner_name):
            classification.is_trust = True
            return classification
            
        # Check for weak church indicators
        if self._has_weak_church_indicators(owner_name):
            classification.is_church = True
            return classification
            
        # Check for weak business indicators
        if self._has_weak_business_indicators(owner_name):
            classification.is_business = True
            return classification
            
        return classification
    
    def _has_strong_business_indicators(self, owner_name: str) -> bool:
        """Check for strong business entity indicators (LLC, INC, CORP, etc.)"""
        strong_suffixes = ['llc', 'inc', 'corp', 'ltd', 'company', 'enterprises', 
                          'corporation', 'incorporated', 'limited']
        
        # Check for exact business suffixes  
        for suffix in strong_suffixes:
            if owner_name.endswith(' ' + suffix) or owner_name.endswith(suffix):
                return True
                
        # Check for strong business entity phrases
        strong_business_phrases = ['housing authority', 'planning commission', 'city of',
                                 'county of', 'commonwealth of', 'state of', 'credit union',
                                 'medical center', 'hospital system', 'school district']
        
        return any(phrase in owner_name for phrase in strong_business_phrases)
    
    def _has_strong_trust_indicators(self, owner_name: str) -> bool:
        """Check for strong trust entity indicators"""
        strong_trust_phrases = ['family trust', 'living trust', 'revocable trust', 
                              'irrevocable trust', 'testamentary trust', 'estate of',
                              'trust of', 'trust for']
        
        return any(phrase in owner_name for phrase in strong_trust_phrases)
    
    def _has_strong_church_indicators(self, owner_name: str) -> bool:
        """Check for strong church entity indicators"""
        strong_church_phrases = ['baptist church', 'methodist church', 'catholic church',
                               'presbyterian church', 'episcopal church', 'lutheran church',
                               'first church', 'church of', 'diocese of', 'ministry of']
        
        return any(phrase in owner_name for phrase in strong_church_phrases)
    
    def _has_weak_trust_indicators(self, owner_name: str) -> bool:
        """Check for weak trust indicators (partial keyword matches)"""
        # Only match if it seems like a legal context, not an address
        if any(addr_word in owner_name for addr_word in ['street', 'road', 'avenue', 'lane', 'drive']):
            return False
            
        return any(keyword in owner_name for keyword in self.TRUST_KEYWORDS)
    
    def _has_weak_church_indicators(self, owner_name: str) -> bool:
        """Check for weak church indicators (partial keyword matches)"""
        # Skip if it appears to be an address or personal name context
        if any(addr_word in owner_name for addr_word in ['street', 'road', 'avenue', 'lane', 'drive']):
            return False
            
        return any(keyword in owner_name for keyword in self.CHURCH_KEYWORDS)
    
    def _has_weak_business_indicators(self, owner_name: str) -> bool:
        """Check for weak business indicators (partial keyword matches)"""
        # Skip if it appears to be an address context
        if any(addr_word in owner_name for addr_word in ['street', 'road', 'avenue', 'lane', 'drive']):
            return False
            
        return any(keyword in owner_name for keyword in self.BUSINESS_KEYWORDS)
    
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
    
    def detect_inherited_property(self, owner_name: str, grantor1_name: str = '', grantor2_name: str = '') -> bool:
        """
        Detect if property is likely inherited by comparing owner and grantor surnames.
        In GIS data, names are formatted as "LASTNAME FIRSTNAME MIDDLENAME".
        
        Args:
            owner_name: Current owner name
            grantor1_name: First grantor name
            grantor2_name: Second grantor name
            
        Returns:
            True if likely inherited property, False otherwise
        """
        owner_surname = self._extract_surname(owner_name)
        
        # Must have a valid owner surname
        if not owner_surname or len(owner_surname) < 3:
            return False
        
        # Check against grantor1
        if grantor1_name:
            grantor1_surname = self._extract_surname(grantor1_name)
            if grantor1_surname and len(grantor1_surname) >= 3 and owner_surname == grantor1_surname:
                return True
        
        # Check against grantor2
        if grantor2_name:
            grantor2_surname = self._extract_surname(grantor2_name)
            if grantor2_surname and len(grantor2_surname) >= 3 and owner_surname == grantor2_surname:
                return True
        
        return False
    
    def _extract_surname(self, name: str) -> str:
        """
        Extract surname from name where format is "LASTNAME FIRSTNAME MIDDLENAME".
        
        Args:
            name: Full name string
            
        Returns:
            Extracted surname or empty string if not extractable
        """
        if pd.isna(name) or not name:
            return ''
        
        # Clean up the name
        name = str(name).strip().upper()
        
        # Skip business entities and organizations
        business_indicators = [
            'LLC', 'INC', 'CORP', 'LTD', 'COMPANY', 'CO', 'CORPORATION', 
            'INCORPORATED', 'ENTERPRISES', 'HOLDINGS', 'PROPERTIES', 
            'INVESTMENTS', 'GROUP', 'VENTURES', 'AUTHORITY', 'FOUNDATION',
            'ASSOCIATION', 'PARTNERSHIP', 'CENTER', 'MEDICAL', 'HOSPITAL', 
            'CLINIC', 'SERVICES', 'TRUST', 'ESTATE', 'MINISTRY', 'CHURCH'
        ]
        
        if any(indicator in name for indicator in business_indicators):
            return ''
        
        # Skip government entities
        govt_indicators = ['CITY OF', 'COUNTY OF', 'STATE OF', 'VIRGINIA', 'ROANOKE']
        if any(indicator in name for indicator in govt_indicators):
            return ''
        
        # Skip inactive entries
        if '(INACTIVE)' in name or 'INACTIVE' in name or 'MULTIPLE OWNERS' in name:
            return ''
        
        # Handle comma-separated names (Surname, First Middle)
        if ',' in name:
            surname = name.split(',')[0].strip()
            # Validate surname: at least 3 chars for family names, no numbers
            if len(surname) >= 3 and len(surname) <= 25 and not any(char.isdigit() for char in surname):
                return surname
            return ''
        
        # Handle joint ownership with "&" - get first person's surname
        if ' & ' in name:
            first_person = name.split(' & ')[0].strip()
            words = first_person.split()
            if len(words) >= 1:
                first_word = words[0]
                # Validate first word as surname
                if (len(first_word) >= 3 and 
                    len(first_word) <= 25 and 
                    not any(char.isdigit() for char in first_word)):
                    return first_word
            return ''
        
        # For space-separated names, first word is the surname
        words = name.split()
        if len(words) >= 1:
            # Skip if looks like an address
            address_words = ['STREET', 'ROAD', 'AVENUE', 'LANE', 'DRIVE', 'ST', 'RD', 'AVE', 'SW', 'NW', 'SE', 'NE']
            if any(word in address_words for word in words):
                return ''
            
            first_word = words[0]
            # Validate first word as surname
            if (len(first_word) >= 3 and 
                len(first_word) <= 25 and 
                not any(char.isdigit() for char in first_word)):
                return first_word
        
        return ''

class PropertyPriorityScorer:
    """
    Assigns priority scores to properties based on business rules.
    Based on the complex priority logic from the SQL stored procedures.
    
    IMPORTANT DATE CRITERIA (from SQL Region table):
    - region_input_date1: Used for ABS1 (Absentee List 3) - properties with old sale dates
    - region_input_date2: Used for BUY1/BUY2 - properties with recent buyers  
    - region_input_amount1: Low sale amount threshold (TRS1, OON1)
    - region_input_amount2: High sale amount threshold (not used for BUY1/BUY2)
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
            region_input_amount2: High amount threshold (not used for BUY1/BUY2)
        """
        # Set region-specific dates based on SQL stored procedure defaults
        if region_input_date1 is None:
            # ABS1 looks for properties sold before this date (15 years ago typical)
            region_input_date1 = datetime.now() - timedelta(days=365*15)  
        if region_input_date2 is None:
            # BUY1/BUY2 look for properties sold after this date (recent buyers)  
            region_input_date2 = datetime.now() - timedelta(days=365*5)   
            
        self.region_input_date1 = region_input_date1
        self.region_input_date2 = region_input_date2  
        self.region_input_amount1 = region_input_amount1
        self.region_input_amount2 = region_input_amount2
        
        logger.info(f"[CONFIG] Priority Scorer Configuration:")
        logger.info(f"[CONFIG]   ABS1 date cutoff (old sales): {region_input_date1.strftime('%Y-%m-%d')}")
        logger.info(f"[CONFIG]   BUY1/BUY2 date cutoff (recent): {region_input_date2.strftime('%Y-%m-%d')}")
        logger.info(f"[CONFIG]   Low amount threshold: ${region_input_amount1:,}")
        logger.info(f"[CONFIG]   BUY1: Recent cash buyers OR recent absentee buyers")
        
        # Priority definitions from SQL
        self.priorities = {
            1: "OIN1 - Owner-Occupant List 1",      # Owner occupied + grantor match
            2: "OWN1 - Owner-Occupant List 3",      # Owner occupied + old sale date
            3: "OON1 - Owner-Occupant List 4",      # Owner occupied + low sale amount 
            4: "BUY2 - Owner-Occupant List 5",      # Owner occupied + recent non-cash buyer
            5: "TRS2 - Trust",                      # Trusts
            6: "INH1 - Absentee List 1",           # Absentee + grantor match
            7: "ABS1 - Absentee List 3",           # Absentee + old sale date
            8: "TRS1 - Absentee List 4",           # Absentee + low sale amount
            9: "BUY1 - Investor Buyers",           # Recent cash buyers OR recent absentee buyers
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
            
        # BUY1: Owner-occupied recent cash buyers = Priority 9  
        if (sale_date >= self.region_input_date2 and self._is_cash_buyer(row)):
            return 9
            
        # BUY2: Owner-occupied recent non-cash buyers = Priority 4
        if sale_date >= self.region_input_date2:
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
            
        # BUY1: Absentee recent buyers = Priority 9
        if sale_date >= self.region_input_date2:
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
            return VERY_OLD_DATE
            
        try:
            parsed_date = pd.to_datetime(date_val)
            
            # Handle SQL sentinel dates (1900-01-01) as very old dates
            if parsed_date.year <= 1900:
                return VERY_OLD_DATE
                
            # Sanity check - future dates are probably errors
            if parsed_date > datetime.now():
                logger.debug(f"Future date detected: {parsed_date}, treating as very old")
                return VERY_OLD_DATE
                
            return parsed_date
            
        except (ValueError, TypeError):
            logger.debug(f"Could not parse date: {date_val}, treating as very old")
            return VERY_OLD_DATE
    
    def _is_cash_buyer(self, row: pd.Series) -> bool:
        """
        Check if property indicates a cash buyer transaction.
        
        Returns True if Last Cash Buyer field indicates cash purchase.
        """
        # Check Last Cash Buyer field only
        last_cash_buyer = row.get('Last Cash Buyer', '')
        if pd.notna(last_cash_buyer) and str(last_cash_buyer).lower() in ['true', 'yes', '1', 'y']:
            return True
            
        return False
    
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

    def _enhance_priority_with_main_file_fields(self, row: pd.Series, priority_code: str) -> str:
        """
        Enhance priority code with main file field indicators (Vacant, Lien, Bankruptcy, PreForeclosure).
        Similar to niche list processing but for fields within the main file.
        
        Args:
            row: Property data row
            priority_code: Original priority code (e.g., "DEFAULT", "OWN1", "ABS1")
            
        Returns:
            Enhanced priority code with prefixes (e.g., "Vacant-DEFAULT", "Lien-OWN1")
        """
        enhancements = []
        
        # Check Vacant field
        vacant = row.get('Vacant', '')
        if pd.notna(vacant) and str(vacant).lower() in ['yes', 'true', '1', 'y']:
            enhancements.append('Vacant')
        
        # Check Lien Type field  
        lien_type = row.get('Lien Type', '')
        if pd.notna(lien_type) and str(lien_type).strip() != '':
            enhancements.append('Lien')
        
        # Check BK Date field
        bk_date = row.get('BK Date', '')
        if pd.notna(bk_date) and str(bk_date).strip() != '':
            enhancements.append('Bankruptcy')
        
        # Check Pre-FC Recording Date field
        prefc_date = row.get('Pre-FC Recording Date', '')
        if pd.notna(prefc_date) and str(prefc_date).strip() != '':
            enhancements.append('PreForeclosure')
        
        # Build enhanced priority code with prefixes
        if enhancements:
            prefix = '-'.join(enhancements)
            return f"{prefix}-{priority_code}"
        
        return priority_code

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
        logger.info(f"[PROCESSING] Starting file: {Path(file_path).name}")
        
        try:
            # Validate file exists and is readable
            if not Path(file_path).exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Read Excel file with error handling and memory optimization
            try:
                # Use dtype optimization to reduce memory usage
                df = pd.read_excel(file_path, dtype={'FIPS': 'category'})
                
                # Convert only specific columns to category to avoid assignment issues
                # Skip columns we might modify later
                protected_columns = {'Owner 1 Last Name', 'Owner 1 First Name', 'Address', 'Mailing Address'}
                string_columns = df.select_dtypes(include=['object']).columns
                for col in string_columns:
                    if (col not in protected_columns and 
                        df[col].nunique() / len(df) < 0.5):  # Less than 50% unique values
                        df[col] = df[col].astype('category')
                        
            except Exception as e:
                raise ValueError(f"Failed to read Excel file {file_path}: {e}")
            
            if df.empty:
                raise ValueError(f"Excel file is empty: {file_path}")
                
            logger.info(f"[DATA] Loaded {len(df):,} records from {Path(file_path).name}")
            
            # Validate required columns
            self._validate_dataframe_structure(df, file_path)
        
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {e}")
            raise
        
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
        
        try:
            # Vectorized processing for better performance
            logger.info("Starting vectorized property classification...")
            
            # Vectorized classification with error handling
            df['IsTrust'] = df['OwnerName'].apply(lambda x: self.classifier._is_trust(str(x).lower()) if pd.notna(x) else False)
            df['IsChurch'] = df.apply(lambda row: self.classifier._is_church(str(row['OwnerName']).lower()) if pd.notna(row['OwnerName']) and not row['IsTrust'] else False, axis=1)
            df['IsBusiness'] = df.apply(lambda row: self.classifier._is_business(str(row['OwnerName']).lower(), row['IsTrust']) if pd.notna(row['OwnerName']) and not row['IsChurch'] else False, axis=1)
        except Exception as e:
            logger.error(f"Error in property classification: {e}")
            # Set default values and continue
            df['IsTrust'] = False
            df['IsChurch'] = False  
            df['IsBusiness'] = False
        
        # Vectorized owner occupancy check
        df['IsOwnerOccupied'] = df.apply(lambda row: self._check_owner_occupancy(row), axis=1)
        
        # Vectorized grantor matching
        df['OwnerGrantorMatch'] = df.apply(lambda row: self.classifier._check_grantor_match(
            str(row['OwnerName']).lower() if pd.notna(row['OwnerName']) else '', 
            row.get('Grantor', '')
        ), axis=1)
        
        try:
            # Vectorized date/amount parsing
            logger.info("Parsing dates and amounts...")
            df['ParsedSaleDate'] = df.get('Last Sale Date', pd.Series()).apply(self.scorer._parse_date) 
            df['ParsedSaleAmount'] = df.get('Last Sale Amount', pd.Series()).apply(self.scorer._parse_amount)
        except Exception as e:
            logger.error(f"Error parsing dates/amounts: {e}")
            # Set default values
            df['ParsedSaleDate'] = datetime(1850, 1, 1)
            df['ParsedSaleAmount'] = None
        
        # Track parsing issues
        df['DateParseIssues'] = ''
        invalid_date_mask = pd.notna(df['Last Sale Date']) & df['ParsedSaleDate'].isna()
        invalid_amount_mask = pd.notna(df['Last Sale Amount']) & df['ParsedSaleAmount'].isna()
        
        df.loc[invalid_date_mask, 'DateParseIssues'] = 'InvalidDate'
        df.loc[invalid_amount_mask, 'DateParseIssues'] = df.loc[invalid_amount_mask, 'DateParseIssues'] + ';InvalidAmount'
        df['DateParseIssues'] = df['DateParseIssues'].str.strip(';')
        
        # Vectorized priority scoring
        logger.info("Calculating priorities...")
        priorities = []
        enhanced_codes = []
        enhanced_names = []
        
        for idx, row in df.iterrows():
            try:
                if idx % PROGRESS_LOG_INTERVAL == 0:  # Less frequent progress logging
                    logger.info(f"Processed {idx:,} records...")
                
                # Create classification object from vectorized results
                classification = PropertyClassification(
                    is_trust=row.get('IsTrust', False),
                    is_church=row.get('IsChurch', False), 
                    is_business=row.get('IsBusiness', False),
                    is_owner_occupied=row.get('IsOwnerOccupied', False),
                    owner_grantor_match=row.get('OwnerGrantorMatch', False)
                )
                
                # Score priority
                priority = self.scorer.score_property(row, classification)
                
                # Enhance priority code
                enhanced_code = self.scorer._enhance_priority_with_main_file_fields(row, priority.priority_code)
                enhanced_name = priority.priority_name
                
                if enhanced_code != priority.priority_code:
                    prefix = enhanced_code.replace(f"-{priority.priority_code}", "")
                    enhanced_name = f"{prefix} Enhanced - {priority.priority_name}"
                
                priorities.append(priority.priority_id)
                enhanced_codes.append(enhanced_code)
                enhanced_names.append(enhanced_name)
                
            except (KeyError, AttributeError) as data_error:
                logger.error(f"Data structure error in record {idx}: {data_error}. Missing required fields.")
                priorities.append(DEFAULT_PRIORITY_ID)
                enhanced_codes.append(DEFAULT_PRIORITY_CODE)
                enhanced_names.append('Default - Missing Data')
            except (ValueError, TypeError) as business_error:
                logger.error(f"Business logic error in record {idx}: {business_error}. Invalid data values.")
                priorities.append(DEFAULT_PRIORITY_ID)
                enhanced_codes.append(DEFAULT_PRIORITY_CODE)
                enhanced_names.append('Default - Invalid Data')
            except Exception as unexpected_error:
                logger.critical(f"Unexpected error processing record {idx}: {unexpected_error}. This may indicate a system issue.")
                # Re-raise critical errors that shouldn't be silently handled
                if "memory" in str(unexpected_error).lower() or "system" in str(unexpected_error).lower():
                    raise
                priorities.append(DEFAULT_PRIORITY_ID)
                enhanced_codes.append(DEFAULT_PRIORITY_CODE)
                enhanced_names.append('Default - System Error')
        
        # Assign results in bulk
        df['PriorityId'] = priorities
        df['PriorityCode'] = enhanced_codes
        df['PriorityName'] = enhanced_names
        
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
        
        # Report memory usage
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        logger.info(f"Processing complete! Final DataFrame memory usage: {memory_usage:.1f} MB")
        
        return df
    
    def _validate_dataframe_structure(self, df: pd.DataFrame, file_path: str):
        """Validate DataFrame structure and data quality"""
        # Check required columns
        missing_required = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_required:
            raise ValueError(f"Missing required columns in {file_path}: {missing_required}")
        
        # Check recommended columns
        missing_recommended = [col for col in RECOMMENDED_COLUMNS if col not in df.columns]
        if missing_recommended:
            logger.warning(f"Missing recommended columns in {file_path}: {missing_recommended}")
        
        # Validate data quality
        self._validate_data_quality(df, file_path)
    
    def _validate_data_quality(self, df: pd.DataFrame, file_path: str):
        """Perform data quality checks"""
        total_records = len(df)
        
        # Check for completely empty owner names
        empty_owners = df['Owner 1 Last Name'].isna() & df['Owner 1 First Name'].isna()
        if empty_owners.any():
            count = empty_owners.sum()
            logger.warning(f"Found {count} records ({count/total_records*100:.1f}%) with no owner name in {file_path}")
        
        # Check for empty addresses
        if 'Address' in df.columns:
            empty_addresses = df['Address'].isna() | (df['Address'].astype(str).str.strip() == '')
            if empty_addresses.any():
                count = empty_addresses.sum()
                logger.warning(f"Found {count} records ({count/total_records*100:.1f}%) with empty addresses in {file_path}")
        
        # Check sale amounts for unrealistic values
        if 'Last Sale Amount' in df.columns:
            amounts = pd.to_numeric(df['Last Sale Amount'], errors='coerce')
            unrealistic_high = amounts > MAX_REASONABLE_SALE_AMOUNT
            unrealistic_negative = amounts < 0
            
            if unrealistic_high.any():
                count = unrealistic_high.sum()
                logger.warning(f"Found {count} records with sale amounts > ${MAX_REASONABLE_SALE_AMOUNT:,} in {file_path}")
            
            if unrealistic_negative.any():
                count = unrealistic_negative.sum()
                logger.warning(f"Found {count} records with negative sale amounts in {file_path}")
    
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