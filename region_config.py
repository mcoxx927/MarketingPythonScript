"""
Region Configuration for Direct Mail Processing

This module handles region-specific parameters that were previously stored 
in database tables. It replaces the SQL queries to Region table for 
InputDate1, InputDate2, InputAmount1, InputAmount2.
"""

import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

@dataclass
class RegionConfig:
    """
    Region-specific configuration parameters.
    These were previously stored in the Region database table.
    """
    region_id: int
    region_name: str
    
    # Date criteria for priority scoring
    input_date1: datetime  # Cutoff for ABS1 (old sale dates)
    input_date2: datetime  # Cutoff for BUY1/BUY2 (recent buyers)
    
    # Amount thresholds for priority scoring  
    input_amount1: float   # Low sale amount threshold (TRS1, OON1)
    input_amount2: float   # High sale amount threshold (BUY1, BUY2)
    
    # Additional regional parameters
    owner_occupancy_months_back: int = 6  # How far back to check for recent updates
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.input_date1 >= self.input_date2:
            logger.warning(f"input_date1 ({self.input_date1}) should be older than input_date2 ({self.input_date2})")
        
        if self.input_amount1 >= self.input_amount2:
            logger.warning(f"input_amount1 ({self.input_amount1}) should be less than input_amount2 ({self.input_amount2})")


class RegionConfigManager:
    """
    Manages region configurations and provides lookup functionality.
    This replaces the database queries for region parameters.
    """
    
    def __init__(self):
        self.configs: Dict[int, RegionConfig] = {}
        self._load_default_configs()
    
    def _load_default_configs(self):
        """
        Load default configurations for known regions.
        Based on typical real estate market parameters.
        """
        
        # Default Roanoke configuration (Region ID 40 from SQL)
        roanoke_config = RegionConfig(
            region_id=40,
            region_name="Roanoke City, VA",
            
            # Date criteria - these were in the SQL Region table
            input_date1=datetime.now() - timedelta(days=365*15),  # 15 years ago for "old" properties
            input_date2=datetime.now() - timedelta(days=365*5),   # 5 years ago for "recent" buyers
            
            # Amount thresholds - typical for Roanoke market
            input_amount1=75000,   # Low sale amount threshold
            input_amount2=200000,  # High sale amount threshold (cash buyer indicator)
        )
        
        self.configs[40] = roanoke_config
        
        # Add other regions as needed
        self._add_default_region_configs()
    
    def _add_default_region_configs(self):
        """Add configurations for other common regions"""
        
        # Generic Virginia market
        va_config = RegionConfig(
            region_id=51,  # Virginia FIPS code
            region_name="Virginia (Generic)",
            input_date1=datetime.now() - timedelta(days=365*12),
            input_date2=datetime.now() - timedelta(days=365*3),
            input_amount1=100000,
            input_amount2=250000,
        )
        self.configs[51] = va_config
        
        # Add more regions as needed...
    
    def get_config(self, region_id: int = None, region_name: str = None) -> RegionConfig:
        """
        Get configuration for a specific region.
        
        Args:
            region_id: Region ID to lookup
            region_name: Region name to lookup (if no ID provided)
            
        Returns:
            RegionConfig object
        """
        # Default to Roanoke if nothing specified
        if region_id is None and region_name is None:
            region_id = 40
        
        if region_id and region_id in self.configs:
            return self.configs[region_id]
        
        if region_name:
            for config in self.configs.values():
                if region_name.lower() in config.region_name.lower():
                    return config
        
        # Fallback to default Roanoke config
        logger.warning(f"Region not found (ID: {region_id}, Name: {region_name}), using default Roanoke config")
        return self.configs[40]
    
    def add_custom_config(self, config: RegionConfig):
        """Add or update a custom region configuration"""
        self.configs[config.region_id] = config
        logger.info(f"Added/updated config for region {config.region_id}: {config.region_name}")
    
    def list_regions(self) -> Dict[int, str]:
        """List all configured regions"""
        return {config.region_id: config.region_name for config in self.configs.values()}
    
    @classmethod
    def from_excel_config(cls, config_file: str):
        """
        Load region configurations from an Excel file.
        Useful for non-technical users to manage configurations.
        
        Expected Excel format:
        - Sheet: 'RegionConfig'
        - Columns: RegionId, RegionName, InputDate1, InputDate2, InputAmount1, InputAmount2
        """
        manager = cls()
        
        try:
            df = pd.read_excel(config_file, sheet_name='RegionConfig')
            
            for _, row in df.iterrows():
                config = RegionConfig(
                    region_id=int(row['RegionId']),
                    region_name=str(row['RegionName']),
                    input_date1=pd.to_datetime(row['InputDate1']),
                    input_date2=pd.to_datetime(row['InputDate2']),
                    input_amount1=float(row['InputAmount1']),
                    input_amount2=float(row['InputAmount2'])
                )
                manager.add_custom_config(config)
                
            logger.info(f"Loaded {len(df)} region configurations from {config_file}")
            
        except FileNotFoundError:
            logger.warning(f"Config file {config_file} not found, using defaults")
        except Exception as e:
            logger.error(f"Error loading config from {config_file}: {e}")
            
        return manager


def create_sample_config_file(output_file: str = "region_config_template.xlsx"):
    """
    Create a sample configuration Excel file that users can modify.
    """
    sample_data = [
        {
            'RegionId': 40,
            'RegionName': 'Roanoke City, VA',
            'InputDate1': '2009-01-01',  # 15 years ago
            'InputDate2': '2019-01-01',  # 5 years ago  
            'InputAmount1': 75000,
            'InputAmount2': 200000,
            'Notes': 'Default Roanoke configuration'
        },
        {
            'RegionId': 51,
            'RegionName': 'Virginia Beach, VA',
            'InputDate1': '2012-01-01',  # 12 years ago
            'InputDate2': '2021-01-01',  # 3 years ago
            'InputAmount1': 150000,
            'InputAmount2': 400000,
            'Notes': 'Higher value market'
        },
        {
            'RegionId': 99,
            'RegionName': 'Custom Region Template',
            'InputDate1': '2010-01-01',
            'InputDate2': '2020-01-01',
            'InputAmount1': 100000,
            'InputAmount2': 250000,
            'Notes': 'Template for new regions'
        }
    ]
    
    df = pd.DataFrame(sample_data)
    df.to_excel(output_file, sheet_name='RegionConfig', index=False)
    
    print(f"Sample configuration file created: {output_file}")
    print("Edit this file to customize your region parameters, then use:")
    print(f"  manager = RegionConfigManager.from_excel_config('{output_file}')")


# Date handling utilities
class DateHandler:
    """
    Handles date parsing and blank value management.
    Addresses the SQL stored procedure's handling of '1900-01-01' and NULL dates.
    """
    
    @staticmethod
    def parse_sale_date(date_value) -> Optional[datetime]:
        """
        Parse sale date with proper handling of blank/invalid values.
        
        The SQL stored procedure used '1900-01-01' as a sentinel value for missing dates.
        We convert these to None for cleaner Python handling.
        """
        if pd.isna(date_value) or date_value == '' or date_value is None:
            return None
            
        try:
            parsed_date = pd.to_datetime(date_value)
            
            # Convert SQL sentinel dates to None
            if parsed_date.year <= 1900:
                return None
                
            # Sanity check - dates in the future are probably errors
            if parsed_date > datetime.now():
                logger.warning(f"Future date detected: {parsed_date}, treating as None")
                return None
                
            return parsed_date
            
        except (ValueError, TypeError):
            logger.debug(f"Could not parse date: {date_value}")
            return None
    
    @staticmethod
    def parse_amount(amount_value) -> Optional[float]:
        """
        Parse monetary amounts with proper handling of blank/invalid values.
        """
        if pd.isna(amount_value) or amount_value == '' or amount_value is None:
            return None
            
        try:
            # Handle string amounts with commas, dollar signs, etc.
            if isinstance(amount_value, str):
                cleaned = amount_value.replace(',', '').replace('$', '').strip()
                if not cleaned:
                    return None
                amount = float(cleaned)
            else:
                amount = float(amount_value)
            
            # Negative amounts are probably errors
            if amount < 0:
                return None
                
            return amount
            
        except (ValueError, TypeError):
            logger.debug(f"Could not parse amount: {amount_value}")
            return None
    
    @staticmethod
    def is_recent_enough(date_val: Optional[datetime], cutoff_date: datetime) -> bool:
        """
        Check if a date is recent enough (after cutoff date).
        Handles None dates gracefully.
        """
        if date_val is None:
            return False
        return date_val >= cutoff_date
    
    @staticmethod
    def is_old_enough(date_val: Optional[datetime], cutoff_date: datetime) -> bool:
        """
        Check if a date is old enough (before cutoff date).
        Handles None dates gracefully.
        """
        if date_val is None:
            return False
        return date_val <= cutoff_date


if __name__ == "__main__":
    # Demo usage
    print("=== Region Configuration Demo ===")
    
    # Create manager with defaults
    manager = RegionConfigManager()
    
    # List available regions
    print("Available regions:")
    for region_id, name in manager.list_regions().items():
        print(f"  {region_id}: {name}")
    
    # Get Roanoke config
    roanoke_config = manager.get_config(40)
    print(f"\nRoanoke Configuration:")
    print(f"  Input Date 1 (old sales): {roanoke_config.input_date1.strftime('%Y-%m-%d')}")
    print(f"  Input Date 2 (recent buyers): {roanoke_config.input_date2.strftime('%Y-%m-%d')}")
    print(f"  Amount 1 (low threshold): ${roanoke_config.input_amount1:,}")
    print(f"  Amount 2 (high threshold): ${roanoke_config.input_amount2:,}")
    
    # Create sample config file
    create_sample_config_file()
    
    # Test date handling
    print("\n=== Date Handling Demo ===")
    handler = DateHandler()
    
    test_dates = ['2020-01-15', '1900-01-01', '', None, 'invalid', '2025-12-31']
    for test_date in test_dates:
        parsed = handler.parse_sale_date(test_date)
        print(f"  '{test_date}' -> {parsed}")
    
    test_amounts = [150000, '$250,000', '100000.50', '', None, 'invalid', -5000]
    for test_amount in test_amounts:
        parsed = handler.parse_amount(test_amount)
        print(f"  '{test_amount}' -> {parsed}")