"""
Multi-Region Configuration System

This module handles loading, validating, and managing region-specific configurations
for the real estate direct mail processing system.
"""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class RegionConfig:
    """Configuration for a specific region"""
    region_name: str
    region_code: str
    region_input_date1: datetime  # ABS1 cutoff date
    region_input_date2: datetime  # BUY1/BUY2 cutoff date  
    region_input_amount1: float   # Low amount threshold
    region_input_amount2: float   # High amount threshold
    market_type: str
    description: str
    notes: str
    
    def __post_init__(self):
        """Validate configuration after creation"""
        if self.region_input_date1 >= self.region_input_date2:
            logger.warning(f"{self.region_code}: date1 ({self.region_input_date1}) should be older than date2 ({self.region_input_date2})")
            
        if self.region_input_amount1 >= self.region_input_amount2:
            logger.warning(f"{self.region_code}: amount1 ({self.region_input_amount1}) should be less than amount2 ({self.region_input_amount2})")

class MultiRegionConfigManager:
    """Manages configurations for multiple regions"""
    
    def __init__(self, regions_dir: str = "regions"):
        self.regions_dir = Path(regions_dir)
        self.configs: Dict[str, RegionConfig] = {}
        self._load_all_configs()
    
    def _load_all_configs(self):
        """Load all region configurations from the regions directory"""
        if not self.regions_dir.exists():
            raise FileNotFoundError(f"Regions directory not found: {self.regions_dir}")
        
        config_count = 0
        for region_dir in self.regions_dir.iterdir():
            if region_dir.is_dir():
                config_file = region_dir / "config.json"
                if config_file.exists():
                    try:
                        config = self._load_region_config(str(region_dir))
                        self.configs[region_dir.name] = config
                        config_count += 1
                        logger.info(f"Loaded config for {config.region_name} ({region_dir.name})")
                    except Exception as e:
                        logger.error(f"Failed to load config for {region_dir.name}: {e}")
                else:
                    logger.warning(f"No config.json found in {region_dir}")
        
        logger.info(f"Loaded {config_count} region configurations")
    
    def _load_region_config(self, region_path: str) -> RegionConfig:
        """Load configuration for a specific region"""
        config_file = Path(region_path) / "config.json"
        
        with open(config_file, 'r') as f:
            data = json.load(f)
        
        # Parse dates
        date1 = datetime.strptime(data['region_input_date1'], '%Y-%m-%d')
        date2 = datetime.strptime(data['region_input_date2'], '%Y-%m-%d')
        
        return RegionConfig(
            region_name=data['region_name'],
            region_code=data['region_code'],
            region_input_date1=date1,
            region_input_date2=date2,
            region_input_amount1=float(data['region_input_amount1']),
            region_input_amount2=float(data['region_input_amount2']),
            market_type=data.get('market_type', 'Unknown'),
            description=data.get('description', ''),
            notes=data.get('notes', '')
        )
    
    def get_region_config(self, region_key: str) -> RegionConfig:
        """Get configuration for a specific region"""
        if region_key not in self.configs:
            available = list(self.configs.keys())
            raise ValueError(f"Region '{region_key}' not found. Available regions: {available}")
        
        return self.configs[region_key]
    
    def list_regions(self) -> List[Dict[str, str]]:
        """List all available regions with basic info"""
        regions = []
        for key, config in self.configs.items():
            regions.append({
                'key': key,
                'name': config.region_name,
                'code': config.region_code,
                'market_type': config.market_type,
                'description': config.description
            })
        return sorted(regions, key=lambda x: x['name'])
    
    def get_region_directory(self, region_key: str) -> Path:
        """Get the directory path for a region"""
        if region_key not in self.configs:
            raise ValueError(f"Region '{region_key}' not found")
        
        return self.regions_dir / region_key
    
    def validate_region_files(self, region_key: str) -> Dict[str, bool]:
        """Validate that required files exist for a region"""
        region_dir = self.get_region_directory(region_key)
        
        # Check for main region file
        main_files = list(region_dir.glob("main_region.*"))
        has_main = len(main_files) > 0
        
        # Check for Excel files
        excel_files = list(region_dir.glob("*.xlsx"))
        has_excel = len(excel_files) > 0
        
        return {
            'has_config': (region_dir / "config.json").exists(),
            'has_main_file': has_main,
            'has_excel_files': has_excel,
            'total_files': len(excel_files),
            'valid': has_main and has_excel
        }
    
    def create_output_directory(self, region_key: str) -> Path:
        """Create and return the output directory for a region"""
        config = self.get_region_config(region_key)
        
        # Create output structure: output/region_key/YYYY_MM/
        output_dir = Path("output") / region_key / datetime.now().strftime('%Y_%m')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        return output_dir

def demo_region_config():
    """Demo function to show region configuration usage"""
    try:
        manager = MultiRegionConfigManager()
        
        print("=== AVAILABLE REGIONS ===")
        for region in manager.list_regions():
            print(f"{region['code']:4} | {region['name']:25} | {region['market_type']:15} | {region['description']}")
        
        print(f"\n=== ROANOKE CONFIGURATION ===")
        if 'roanoke_city_va' in manager.configs:
            config = manager.get_region_config('roanoke_city_va')
            print(f"Region: {config.region_name}")
            print(f"Code: {config.region_code}")
            print(f"ABS1 Date Cutoff: {config.region_input_date1.strftime('%Y-%m-%d')}")
            print(f"BUY Date Cutoff: {config.region_input_date2.strftime('%Y-%m-%d')}")
            print(f"Amount Thresholds: ${config.region_input_amount1:,} / ${config.region_input_amount2:,}")
            print(f"Market Type: {config.market_type}")
            
            # Check file validation
            validation = manager.validate_region_files('roanoke_city_va')
            print(f"File Validation: {validation}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run demo
    demo_region_config()