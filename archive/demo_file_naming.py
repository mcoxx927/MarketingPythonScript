"""
Demo script to show the improved file naming with region codes.
"""

from multi_region_config import MultiRegionConfigManager
from datetime import datetime

def demo_file_naming():
    """Demonstrate the new file naming conventions"""
    print("=== NEW FILE NAMING CONVENTIONS ===")
    print("Files now include region codes for better organization:")
    print()
    
    config_manager = MultiRegionConfigManager()
    
    # Show examples for different regions
    example_regions = ['roanoke_city_va', 'virginia_beach_va', 'richmond_city_va']
    
    for region_key in example_regions:
        if region_key in config_manager.configs:
            config = config_manager.get_region_config(region_key)
            region_code = config.region_code.lower()
            date_str = datetime.now().strftime('%Y%m%d')
            time_str = datetime.now().strftime('%Y%m%d_%H%M')
            
            print(f"Region: {config.region_name} ({config.region_code})")
            print(f"  Main file: {region_code}_main_region_enhanced_{date_str}.xlsx")
            print(f"  Summary:   {region_code}_processing_summary_{date_str}.xlsx")
            print(f"  Log file:  {region_code}_processing_{time_str}.log")
            print()
    
    print("BENEFITS:")
    print("[CHECK] Clear region identification in file names")
    print("[CHECK] Easy sorting and organization") 
    print("[CHECK] No confusion when processing multiple regions")
    print("[CHECK] Professional naming convention")

if __name__ == "__main__":
    demo_file_naming()