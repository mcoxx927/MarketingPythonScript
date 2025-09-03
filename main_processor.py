"""
Main Real Estate Direct Mail Processing Script

This is the main entry point that processes both the main region file 
and all niche lists, applies business rules, and generates output files
ready for your direct mail campaigns.

This replaces the SQL stored procedures with a more maintainable Python approach.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Optional
import argparse

from property_processor import PropertyProcessor
from niche_processor import NicheListProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    """
    Performs data quality checks and validation on processed data.
    """
    
    def __init__(self):
        self.quality_issues = []
    
    def validate_dataframe(self, df: pd.DataFrame, file_name: str) -> Dict:
        """
        Validate a processed DataFrame and return quality metrics.
        
        Args:
            df: DataFrame to validate
            file_name: Name of source file for reporting
            
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Running data quality checks on {file_name}")
        
        results = {
            'file_name': file_name,
            'total_records': len(df),
            'issues': [],
            'warnings': [],
            'quality_score': 100.0
        }
        
        # Check for required columns
        required_cols = ['OwnerName', 'Address', 'PriorityCode']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            results['issues'].append(f"Missing required columns: {missing_cols}")
        
        # Check for empty owner names
        empty_owners = df['OwnerName'].isna() | (df['OwnerName'].str.strip() == '')
        if empty_owners.any():
            count = empty_owners.sum()
            results['warnings'].append(f"{count} records with empty owner names")
        
        # Check for empty addresses
        if 'Address' in df.columns:
            empty_addresses = df['Address'].isna() | (df['Address'].str.strip() == '')
            if empty_addresses.any():
                count = empty_addresses.sum()
                results['warnings'].append(f"{count} records with empty addresses")
        
        # Check for invalid zip codes
        if 'Zip' in df.columns:
            invalid_zips = ~df['Zip'].astype(str).str.match(r'^\d{5}(-\d{4})?$', na=False)
            if invalid_zips.any():
                count = invalid_zips.sum()
                results['warnings'].append(f"{count} records with invalid zip codes")
        
        # Check priority distribution
        if 'PriorityCode' in df.columns:
            priority_dist = df['PriorityCode'].value_counts()
            if 'DEFAULT' in priority_dist and priority_dist['DEFAULT'] / len(df) > 0.8:
                results['warnings'].append("Over 80% of records have DEFAULT priority - check business rules")
        
        # Check for duplicate addresses
        if 'Address' in df.columns and 'OwnerName' in df.columns:
            duplicates = df.duplicated(subset=['Address', 'OwnerName'])
            if duplicates.any():
                count = duplicates.sum()
                results['warnings'].append(f"{count} potential duplicate records")
        
        # Calculate quality score
        penalty = len(results['issues']) * 20 + len(results['warnings']) * 5
        results['quality_score'] = max(0, 100 - penalty)
        
        # Log results
        if results['issues']:
            logger.error(f"Data quality issues in {file_name}: {results['issues']}")
        if results['warnings']:
            logger.warning(f"Data quality warnings in {file_name}: {results['warnings']}")
        
        logger.info(f"Quality score for {file_name}: {results['quality_score']:.1f}/100")
        
        return results
    
    def generate_quality_report(self, validation_results: List[Dict], 
                              output_file: str = "data_quality_report.xlsx") -> None:
        """
        Generate a comprehensive data quality report.
        
        Args:
            validation_results: List of validation result dictionaries
            output_file: Output file path
        """
        # Create summary DataFrame
        summary_data = []
        for result in validation_results:
            summary_data.append({
                'File': result['file_name'],
                'Total_Records': result['total_records'],
                'Quality_Score': result['quality_score'],
                'Issues_Count': len(result['issues']),
                'Warnings_Count': len(result['warnings']),
                'Issues': '; '.join(result['issues']) if result['issues'] else '',
                'Warnings': '; '.join(result['warnings']) if result['warnings'] else ''
            })
        
        summary_df = pd.DataFrame(summary_data)
        
        with pd.ExcelWriter(output_file) as writer:
            summary_df.to_excel(writer, sheet_name='Quality_Summary', index=False)
        
        logger.info(f"Data quality report saved to: {output_file}")

class DirectMailProcessor:
    """
    Main orchestrator class that coordinates all processing steps.
    """
    
    def __init__(self, excel_dir: str = "Excel files", output_dir: str = "output"):
        self.excel_dir = Path(excel_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize processors
        self.property_processor = PropertyProcessor()
        self.niche_processor = NicheListProcessor()
        self.quality_checker = DataQualityChecker()
        
        # Processing results
        self.main_region_result = None
        self.niche_results = {}
        self.quality_results = []
    
    def process_main_region(self, file_name: str = None) -> pd.DataFrame:
        """
        Process the main region file (large file with 20k+ records).
        
        Args:
            file_name: Optional override for main file name
            
        Returns:
            Processed main region DataFrame
        """
        if file_name is None:
            # Auto-detect main region file (largest file)
            excel_files = list(self.excel_dir.glob("*.xlsx"))
            if not excel_files:
                raise FileNotFoundError(f"No Excel files found in {self.excel_dir}")
            
            # Find largest file (likely the main region file)
            largest_file = None
            max_size = 0
            for file_path in excel_files:
                try:
                    df = pd.read_excel(file_path, nrows=1)  # Just check if readable
                    file_size = file_path.stat().st_size
                    if file_size > max_size:
                        max_size = file_size
                        largest_file = file_path
                except:
                    continue
            
            if largest_file is None:
                raise FileNotFoundError("Could not determine main region file")
                
            main_file_path = str(largest_file)
        else:
            main_file_path = str(self.excel_dir / file_name)
        
        logger.info(f"Processing main region file: {main_file_path}")
        
        # Process with property processor
        self.main_region_result = self.property_processor.process_excel_file(main_file_path)
        
        # Add processing metadata
        self.main_region_result['ProcessingType'] = 'MainRegion'
        self.main_region_result['ProcessedDate'] = datetime.now()
        
        # Validate data quality
        quality_result = self.quality_checker.validate_dataframe(
            self.main_region_result, 
            Path(main_file_path).name
        )
        self.quality_results.append(quality_result)
        
        # Save main region results
        output_file = self.output_dir / "main_region_processed.xlsx"
        self.main_region_result.to_excel(output_file, index=False)
        logger.info(f"Main region results saved to: {output_file}")
        
        return self.main_region_result
    
    def process_all_niches(self) -> Dict[str, pd.DataFrame]:
        """
        Process all niche list files.
        
        Returns:
            Dictionary mapping niche types to processed DataFrames
        """
        logger.info("Processing all niche lists...")
        
        # Process with niche processor
        self.niche_results = self.niche_processor.process_all_niche_files(str(self.excel_dir))
        
        # Validate each niche list
        for niche_type, df in self.niche_results.items():
            quality_result = self.quality_checker.validate_dataframe(df, f"{niche_type}_niche")
            self.quality_results.append(quality_result)
        
        # Save individual niche results
        for niche_type, df in self.niche_results.items():
            output_file = self.output_dir / f"niche_{niche_type.lower()}_processed.xlsx"
            df.to_excel(output_file, index=False)
            logger.info(f"{niche_type} niche results saved to: {output_file}")
        
        return self.niche_results
    
    def generate_final_report(self) -> None:
        """
        Generate comprehensive final processing report.
        """
        logger.info("Generating final processing report...")
        
        # Combine all results for master report
        report_data = {
            'Processing_Summary': [],
            'Priority_Distribution': [],
            'Classification_Summary': []
        }
        
        # Main region summary
        if self.main_region_result is not None:
            df = self.main_region_result
            report_data['Processing_Summary'].append({
                'Type': 'Main Region',
                'File_Count': 1,
                'Total_Records': len(df),
                'Owner_Occupied': df['IsOwnerOccupied'].sum(),
                'Trusts': df['IsTrust'].sum(),
                'Churches': df['IsChurch'].sum(),
                'Businesses': df['IsBusiness'].sum(),
                'With_Distress': df.get('DistressScore', pd.Series([0])).gt(0).sum()
            })
            
            # Priority distribution for main region
            priority_dist = df['PriorityName'].value_counts()
            for priority, count in priority_dist.items():
                report_data['Priority_Distribution'].append({
                    'Type': 'Main Region',
                    'Priority': priority,
                    'Count': count,
                    'Percentage': (count / len(df)) * 100
                })
        
        # Niche summaries
        total_niche_records = 0
        total_niche_distress = 0
        
        for niche_type, df in self.niche_results.items():
            total_niche_records += len(df)
            niche_distress = df.get('DistressScore', pd.Series([0])).gt(0).sum()
            total_niche_distress += niche_distress
            
            report_data['Processing_Summary'].append({
                'Type': f'Niche - {niche_type}',
                'File_Count': 1,
                'Total_Records': len(df),
                'Owner_Occupied': df['IsOwnerOccupied'].sum(),
                'Trusts': df['IsTrust'].sum(),
                'Churches': df['IsChurch'].sum(),
                'Businesses': df['IsBusiness'].sum(),
                'With_Distress': niche_distress
            })
        
        # Overall niche summary
        if self.niche_results:
            report_data['Processing_Summary'].append({
                'Type': 'All Niches Combined',
                'File_Count': len(self.niche_results),
                'Total_Records': total_niche_records,
                'Owner_Occupied': 'See Individual',
                'Trusts': 'See Individual',
                'Churches': 'See Individual', 
                'Businesses': 'See Individual',
                'With_Distress': total_niche_distress
            })
        
        # Export report
        report_file = self.output_dir / "final_processing_report.xlsx"
        with pd.ExcelWriter(report_file) as writer:
            
            # Processing summary
            pd.DataFrame(report_data['Processing_Summary']).to_excel(
                writer, sheet_name='Processing_Summary', index=False
            )
            
            # Priority distribution
            if report_data['Priority_Distribution']:
                pd.DataFrame(report_data['Priority_Distribution']).to_excel(
                    writer, sheet_name='Priority_Distribution', index=False
                )
        
        logger.info(f"Final processing report saved to: {report_file}")
        
        # Generate quality report
        self.quality_checker.generate_quality_report(
            self.quality_results, 
            str(self.output_dir / "data_quality_report.xlsx")
        )
        
        # Console summary
        print("\n" + "="*60)
        print("PROCESSING COMPLETE - SUMMARY")
        print("="*60)
        
        if self.main_region_result is not None:
            print(f"Main Region: {len(self.main_region_result):,} records processed")
        
        if self.niche_results:
            print(f"Niche Lists: {len(self.niche_results)} types, {total_niche_records:,} total records")
            
        total_records = (len(self.main_region_result) if self.main_region_result is not None else 0) + total_niche_records
        print(f"Total Records: {total_records:,}")
        print(f"Output Directory: {self.output_dir}")
        print(f"Reports Generated: final_processing_report.xlsx, data_quality_report.xlsx")
        print("="*60)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Real Estate Direct Mail Processor")
    parser.add_argument("--excel-dir", default="Excel files", help="Directory containing Excel files")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--main-only", action="store_true", help="Process only main region file")
    parser.add_argument("--niches-only", action="store_true", help="Process only niche lists")
    
    args = parser.parse_args()
    
    logger.info("Starting Real Estate Direct Mail Processing")
    logger.info(f"Excel Directory: {args.excel_dir}")
    logger.info(f"Output Directory: {args.output_dir}")
    
    try:
        # Initialize processor
        processor = DirectMailProcessor(args.excel_dir, args.output_dir)
        
        # Process based on arguments
        if args.main_only:
            logger.info("Processing main region file only")
            processor.process_main_region()
            
        elif args.niches_only:
            logger.info("Processing niche lists only")
            processor.process_all_niches()
            
        else:
            logger.info("Processing main region and all niche lists")
            processor.process_main_region()
            processor.process_all_niches()
        
        # Generate final report
        processor.generate_final_report()
        
        logger.info("Processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


if __name__ == "__main__":
    main()