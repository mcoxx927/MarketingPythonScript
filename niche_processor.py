"""
Niche List Processor for Real Estate Direct Mail

This module processes niche lists (PreForeclosure, Liens, Bankruptcy, etc.) 
and enhances them with distress indicators and custom priority codes.
Based on the GetFinalizedNicheList stored procedure logic.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

from property_processor import PropertyProcessor, PropertyClassifier

logger = logging.getLogger(__name__)

@dataclass
class NicheListConfig:
    """Configuration for processing a specific niche list type"""
    file_pattern: str
    niche_type: str
    base_priority_code: str
    distress_indicators: List[str]

class DistressIndicatorEngine:
    """
    Adds distress indicators to properties based on available data.
    Based on the complex niche list enhancement logic from SQL.
    """
    
    def __init__(self):
        self.indicators = {
            'HE': 'High Equity',           # Est. Loan-to-Value <= 50%
            'Liens': 'Liens',              # Has lien information
            'Bankrupcy': 'Bankruptcy',     # Has bankruptcy date
            'Divorce': 'Divorce',          # Has divorce date  
            'PreFor': 'Pre-Foreclosure',   # Has pre-foreclosure info
            'F&C': 'Free & Clear',         # Loan-to-Value = 0 and has equity
            'Vacant': 'Vacant',            # Marked as vacant
            'NCOA_Moves': 'NCOA Moves',    # NCOA indicates move
            'NCOA_Drops': 'NCOA Drops',    # NCOA indicates delivery issues
        }
    
    def add_distress_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add distress indicator flags and enhanced priority codes to DataFrame.
        
        Args:
            df: Property DataFrame
            
        Returns:
            Enhanced DataFrame with distress indicators
        """
        df = df.copy()
        
        # Initialize indicator columns
        for code, name in self.indicators.items():
            df[f'Has_{code}'] = False
        
        df['DistressScore'] = 0
        df['DistressIndicators'] = ''
        df['EnhancedPriorityCode'] = df.get('PriorityCode', 'DEFAULT')
        
        # High Equity (HE): Est. Loan-to-Value <= 50%
        he_mask = ((df['Est. Loan-to-Value'].fillna(0) <= 50) & 
                   (df['Est. Loan-to-Value'].fillna(0) > 0))
        df.loc[he_mask, 'Has_HE'] = True
        
        # Liens: Has lien type and/or lien amount
        liens_mask = ((df['Lien Type'].notna() & (df['Lien Type'] != '')) |
                     (df['Lien Amount'].notna() & (df['Lien Amount'] != '')))
        df.loc[liens_mask, 'Has_Liens'] = True
        
        # Bankruptcy: Has BK Date
        bk_mask = (df['BK Date'].notna() & (df['BK Date'] != ''))
        df.loc[bk_mask, 'Has_Bankrupcy'] = True
        
        # Divorce: Has Divorce Date  
        divorce_mask = (df['Divorce Date'].notna() & (df['Divorce Date'] != ''))
        df.loc[divorce_mask, 'Has_Divorce'] = True
        
        # Pre-Foreclosure: Has Pre-FC Recording Date
        prefc_mask = (df['Pre-FC Recording Date'].notna() & (df['Pre-FC Recording Date'] != ''))
        df.loc[prefc_mask, 'Has_PreFor'] = True
        
        # Free & Clear: Loan-to-Value = 0 and has equity
        fc_mask = ((df['Est. Loan-to-Value'].fillna(0) == 0) & 
                   (df['Est. Equity'].notna()) & 
                   (df['Est. Equity'] != '') &
                   (df['Est. Equity'] != 0))
        df.loc[fc_mask, 'Has_F&C'] = True
        
        # Vacant: Vacant = 'Yes'
        vacant_mask = (df['Vacant'].str.lower() == 'yes')
        df.loc[vacant_mask, 'Has_Vacant'] = True
        
        # NCOA indicators (if Response NCOA column exists)
        if 'ResponseNCOA' in df.columns:
            moves_mask = df['ResponseNCOA'].isin(['A', '91', '92'])
            df.loc[moves_mask, 'Has_NCOA_Moves'] = True
            
            drops_mask = ((df['ResponseNCOA'].notna()) & 
                         (df['ResponseNCOA'] != '') &
                         (~df['ResponseNCOA'].isin(['A', '91', '92'])))
            df.loc[drops_mask, 'Has_NCOA_Drops'] = True
        
        # Calculate distress score and build indicator string
        for idx, row in df.iterrows():
            indicators = []
            score = 0
            
            for code, name in self.indicators.items():
                if row.get(f'Has_{code}', False):
                    indicators.append(code)
                    score += 1
            
            df.at[idx, 'DistressScore'] = score
            df.at[idx, 'DistressIndicators'] = '-'.join(indicators)
            
            # Build enhanced priority code (prepend indicators to base priority)
            if indicators:
                base_priority = row.get('PriorityCode', 'DEFAULT') 
                enhanced_code = '-'.join(indicators) + '-' + base_priority
                df.at[idx, 'EnhancedPriorityCode'] = enhanced_code
        
        return df

class NicheListProcessor:
    """
    Main processor for niche lists that combines base property processing 
    with niche-specific enhancements.
    """
    
    def __init__(self):
        self.property_processor = PropertyProcessor()
        self.distress_engine = DistressIndicatorEngine()
        
        # Define niche list configurations
        self.niche_configs = {
            'PreForeclosure': NicheListConfig(
                file_pattern='*PreForeclosure*',
                niche_type='Foreclosure',
                base_priority_code='Foreclosure',
                distress_indicators=['PreFor', 'Liens']
            ),
            'Liens': NicheListConfig(
                file_pattern='*Liens*',
                niche_type='Liens', 
                base_priority_code='Liens',
                distress_indicators=['Liens']
            ),
            'Bankruptcy': NicheListConfig(
                file_pattern='*Bankruptcy*',
                niche_type='Bankruptcy',
                base_priority_code='Bankruptcy', 
                distress_indicators=['Bankrupcy', 'HE']
            ),
            'CashBuyers': NicheListConfig(
                file_pattern='*Cash*Buyers*',
                niche_type='CashBuyers',
                base_priority_code='CashBuyers',
                distress_indicators=['HE']
            ),
            'TiredLandlords': NicheListConfig(
                file_pattern='*Tired*Landlords*',
                niche_type='TiredLandlords', 
                base_priority_code='TiredLandlords',
                distress_indicators=['HE', 'Vacant']
            ),
            'PreProbate': NicheListConfig(
                file_pattern='*Pre*Probate*',
                niche_type='PreProbate',
                base_priority_code='PreProbate', 
                distress_indicators=['Divorce']
            ),
            'TaxDelinquencies': NicheListConfig(
                file_pattern='*Tax*Delinquenc*',
                niche_type='TaxDelinquencies',
                base_priority_code='TaxDelinquencies',
                distress_indicators=['Liens']
            ),
            'InterFamily': NicheListConfig(
                file_pattern='*InterFamily*',
                niche_type='InterFamily',
                base_priority_code='InterFamily', 
                distress_indicators=['HE']
            )
        }
    
    def process_niche_file(self, file_path: str, niche_type: str = None) -> pd.DataFrame:
        """
        Process a single niche list file.
        
        Args:
            file_path: Path to niche list Excel file
            niche_type: Override niche type detection
            
        Returns:
            Enhanced DataFrame with niche-specific processing
        """
        logger.info(f"Processing niche file: {file_path}")
        
        # Auto-detect niche type if not provided
        if not niche_type:
            niche_type = self._detect_niche_type(file_path)
        
        # Get configuration for this niche type
        config = self.niche_configs.get(niche_type)
        if not config:
            logger.warning(f"Unknown niche type: {niche_type}, using default processing")
            config = NicheListConfig("*", niche_type, niche_type, [])
        
        # Process with base property processor
        df = self.property_processor.process_excel_file(file_path)
        
        # Override priority codes with niche-specific codes
        df['PriorityCode'] = config.base_priority_code
        df['PriorityName'] = f"{config.niche_type} List"
        df['NicheType'] = config.niche_type
        
        # Add distress indicators
        df = self.distress_engine.add_distress_indicators(df)
        
        # Add processing metadata
        df['ProcessedDate'] = pd.Timestamp.now()
        df['SourceFile'] = Path(file_path).name
        
        logger.info(f"Niche processing complete: {len(df)} records, {niche_type} type")
        
        return df
    
    def process_all_niche_files(self, excel_dir: str = "Excel files") -> Dict[str, pd.DataFrame]:
        """
        Process all niche list files in the directory.
        
        Args:
            excel_dir: Directory containing Excel files
            
        Returns:
            Dictionary mapping niche types to processed DataFrames
        """
        results = {}
        excel_path = Path(excel_dir)
        
        # Skip the main region file
        main_file_pattern = "*Property Export Roanoke+City_2C+VA.xlsx"
        
        for excel_file in excel_path.glob("*.xlsx"):
            # Skip main region file
            if excel_file.match(main_file_pattern):
                continue
                
            niche_type = self._detect_niche_type(str(excel_file))
            
            try:
                df = self.process_niche_file(str(excel_file), niche_type)
                results[niche_type] = df
                
                # Show summary
                print(f"\n--- {niche_type} Summary ---")
                print(f"Records: {len(df)}")
                print(f"Distress Score Distribution:")
                score_dist = df['DistressScore'].value_counts().sort_index()
                for score, count in score_dist.items():
                    print(f"  Score {score}: {count} records")
                
                print(f"Top Distress Indicators:")
                indicator_counts = df['DistressIndicators'].value_counts().head(5)
                for indicator, count in indicator_counts.items():
                    if indicator:  # Skip empty indicators
                        print(f"  {indicator}: {count} records")
                        
            except Exception as e:
                logger.error(f"Error processing {excel_file}: {e}")
                
        return results
    
    def _detect_niche_type(self, file_path: str) -> str:
        """Auto-detect niche type from file name"""
        file_name = str(file_path).lower()
        
        # Check each pattern
        for niche_type, config in self.niche_configs.items():
            pattern = config.file_pattern.lower().replace('*', '')
            if pattern.replace('*', '') in file_name:
                return niche_type
        
        # Default fallback
        return "Unknown"
    
    def export_summary_report(self, results: Dict[str, pd.DataFrame], 
                             output_file: str = "niche_processing_summary.xlsx") -> None:
        """
        Export a summary report of all niche list processing results.
        
        Args:
            results: Dictionary of processed niche DataFrames
            output_file: Output Excel file path
        """
        with pd.ExcelWriter(output_file) as writer:
            
            # Summary sheet
            summary_data = []
            for niche_type, df in results.items():
                summary_data.append({
                    'Niche_Type': niche_type,
                    'Total_Records': len(df),
                    'With_Distress': (df['DistressScore'] > 0).sum(),
                    'Avg_Distress_Score': df['DistressScore'].mean(),
                    'Max_Distress_Score': df['DistressScore'].max(),
                    'Top_Indicator': df['DistressIndicators'].value_counts().index[0] if len(df) > 0 else '',
                    'Owner_Occupied': df['IsOwnerOccupied'].sum(),
                    'Trusts': df['IsTrust'].sum(),
                    'Businesses': df['IsBusiness'].sum(),
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Individual niche sheets (sample data)
            for niche_type, df in results.items():
                sheet_name = niche_type[:30]  # Excel sheet name limit
                sample_df = df.head(1000)  # Limit rows per sheet
                sample_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"\nSummary report exported to: {output_file}")


if __name__ == "__main__":
    # Example usage
    processor = NicheListProcessor()
    
    # Process all niche files
    results = processor.process_all_niche_files()
    
    # Export summary report
    processor.export_summary_report(results)
    
    print("\n=== OVERALL SUMMARY ===")
    total_records = sum(len(df) for df in results.values())
    total_with_distress = sum((df['DistressScore'] > 0).sum() for df in results.values())
    
    print(f"Total niche records processed: {total_records}")
    print(f"Records with distress indicators: {total_with_distress}")
    print(f"Niche types processed: {len(results)}")
    
    for niche_type, df in results.items():
        print(f"  - {niche_type}: {len(df)} records")