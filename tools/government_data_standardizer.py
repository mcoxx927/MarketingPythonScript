import argparse
import re
from pathlib import Path
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
import json


class BaseGovernmentCleaner(ABC):
    def __init__(self, data_type: str, region: str):
        self.data_type = data_type
        self.region = region
        self.standard_columns = [
            "Owner 1 Last Name", "Owner 1 First Name", "Address", "City", "State", "Zip",
            "Mailing Address", "Mailing Unit #", "Mailing City", "Mailing State", 
            "Mailing Zip", "Mailing Zip+4", "Last Sale Date", "Last Sale Amount",
            "Parcel ID", "Current Owner"
        ]
    
    @abstractmethod
    def detect_format(self, df: pd.DataFrame) -> bool:
        pass
    
    @abstractmethod
    def extract_data(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    def parse_owner(self, name: str) -> Tuple[str, str]:
        if not isinstance(name, str):
            return "", ""
        name = name.strip()
        if not name:
            return "", ""
        if "," in name:
            last, first = name.split(",", 1)
            return last.strip(), first.strip()
        parts = name.split()
        if len(parts) >= 2:
            return parts[-1], " ".join(parts[:-1])
        return name, ""
    
    def normalize_address(self, addr: str) -> str:
        if not isinstance(addr, str):
            return ""
        return addr.strip().upper()
    
    def create_standard_record(self, **kwargs) -> Dict[str, str]:
        record = {col: "" for col in self.standard_columns}
        record.update(kwargs)
        return record


class TabularDataCleaner(BaseGovernmentCleaner):
    def __init__(self, data_type: str, region: str, column_mapping: Dict[str, str]):
        super().__init__(data_type, region)
        self.column_mapping = column_mapping
    
    def detect_format(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False
        first_row = [str(v).lower() if v is not None else "" for v in df.iloc[0].tolist()]
        mapped_keys = [key.lower() for key in self.column_mapping.keys()]
        matches = sum(1 for key in mapped_keys if any(key in cell for cell in first_row))
        return matches >= len(mapped_keys) * 0.6  # 60% match threshold
    
    def extract_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df_clean = df.copy()
        df_clean.columns = df_clean.iloc[0]
        df_clean = df_clean.drop(df_clean.index[0]).reset_index(drop=True)
        
        records = []
        for _, row in df_clean.iterrows():
            parcel_id = str(row.get(self.column_mapping.get("parcel", ""), "") or "").strip()
            owner = str(row.get(self.column_mapping.get("owner", ""), "") or "").strip()
            address = str(row.get(self.column_mapping.get("address", ""), "") or "").strip()
            
            if not address and not parcel_id:
                continue
            
            last, first = self.parse_owner(owner)
            
            record = self.create_standard_record(
                **{
                    "Parcel ID": parcel_id,
                    "Current Owner": owner,
                    "Owner 1 Last Name": last,
                    "Owner 1 First Name": first,
                    "Address": self.normalize_address(address),
                }
            )
            
            # Add data type specific fields
            if self.data_type == "code_enforcement":
                record["Case Number"] = str(row.get("CASE NO", "") or "").strip()
                record["Case Type"] = str(row.get("CASE TYPE", "") or "").strip()
                record["Status"] = str(row.get("STATUS", "") or "").strip()
            
            records.append(record)
        
        if not records:
            return pd.DataFrame(columns=self.standard_columns)
        
        return pd.DataFrame(records).drop_duplicates(subset=["Address"]).reset_index(drop=True)


class ReportLayoutCleaner(BaseGovernmentCleaner):
    def __init__(self, data_type: str, region: str):
        super().__init__(data_type, region)
    
    def detect_format(self, df: pd.DataFrame) -> bool:
        header_found = False
        for i in range(min(50, len(df))):
            row_vals = [str(v).lower() if v is not None else "" for v in df.iloc[i].tolist()]
            if any("parcel id" in v for v in row_vals) and any("current owner" in v for v in row_vals):
                header_found = True
                break
        return header_found
    
    def extract_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # This would import the existing tax delinquent logic
        from .clean_tax_delinquent import clean_delinquent_excel
        # Implementation would be moved here for consistency
        pass


class GovernmentDataStandardizer:
    def __init__(self):
        self.cleaners = []
        self.data_type_configs = {
            "code_enforcement": {
                "type": "tabular",
                "column_mapping": {
                    "parcel": "PARCEL NO",
                    "owner": "OWNER NAME", 
                    "address": "SITE ADDRESS"
                }
            },
            "tax_delinquent": {
                "type": "report_layout"
            }
        }
    
    def detect_data_type(self, file_path: Path) -> Optional[str]:
        filename = file_path.name.lower()
        if "code" in filename and "enforcement" in filename:
            return "code_enforcement"
        elif "tax" in filename and ("delinquent" in filename or "delinq" in filename):
            return "tax_delinquent"
        elif "permit" in filename:
            return "permits"
        elif "violation" in filename:
            return "violations"
        return None
    
    def create_cleaner(self, data_type: str, region: str) -> Optional[BaseGovernmentCleaner]:
        config = self.data_type_configs.get(data_type)
        if not config:
            return None
        
        if config["type"] == "tabular":
            return TabularDataCleaner(data_type, region, config["column_mapping"])
        elif config["type"] == "report_layout":
            return ReportLayoutCleaner(data_type, region)
        return None
    
    def process_file(self, input_path: Path, region: str, data_type: str = None, output_date: str = None) -> Path:
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        # Auto-detect data type if not provided
        if not data_type:
            data_type = self.detect_data_type(input_path)
            if not data_type:
                raise ValueError(f"Could not determine data type from filename: {input_path.name}")
        
        # Create appropriate cleaner
        cleaner = self.create_cleaner(data_type, region)
        if not cleaner:
            raise ValueError(f"No cleaner available for data type: {data_type}")
        
        # Read and process file
        df = pd.read_excel(input_path, header=None, dtype=str)
        
        # Verify cleaner can handle this format
        if not cleaner.detect_format(df):
            raise ValueError(f"File format not compatible with {data_type} cleaner")
        
        # Extract and standardize data
        cleaned_df = cleaner.extract_data(df)
        
        # Determine output date
        if not output_date:
            date_match = re.search(r"(\d{1,2})[-_/](\d{1,2})[-_/](\d{2,4})", input_path.name)
            if date_match:
                mm, dd, yyyy = date_match.groups()
                yyyy = ("20" + yyyy) if len(yyyy) == 2 else yyyy
                output_date = f"{yyyy}{int(mm):02d}{int(dd):02d}"
            else:
                output_date = "unknown"
        
        # Save to region directory
        region_dir = Path("regions") / region
        region_dir.mkdir(parents=True, exist_ok=True)
        output_path = region_dir / f"{region}_{data_type}_{output_date}.xlsx"
        cleaned_df.to_excel(output_path, index=False)
        
        return output_path


def main():
    parser = argparse.ArgumentParser(description="Standardize government data files into niche format")
    parser.add_argument("--input", required=True, help="Path to government data file")
    parser.add_argument("--region", required=True, help="Region key (e.g., roanoke_city_va)")
    parser.add_argument("--type", help="Data type (auto-detected if not specified)")
    parser.add_argument("--date", help="YYYYMMDD for output filename (auto-inferred if not specified)")
    parser.add_argument("--list-types", action="store_true", help="List supported data types")
    
    args = parser.parse_args()
    
    standardizer = GovernmentDataStandardizer()
    
    if args.list_types:
        print("Supported data types:")
        for data_type, config in standardizer.data_type_configs.items():
            print(f"  {data_type}: {config['type']}")
        return
    
    try:
        input_path = Path(args.input)
        output_path = standardizer.process_file(
            input_path, 
            args.region, 
            args.type, 
            args.date
        )
        print(f"Processed {args.input}")
        print(f"Saved standardized niche file: {output_path}")
        
    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()