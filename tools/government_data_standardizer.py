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
        self.region_fips = None  # Will be set by process_file method
        self.standard_columns = [
            "Owner 1 Last Name", "Owner 1 First Name", "Address", "City", "State", "Zip",
            "Mailing Address", "Mailing Unit #", "Mailing City", "Mailing State", 
            "Mailing Zip", "Mailing Zip+4", "Last Sale Date", "Last Sale Amount",
            "Parcel ID", "Current Owner", "FIPS"
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
        # For code enforcement, use legacy cleaner with GIS augmentation
        if self.data_type == "code_enforcement":
            return self._extract_code_enforcement_with_gis(df)
        
        # Generic tabular extraction for other types
        return self._extract_generic_tabular(df)
    
    def _extract_code_enforcement_with_gis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract code enforcement data using legacy cleaner with GIS augmentation"""
        # Create temporary file to pass to legacy cleaner
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
            df.to_excel(temp_file.name, index=False, header=False)
            temp_path = Path(temp_file.name)
        
        try:
            # Use legacy code enforcement cleaner
            try:
                from clean_code_enforcement import clean_code_enforcement_excel
                from gis_utils import load_gis_data
                
                # Load GIS data if available
                gis_data = None
                gis_dir = Path("government_data") / self.region / "gis"
                potential_gis_files = list(gis_dir.glob("Parcels*.csv")) if gis_dir.exists() else []
                gis_path = potential_gis_files[0] if potential_gis_files else None
                if gis_path and gis_path.exists():
                    try:
                        gis_data = load_gis_data(gis_path)
                        print(f"Code enforcement: Using GIS data with {len(gis_data)} parcels")
                    except Exception as e:
                        print(f"Could not load GIS data: {e}")
                
                result_df = clean_code_enforcement_excel(temp_path, gis_data)
                
                # Add FIPS if missing
                if 'FIPS' not in result_df.columns:
                    result_df['FIPS'] = self.region_fips or ""
                    
                return result_df
                
            except ImportError as e:
                print(f"Could not import legacy code enforcement cleaner: {e}")
                print("Falling back to generic tabular extraction")
                # Fall back to generic processing
                return self._extract_generic_tabular(df)
            except Exception as e:
                print(f"Error running legacy code enforcement cleaner: {e}")
                print("Falling back to generic tabular extraction")
                return self._extract_generic_tabular(df)
                
        finally:
            # Clean up temp file
            if temp_path.exists():
                os.unlink(temp_path)
    
    def _extract_generic_tabular(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generic tabular data extraction (original logic)"""
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
                    "FIPS": self.region_fips or ""
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
            # Look for tax delinquent patterns
            if (any("parcel" in v for v in row_vals) and 
                (any("account" in v for v in row_vals) or any("owner" in v for v in row_vals))):
                header_found = True
                break
        return header_found
    
    def extract_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Route to region-specific tax delinquent cleaners
        import tempfile
        import os
        
        # Create a temporary file to pass to the existing cleaners
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
            # Write the dataframe to temp file
            df.to_excel(temp_file.name, index=False, header=False)
            temp_path = Path(temp_file.name)
        
        try:
            result_df = None
            
            # Load GIS data for tax delinquent processing
            gis_data = None
            gis_dir = Path("government_data") / self.region / "gis"
            potential_gis_files = list(gis_dir.glob("Parcels*.csv")) if gis_dir.exists() else []
            gis_path = potential_gis_files[0] if potential_gis_files else None
            if gis_path and gis_path.exists():
                try:
                    from gis_utils import load_gis_data
                    gis_data = load_gis_data(gis_path)
                    print(f"Tax delinquent: Using GIS data with {len(gis_data)} parcels")
                except Exception as e:
                    print(f"Could not load GIS data for tax delinquent: {e}")
            
            if self.region == "roanoke_city_va":
                try:
                    from clean_tax_delinquent_roanoke import clean_roanoke_tax_delinquent
                    result_df = clean_roanoke_tax_delinquent(temp_path, gis_data)
                except ImportError as e:
                    print(f"Could not import roanoke tax delinquent cleaner: {e}")
                    
            elif self.region == "lynchburg_city_va":
                try:
                    from clean_tax_delinquent_lynchburg import clean_delinquent_excel
                    result_df = clean_delinquent_excel(temp_path, gis_data)
                except ImportError as e:
                    print(f"Could not import lynchburg tax delinquent cleaner: {e}")
            
            else:
                print(f"No specific tax delinquent cleaner found for region: {self.region}")
                
            # Add FIPS if region-specific cleaner was successful and FIPS is missing
            if result_df is not None and not result_df.empty and 'FIPS' not in result_df.columns:
                result_df['FIPS'] = self.region_fips or ""
                print(f"Added FIPS {self.region_fips} to region-specific cleaner output")
                return result_df
                
            elif result_df is not None:
                return result_df
                
        finally:
            # Clean up temp file
            os.unlink(temp_path)
        
        # If we get here, no region-specific cleaner worked
        # Find the header row
        header_row_idx = None
        for i in range(min(10, len(df))):
            row_vals = [str(v).lower() if pd.notna(v) else "" for v in df.iloc[i].tolist()]
            if (any("parcel" in v for v in row_vals) and 
                (any("account" in v for v in row_vals) or any("owner" in v for v in row_vals))):
                header_row_idx = i
                break
        
        if header_row_idx is None:
            return pd.DataFrame(columns=self.standard_columns)
        
        # Extract data starting from header row
        df_clean = df.iloc[header_row_idx:].copy()
        df_clean.columns = df_clean.iloc[0]
        df_clean = df_clean.drop(df_clean.index[0]).reset_index(drop=True)
        
        records = []
        for _, row in df_clean.iterrows():
            # Map common tax delinquent fields
            parcel_id = str(row.get("Parcel Id", "") or "").strip()
            account_name = str(row.get("Account Name", "") or "").strip()
            address = str(row.get("Parcel Address", "") or "").strip()
            amount_due = str(row.get("Amount Due", "") or "").strip()
            
            if not account_name or not address:
                continue
            
            # Parse owner name
            last, first = self.parse_owner(account_name)
            
            record = self.create_standard_record(
                **{
                    "Parcel ID": parcel_id,
                    "Current Owner": account_name,
                    "Owner 1 Last Name": last,
                    "Owner 1 First Name": first,
                    "Address": self.normalize_address(address),
                    "FIPS": self.region_fips or ""
                }
            )
            
            records.append(record)
        
        if not records:
            return pd.DataFrame(columns=self.standard_columns)
        
        return pd.DataFrame(records).drop_duplicates(subset=["Address", "Parcel ID"]).reset_index(drop=True)


class GISParcelCleaner(BaseGovernmentCleaner):
    def __init__(self, data_type: str, region: str, column_mapping: Dict[str, str], name_format: str = "lastname_first"):
        super().__init__(data_type, region)
        self.column_mapping = column_mapping
        self.name_format = name_format  # "lastname_first" or "firstname_last"
    
    def detect_format(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False
        
        # Check for GIS parcel data columns
        column_names = [str(col).lower() for col in df.columns]
        required_columns = ['owner', 'grantor']  # Basic requirement
        gis_indicators = ['parcel', 'taxid', 'gisobjid', 'locaddr', 'assessment']
        
        has_required = all(any(req in col for col in column_names) for req in required_columns)
        has_gis_indicators = sum(1 for indicator in gis_indicators if any(indicator in col for col in column_names)) >= 2
        
        return has_required and has_gis_indicators
    
    def extract_data(self, df: pd.DataFrame) -> pd.DataFrame:
        records = []
        
        for _, row in df.iterrows():
            # Extract basic information
            parcel_id = str(row.get(self.column_mapping.get("parcel", ""), "") or "").strip()
            owner = str(row.get(self.column_mapping.get("owner", ""), "") or "").strip()
            address = str(row.get(self.column_mapping.get("address", ""), "") or "").strip()
            grantor1 = str(row.get(self.column_mapping.get("grantor1", ""), "") or "").strip()
            grantor2 = str(row.get(self.column_mapping.get("grantor2", ""), "") or "").strip()
            
            # Extract mailing address information
            mailing_address = str(row.get(self.column_mapping.get("mailing_address", ""), "") or "").strip()
            mailing_city = str(row.get(self.column_mapping.get("mailing_city", ""), "") or "").strip()
            mailing_state = str(row.get(self.column_mapping.get("mailing_state", ""), "") or "").strip()
            mailing_zip = str(row.get(self.column_mapping.get("mailing_zip", ""), "") or "").strip()
            
            if not owner:  # Skip records without owner
                continue
            
            # Only include inherited properties in niche file
            is_inherited = self.detect_inherited_property(owner, grantor1, grantor2)
            if not is_inherited:
                continue
            
            # Parse owner name (GIS format handled by overridden parse_owner method)
            last, first = self.parse_owner(owner)
            
            record = self.create_standard_record(
                **{
                    "Parcel ID": parcel_id,
                    "Current Owner": owner,
                    "Owner 1 Last Name": last,
                    "Owner 1 First Name": first,
                    "Address": self.normalize_address(address),
                    "Mailing Address": mailing_address,
                    "Mailing City": mailing_city,
                    "Mailing State": mailing_state,
                    "Mailing Zip": mailing_zip,
                    "FIPS": self.region_fips or ""
                }
            )
            
            records.append(record)
        
        if not records:
            return pd.DataFrame(columns=self.standard_columns)
        
        return pd.DataFrame(records).drop_duplicates(subset=["Address", "Parcel ID"]).reset_index(drop=True)
    
    def detect_inherited_property(self, owner_name: str, grantor1_name: str = '', grantor2_name: str = '') -> bool:
        """
        Detect if property is likely inherited by comparing owner and grantor surnames.
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
        Extract surname from name based on the configured name format.
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
            if len(surname) >= 3 and len(surname) <= 25 and not any(char.isdigit() for char in surname):
                return surname
            return ''
        
        # Handle joint ownership with "&" - get first person's surname
        if ' & ' in name:
            first_person = name.split(' & ')[0].strip()
            return self._extract_surname_from_words(first_person)
        
        # Handle space-separated names
        return self._extract_surname_from_words(name)
    
    def _extract_surname_from_words(self, name: str) -> str:
        """Extract surname from space-separated name based on name format"""
        words = name.split()
        
        if len(words) < 1:
            return ''
        
        # Skip if looks like an address
        address_words = ['STREET', 'ROAD', 'AVENUE', 'LANE', 'DRIVE', 'ST', 'RD', 'AVE', 'SW', 'NW', 'SE', 'NE']
        if any(word in address_words for word in words):
            return ''
        
        if self.name_format == "lastname_first":
            # Format: "LASTNAME FIRSTNAME MIDDLENAME" - first word is surname
            first_word = words[0]
            if (len(first_word) >= 3 and 
                len(first_word) <= 25 and 
                not any(char.isdigit() for char in first_word)):
                return first_word
        else:
            # Format: "FIRSTNAME MIDDLENAME LASTNAME" - last word is surname
            suffixes = ['JR', 'SR', 'III', 'II', 'IV', 'V']
            last_word = words[-1]
            
            # If last word is a suffix, use second to last
            if last_word in suffixes and len(words) >= 2:
                surname = words[-2]
            else:
                surname = last_word
            
            if (len(surname) >= 3 and 
                len(surname) <= 25 and 
                not any(char.isdigit() for char in surname) and
                surname not in suffixes):
                return surname
        
        return ''
    
    def parse_owner(self, name: str) -> Tuple[str, str]:
        """
        Override base class to handle GIS format: "LASTNAME FIRSTNAME MIDDLENAME"
        """
        if not isinstance(name, str):
            return "", ""
        name = name.strip()
        if not name:
            return "", ""
        
        # Handle comma-separated names (LASTNAME, FIRSTNAME)
        if "," in name:
            last, first = name.split(",", 1)
            return last.strip(), first.strip()
        
        # Handle joint ownership with "&" - get first person
        if " & " in name:
            first_person = name.split(" & ")[0].strip()
            return self.parse_owner(first_person)
        
        # Handle space-separated names: "LASTNAME FIRSTNAME MIDDLENAME"
        parts = name.split()
        if len(parts) >= 2:
            return parts[0], parts[1]  # First word = last name, second word = first name
        elif len(parts) == 1:
            return parts[0], ""  # Only last name
        
        return name, ""


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
            },
            "inherited": {
                "type": "gis_parcel",
                "column_mapping": {
                    "parcel": "TAXID",
                    "owner": "OWNER",
                    "address": "LOCADDR",
                    "grantor1": "GRANTOR1",
                    "grantor2": "GRANTOR2",
                    "mailing_address": "OWNERADDR1",
                    "mailing_city": "MAILCITY",
                    "mailing_state": "MAILSTATE",
                    "mailing_zip": "MAINZIPCOD"
                },
                "name_format": "lastname_first"
            }
        }
    
    def detect_data_type(self, file_path: Path) -> Optional[str]:
        filename = file_path.name.lower()
        if "code" in filename and "enforcement" in filename:
            return "code_enforcement"
        elif ("tax" in filename or "delinquent" in filename) and ("delinquent" in filename or "delinq" in filename or "real estate" in filename):
            return "tax_delinquent"
        elif "parcel" in filename and ("gis" in filename or "parcels" in filename):
            return "inherited"
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
        elif config["type"] == "gis_parcel":
            return GISParcelCleaner(data_type, region, config["column_mapping"], config.get("name_format", "lastname_first"))
        return None
    
    def process_file(self, input_path: Path, region: str, data_type: str = None, output_date: str = None) -> Path:
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        # Auto-detect data type if not provided
        if not data_type:
            data_type = self.detect_data_type(input_path)
            if not data_type:
                raise ValueError(f"Could not determine data type from filename: {input_path.name}")
        
        # Get FIPS code for the region
        try:
            import sys
            import os
            parent_dir = os.path.join(os.path.dirname(__file__), '..')
            sys.path.append(parent_dir)
            from multi_region_config import MultiRegionConfigManager
            
            config_manager = MultiRegionConfigManager()
            region_config = config_manager.get_region_config(region)
            region_fips = region_config.fips_code
        except Exception as e:
            print(f"WARNING: Could not get FIPS code: {e}")
            region_fips = "UNKNOWN"
        
        # Create appropriate cleaner
        cleaner = self.create_cleaner(data_type, region)
        if not cleaner:
            raise ValueError(f"No cleaner available for data type: {data_type}")
        
        # Set FIPS code on cleaner
        cleaner.region_fips = region_fips
        
        # Read and process file - handle both CSV and Excel
        if input_path.suffix.lower() == '.csv':
            df = pd.read_csv(input_path, dtype=str)
        else:
            df = pd.read_excel(input_path, header=None, dtype=str)
        
        # Verify cleaner can handle this format
        if not cleaner.detect_format(df):
            raise ValueError(f"File format not compatible with {data_type} cleaner")
        
        # Extract and standardize data
        cleaned_df = cleaner.extract_data(df)
        
        # FIPS is now handled by individual cleaners during record creation
        
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
    
    def process_all_region_files(self, region: str, output_date: str = None) -> List[Path]:
        """
        Process all government data files for a region.
        
        Args:
            region: Region key (e.g., roanoke_city_va)
            output_date: YYYYMMDD for output filename (auto-generated if not specified)
            
        Returns:
            List of output file paths
        """
        # Look for government data directory
        gov_data_dir = Path("government_data") / region
        if not gov_data_dir.exists():
            print(f"Government data directory not found: {gov_data_dir}")
            return []
        
        # Find all potential government data files
        file_patterns = [
            "**/*.xlsx", "**/*.xls", "**/*.csv"
        ]
        
        found_files = []
        for pattern in file_patterns:
            found_files.extend(gov_data_dir.glob(pattern))
        
        if not found_files:
            print(f"No government data files found in {gov_data_dir}")
            return []
        
        print(f"Found {len(found_files)} potential government data files in {gov_data_dir}")
        
        output_paths = []
        processed_count = 0
        skipped_count = 0
        
        for file_path in found_files:
            try:
                # Try to detect data type
                data_type = self.detect_data_type(file_path)
                if not data_type:
                    print(f"  Skipped {file_path.name} (unknown data type)")
                    skipped_count += 1
                    continue
                
                print(f"  Processing {file_path.name} as {data_type}...")
                
                # Process the file
                output_path = self.process_file(file_path, region, data_type, output_date)
                output_paths.append(output_path)
                processed_count += 1
                
            except Exception as e:
                print(f"  Error processing {file_path.name}: {e}")
                skipped_count += 1
                continue
        
        print(f"\nBatch processing complete:")
        print(f"  Processed: {processed_count} files")
        print(f"  Skipped: {skipped_count} files")
        
        return output_paths


def main():
    parser = argparse.ArgumentParser(description="Standardize government data files into niche format")
    parser.add_argument("--input", help="Path to government data file (required unless using --process-all)")
    parser.add_argument("--region", required=True, help="Region key (e.g., roanoke_city_va)")
    parser.add_argument("--type", help="Data type (auto-detected if not specified)")
    parser.add_argument("--date", help="YYYYMMDD for output filename (auto-inferred if not specified)")
    parser.add_argument("--list-types", action="store_true", help="List supported data types")
    parser.add_argument("--process-all", action="store_true", help="Process all government data files for the region")
    
    args = parser.parse_args()
    
    standardizer = GovernmentDataStandardizer()
    
    if args.list_types:
        print("Supported data types:")
        for data_type, config in standardizer.data_type_configs.items():
            print(f"  {data_type}: {config['type']}")
        return
    
    if args.process_all:
        try:
            output_paths = standardizer.process_all_region_files(args.region, args.date)
            if output_paths:
                print(f"Processed {len(output_paths)} government data files for {args.region}:")
                for output_path in output_paths:
                    print(f"  {output_path}")
            else:
                print(f"No government data files found for region {args.region}")
        except Exception as e:
            print(f"Error processing region files: {e}")
            exit(1)
        return
    
    if not args.input:
        print("Error: --input is required unless using --process-all")
        exit(1)
    
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