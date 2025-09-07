# Python-SQL Hybrid Architecture Migration Guide

## Overview

This document outlines the migration from SQL-heavy data processing to a Python-first hybrid architecture for the real estate direct mail processing system. The goal is to move complex business logic to maintainable Python code while preserving SQL's efficiency for database operations.

## Current Architecture Problem

The `ProcessUploadLog_Newtest` stored procedure (1,100+ lines) handles both:
- **Data Processing**: Complex business rules, property classification, priority scoring
- **Database Operations**: MERGE statements, foreign key management, data integrity

This creates maintenance challenges and makes business logic difficult to test and modify.

## Target Hybrid Architecture

### Python Responsibilities
- Property classification (Trust, Church, Business detection)
- Priority scoring with complex business rules
- Owner occupancy detection via address comparison
- Niche list integration and enhancement
- Data validation and quality checks
- Pre-computation of all derived fields

### SQL Responsibilities  
- Efficient MERGE operations for Owner and Location tables
- Foreign key resolution and data integrity
- Database constraints and audit logging
- Performance-optimized database operations

## Critical Database Compatibility Requirements

### Monthly Update Process
The system processes the same properties monthly with potential data changes. The hybrid system MUST:

1. **Preserve Existing Unique Identifiers**
2. **Maintain MERGE Operation Logic**
3. **Support Existing Update Patterns**
4. **Not Break Current Database Structure**

## Existing SQL Unique Identifiers (MUST PRESERVE)

### Owner Table Matching
```sql
-- Current SQL Logic
MERGE Owner AS o
USING (...) as S
ON s.ownerSlug = o.Slug  -- PRIMARY MATCHING KEY

-- OwnerSlug Creation Formula:
dbo.ProperCase(UPPER(REPLACE(REPLACE(MAX(RTRIM(LTRIM(ss.Street))), '.', ''), ',',''))+'_'+UPPER(REPLACE(REPLACE(MAX(RTRIM(LTRIM(ss.ZipCode))), '.', ''), ',','')))

-- Example: "123_MAIN_ST_24016"
```

### Location Table Matching  
```sql
-- Current SQL Logic
MERGE Location AS l
USING (...) as S
ON S.LocationPrimaryId = l.PrimaryId AND l.RegionId = @regionId

-- LocationPrimaryId comes from source data (unchanged)
-- RegionId is system parameter (e.g., 40)
```

### Monthly Update Conditions
```sql
-- Only updates recent records (within 6 months)
WHEN MATCHED AND (l.InsertedDate >= DATEADD(MM, -6, GETDATE()) 
                 or l.UpdatedDate >= DATEADD(MM, -6, GETDATE())) THEN
```

## Simplified Modern Architecture (Recommended Approach)

### Replace Complex List Code System with Clean Data Model

The current SQL system creates complex compound codes like `"HE-Liens-ABS1"` and requires dynamic `ListPriorityId` creation. **This is unnecessarily complex and hard to maintain.** 

Instead, let's modernize with a **clean separation of concerns**:

### Python Must Handle Dynamic List Creation

```python
class ListPriorityManager:
    """Manages dynamic creation of ListPriority records and their unique IDs"""
    
    def __init__(self, db_integrator: DatabaseIntegrator):
        self.db_integrator = db_integrator
        self._list_priority_cache = {}  # Cache existing codes to avoid duplicates
        
    def get_or_create_list_priority_id(self, list_code: str) -> int:
        """
        Get existing ListPriorityId or create new one for unique list code.
        
        CRITICAL: This replicates the SQL MERGE logic for ListPriority table.
        """
        if list_code in self._list_priority_cache:
            return self._list_priority_cache[list_code]
            
        # Check if code already exists in database
        existing_id = self._get_existing_list_priority_id(list_code)
        if existing_id:
            self._list_priority_cache[list_code] = existing_id
            return existing_id
            
        # Create new ListPriority record
        new_id = self._create_new_list_priority(list_code)
        self._list_priority_cache[list_code] = new_id
        return new_id
    
    def _get_existing_list_priority_id(self, list_code: str) -> Optional[int]:
        """Query database for existing ListPriorityId"""
        with pyodbc.connect(self.db_integrator.conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ListPriorityId FROM ListPriority WHERE Code = ?", list_code)
            result = cursor.fetchone()
            return result[0] if result else None
    
    def _create_new_list_priority(self, list_code: str) -> int:
        """Create new ListPriority record and return its ID"""
        with pyodbc.connect(self.db_integrator.conn_str) as conn:
            cursor = conn.cursor()
            
            # Insert new ListPriority record
            cursor.execute("""
                INSERT INTO ListPriority (Code, Name, PriorityLevel, ProcessingOrder, IsAbsentee)
                VALUES (?, ?, 0, 0, 2)
            """, list_code, list_code)
            
            # Get the new ID
            cursor.execute("SELECT SCOPE_IDENTITY()")
            new_id = int(cursor.fetchone()[0])
            
            # Add to RegionListFrequency for all regions (replicates SQL logic)
            cursor.execute("SELECT RegionId FROM Region")
            all_regions = [row[0] for row in cursor.fetchall()]
            
            for region_id in all_regions:
                cursor.execute("""
                    INSERT INTO RegionListFrequency (Frequency, ListId, RegionId, DividedBy)
                    VALUES (26, ?, ?, 4)
                """, new_id, region_id)
            
            conn.commit()
            return new_id

    def generate_enhanced_list_codes(self, processed_df: pd.DataFrame) -> pd.Series:
        """
        Generate enhanced list codes following SQL logic from ProcessUploadLog_Newtest.
        
        This replicates the complex enhancement logic in lines 807-841 of the SQL procedure.
        """
        enhanced_codes = processed_df['PriorityCode'].copy()  # Start with base priority code
        
        # High Equity enhancement (EstLoanToValue <= 50% and > 0%)
        high_equity_mask = (
            (processed_df['EstLoanToValue'] <= 50.0) & 
            (processed_df['EstLoanToValue'] > 0.0)
        )
        enhanced_codes.loc[high_equity_mask] = 'HE-' + enhanced_codes.loc[high_equity_mask]
        
        # Liens enhancement
        liens_mask = processed_df['LienType'].notna() & (processed_df['LienType'] != '')
        enhanced_codes.loc[liens_mask] = 'Liens-' + enhanced_codes.loc[liens_mask]
        
        # Bankruptcy enhancement  
        bankruptcy_mask = processed_df['BKDate'].notna()
        enhanced_codes.loc[bankruptcy_mask] = 'Bankrupcy-' + enhanced_codes.loc[bankruptcy_mask]
        
        # Divorce enhancement
        divorce_mask = processed_df['DivorceDate'].notna()
        enhanced_codes.loc[divorce_mask] = 'Divorce-' + enhanced_codes.loc[divorce_mask]
        
        # Pre-Foreclosure enhancement
        prefc_mask = processed_df['PreFcRecordingDate'].notna()
        enhanced_codes.loc[prefc_mask] = 'PreFor-' + enhanced_codes.loc[prefc_mask]
        
        # Free & Clear enhancement (EstLoanToValue = 0 and EstEquity exists)
        free_clear_mask = (
            (processed_df['EstLoanToValue'] == 0.0) & 
            processed_df['EstEquity'].notna() & 
            (processed_df['EstEquity'] != '')
        )
        enhanced_codes.loc[free_clear_mask] = 'F&C-' + enhanced_codes.loc[free_clear_mask]
        
        # Vacant enhancement
        vacant_mask = processed_df['Vacant'] == 'Yes'
        enhanced_codes.loc[vacant_mask] = 'Vacant-' + enhanced_codes.loc[vacant_mask]
        
        # NCOA Moves enhancement (ResponseNCOA = 'A', '91', or '92')
        ncoa_moves_mask = processed_df['ResponseNCOA'].isin(['A', '91', '92'])
        enhanced_codes.loc[ncoa_moves_mask] = 'NCOA_Moves-' + enhanced_codes.loc[ncoa_moves_mask]
        
        # NCOA Drops enhancement (ResponseNCOA exists but not 'A', '91', '92')
        ncoa_drops_mask = (
            processed_df['ResponseNCOA'].notna() & 
            (processed_df['ResponseNCOA'] != '') &
            ~processed_df['ResponseNCOA'].isin(['A', '91', '92'])
        )
        enhanced_codes.loc[ncoa_drops_mask] = 'NCOA_Drops-' + enhanced_codes.loc[ncoa_drops_mask]
        
        return enhanced_codes
```

### Integration with Niche List Processing

```python
def process_niche_list_integration(self, main_df: pd.DataFrame, niche_files: List[str], 
                                 list_manager: ListPriorityManager) -> pd.DataFrame:
    """
    Integrate niche lists and create compound list codes.
    
    Replicates GetFinalizedNicheList stored procedure logic.
    """
    enhanced_df = main_df.copy()
    
    for niche_file in niche_files:
        niche_df = pd.read_excel(niche_file)
        niche_type = self._extract_niche_type_from_filename(niche_file)  # e.g., "Liens", "Foreclosure"
        
        # Match on PrimaryLocationId first, then address fallback
        matched_records = self._match_niche_records(enhanced_df, niche_df)
        
        for idx, record in matched_records.iterrows():
            existing_code = record['PriorityCode']
            
            # Create compound code: "NicheType - ExistingCode"
            compound_code = f"{niche_type} - {existing_code}"
            
            # Check if existing code already contains this niche type
            if f" - {existing_code}" in existing_code and niche_type in existing_code:
                compound_code = existing_code  # Don't duplicate
            
            # Get or create ListPriorityId for this unique compound code
            list_priority_id = list_manager.get_or_create_list_priority_id(compound_code)
            
            # Update the record
            enhanced_df.loc[idx, 'PriorityCode'] = compound_code
            enhanced_df.loc[idx, 'ListPriorityId'] = list_priority_id
    
    return enhanced_df
```

## Python Output Specification

### Required Python Output Format

Python must generate TWO DataFrames using EXACT same unique identifiers AND proper ListPriorityId values:

```python
def generate_database_ready_output(self, processed_df: pd.DataFrame, region_id: int) -> tuple:
    """
    Generate database-ready DataFrames for SQL MERGE operations.
    
    CRITICAL: Must use same unique identifiers as existing SQL system.
    """
    
    # 1. OWNERS DataFrame
    owners_df = pd.DataFrame({
        'OwnerSlug': self._create_owner_slugs(processed_df),  # EXACT SQL formula
        'Name': processed_df['OwnerName'],
        'FirstName': processed_df['OwnerFirstName'], 
        'LastName': processed_df['OwnerLastName'],
        'Address1': processed_df['OwnerAddress1'],
        'Address2': processed_df['OwnerAddress2'],
        'City': processed_df['OwnerCity'],
        'State': processed_df['OwnerState'],
        'Zip': processed_df['OwnerZip'],
        'IsBusiness': processed_df['IsBusiness'],      # Python computed
        'AlternateName': processed_df['AlternateName'], # Python computed  
        'DoNotMail': processed_df['DoNotMail'],
        'MailingZip4': processed_df['OwnerMailingZip4'],
        'MailingCounty': processed_df['LocationCounty'],
        'AccuzipRequest': processed_df['RequestStreet'] + '_' + 
                         processed_df['RequestCity'] + '_' + 
                         processed_df['RequestState'] + '_' + 
                         processed_df['RequestZip'],
        'AccuzipResponse': processed_df['FullAccuzipResponse']
    })
    
    # 2. LOCATIONS DataFrame
    locations_df = pd.DataFrame({
        'PrimaryId': processed_df['LocationPrimaryId'],    # FROM SOURCE - UNCHANGED
        'RegionId': region_id,                             # PARAMETER (e.g., 40)
        'OwnerSlug': self._create_owner_slugs(processed_df), # Links to Owner
        
        # Python Pre-Computed Values (replaces SQL UPDATE logic):
        'ListPriorityId': processed_df['ListPriorityId'],     # FROM ListPriorityManager - CRITICAL!
        'IsOwnerOccupied': processed_df['IsOwnerOccupied'],   # Python computed
        'IsTrust': processed_df['IsTrust'],                   # Python classified
        'IsChurch': processed_df['IsChurch'],                 # Python classified
        'IsBusiness': processed_df['IsBusiness'],             # Python classified
        'OwnerGrantorMatch': processed_df['OwnerGrantorMatch'], # Python computed
        
        # All other location fields from source:
        'Address1': processed_df['LocationAddress1'],
        'City': processed_df['LocationCity'],
        'State': processed_df['LocationState'], 
        'Zip': processed_df['LocationZip'],
        'SellDate': processed_df['LocationSellDate'],
        'SellAmount': processed_df['LocationSellAmount'],
        'Grantor': processed_df['LocationGrantor'],
        'Neighborhood': processed_df['LocationNeighborhood'],
        'Acres': processed_df['LocationAcres'],
        # ... ALL other location fields from existing system
    })
    
    return owners_df, locations_df

def _create_owner_slugs(self, df: pd.DataFrame) -> pd.Series:
    """
    Create OwnerSlug using EXACT same formula as SQL.
    
    CRITICAL: Must match SQL formula exactly for monthly updates to work.
    """
    # Replicate: dbo.ProperCase(UPPER(street_cleaned)+'_'+zip_cleaned)
    street_clean = (df['OwnerAddress1']
                   .fillna('')
                   .astype(str)
                   .str.upper()
                   .str.replace('.', '', regex=False)
                   .str.replace(',', '', regex=False) 
                   .str.strip()
                   .str.replace(r'\s+', '_', regex=True))  # Replace spaces with underscores
    
    zip_clean = (df['OwnerZip']
                .fillna('')
                .astype(str)
                .str.upper()
                .str.replace('.', '', regex=False)
                .str.replace(',', '', regex=False)
                .str.strip())
    
    return street_clean + '_' + zip_clean
```

## Streamlined SQL Procedure

### New Procedure: `ProcessPythonEnhancedData`

```sql
ALTER PROCEDURE [dbo].[ProcessPythonEnhancedData] (
    @uploadLogId INT,
    @regionId INT,
    @OwnersJson NVARCHAR(MAX),      -- JSON from Python owners_df
    @LocationsJson NVARCHAR(MAX)    -- JSON from Python locations_df
)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update processing state
    UPDATE UploadLog SET ProcessingState = 7 WHERE UploadLogId = @uploadLogId;
    
    -- Parse Python JSON data into temp tables
    SELECT * INTO #PythonOwners FROM OPENJSON(@OwnersJson) WITH (
        OwnerSlug NVARCHAR(255),
        Name NVARCHAR(255),
        FirstName NVARCHAR(255),
        LastName NVARCHAR(255),
        Address1 NVARCHAR(255),
        Address2 NVARCHAR(255),
        City NVARCHAR(255),
        State NVARCHAR(255),
        Zip NVARCHAR(255),
        IsBusiness BIT,
        AlternateName NVARCHAR(255),
        DoNotMail NVARCHAR(255),
        MailingZip4 NVARCHAR(50),
        MailingCounty NVARCHAR(255),
        AccuzipRequest NVARCHAR(500),
        AccuzipResponse NVARCHAR(MAX)
    );
    
    SELECT * INTO #PythonLocations FROM OPENJSON(@LocationsJson) WITH (
        PrimaryId NVARCHAR(255),
        RegionId INT,
        OwnerSlug NVARCHAR(255),
        ListPriorityId INT,
        IsOwnerOccupied BIT,
        IsTrust BIT,
        IsChurch BIT,
        IsBusiness BIT,
        OwnerGrantorMatch BIT,
        Address1 NVARCHAR(255),
        City NVARCHAR(255),
        State NVARCHAR(255),
        Zip NVARCHAR(255),
        SellDate DATETIME,
        SellAmount DECIMAL(18,2),
        Grantor NVARCHAR(255)
        -- Add ALL other location fields
    );
    
    -- MERGE Owners (same logic as before, but with pre-processed data)
    UPDATE UploadLog SET ProcessingState = 8 WHERE UploadLogId = @uploadLogId;
    
    MERGE Owner AS o
    USING #PythonOwners AS s
    ON s.OwnerSlug = o.Slug
    WHEN MATCHED THEN
        UPDATE SET 
            [Name] = s.Name,
            FirstName = s.FirstName,
            LastName = s.LastName,
            Address1 = s.Address1,
            Address2 = s.Address2,
            City = s.City,
            [State] = s.State,
            Zip = s.Zip,
            AlternateName = s.AlternateName,
            AccuzipRequest = s.AccuzipRequest,
            AccuzipResponse = s.AccuzipResponse,
            ModifiedDate = GETDATE(),
            UpdatedDate = GETDATE(),
            MailingZip4 = s.MailingZip4,
            MailingCounty = s.MailingCounty,
            DoNotMail = s.DoNotMail
    WHEN NOT MATCHED THEN
        INSERT ([Name], FirstName, LastName, Address1, Address2, City, [State], Zip, 
                Slug, AccuzipRequest, AccuzipResponse, InsertedDate, MailingZip4, 
                MailingCounty, DoNotMail)
        VALUES (s.Name, s.FirstName, s.LastName, s.Address1, s.Address2, s.City, 
                s.State, s.Zip, s.OwnerSlug, s.AccuzipRequest, s.AccuzipResponse, 
                GETDATE(), s.MailingZip4, s.MailingCounty, s.DoNotMail);
    
    -- MERGE Locations (same logic as before, but with pre-processed data)
    UPDATE UploadLog SET ProcessingState = 9 WHERE UploadLogId = @uploadLogId;
    
    -- CRITICAL: ListPriorityId is now PRE-COMPUTED by Python
    -- The SQL procedure should NOT recalculate priority - just use the Python value
    
    MERGE Location AS l
    USING (
        SELECT pl.*, o.OwnerId 
        FROM #PythonLocations pl
        JOIN Owner o ON pl.OwnerSlug = o.Slug
    ) AS s
    ON s.PrimaryId = l.PrimaryId AND l.RegionId = @regionId
    WHEN MATCHED 
        AND (l.InsertedDate >= DATEADD(MM, -6, GETDATE()) 
             OR l.UpdatedDate >= DATEADD(MM, -6, GETDATE())) 
    THEN
        UPDATE SET
            OwnerId = s.OwnerId,
            SellDate = s.SellDate,
            SellAmount = s.SellAmount,
            Address1 = s.Address1,
            City = s.City,
            State = s.State,
            Zip = s.Zip,
            Grantor = s.Grantor,
            IsBusiness = s.IsBusiness,
            IsChurch = s.IsChurch,
            IsTrust = s.IsTrust,
            IsOwnerOccupied = s.IsOwnerOccupied,
            OwnerGrantorMatch = s.OwnerGrantorMatch,
            ListPriorityId = s.ListPriorityId,  -- Pre-computed by Python!
            ModifiedDate = GETDATE(),
            UpdatedDate = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (RegionId, PrimaryId, OwnerId, SellDate, SellAmount, Address1, City, State, Zip,
                Grantor, IsBusiness, IsChurch, IsTrust, IsOwnerOccupied, OwnerGrantorMatch,
                ListPriorityId, InsertedDate)
        VALUES (@regionId, s.PrimaryId, s.OwnerId, s.SellDate, s.SellAmount, s.Address1, 
                s.City, s.State, s.Zip, s.Grantor, s.IsBusiness, s.IsChurch, s.IsTrust,
                s.IsOwnerOccupied, s.OwnerGrantorMatch, s.ListPriorityId, GETDATE());
    
    -- Final updates and cleanup (same as before)
    UPDATE UploadLog SET ProcessingState = 10 WHERE UploadLogId = @uploadLogId;
    
    UPDATE [Owner] SET FullAddress = REPLACE(Address1, '''', '') + ' ' + 
                                    REPLACE(City, '''', '') + ' ' + 
                                    REPLACE([State], '''', '') + ' ' + 
                                    REPLACE(Zip, '''', '')
    WHERE OwnerId IN(SELECT DISTINCT OwnerId FROM [Location] WHERE RegionId = @regionId);
    
    -- Complete processing
    DECLARE @TotalProcessCount INT = (SELECT COUNT(*) FROM #PythonLocations);
    UPDATE UploadLog SET 
        RowsProcessed = @TotalProcessCount,
        ProcessingState = 3,
        ProcessingEnded = GETDATE() 
    WHERE UploadLogId = @uploadLogId;
    
    -- Cleanup
    DROP TABLE #PythonOwners;
    DROP TABLE #PythonLocations;
END
```

## Technology Stack for AWS EC2 SQL Server

### Required Components

**Python Environment:**
```bash
pip install pyodbc pandas sqlalchemy boto3
```

**AWS EC2 SQL Server:**
- SQL Server 2016+ (for OPENJSON support)
- TCP/IP enabled on port 1433
- Security Group allowing inbound connections on port 1433
- SQL Server Authentication enabled

**ODBC Driver Installation:**

**Windows:**
- Download "ODBC Driver 17 for SQL Server" from Microsoft

**Linux:**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

**macOS:**
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql17 mssql-tools
```

### Connection Security Options

**Option 1: Direct Connection (Simple)**
```python
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=your-ec2-public-ip,1433;"  # or DNS name
    "DATABASE=YourDatabase;"
    "UID=your_username;"
    "PWD=your_password;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
```

**Option 2: Through VPN/Private Network (More Secure)**
```python
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=your-ec2-private-ip,1433;"  # Private IP through VPN
    "DATABASE=YourDatabase;"
    "UID=your_username;"
    "PWD=your_password;"
)
```

**Option 3: Using AWS Systems Manager (Most Secure)**
```python
import boto3

def get_db_credentials():
    ssm = boto3.client('ssm')
    username = ssm.get_parameter(Name='/myapp/db/username', WithDecryption=True)['Parameter']['Value']
    password = ssm.get_parameter(Name='/myapp/db/password', WithDecryption=True)['Parameter']['Value']
    return username, password
```

### Connection Testing
```python
import pyodbc

def test_connection(conn_str):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        result = cursor.fetchone()
        print("Connected successfully:", result[0])
        
        # Test OPENJSON functionality
        cursor.execute("""
            SELECT * FROM OPENJSON('[{"name":"test","value":123}]') 
            WITH (name NVARCHAR(50), value INT)
        """)
        print("OPENJSON test passed")
        conn.close()
        return True
    except Exception as e:
        print("Connection failed:", e)
        return False
```

## Python-SQL Integration Layer

```python
import pyodbc
import pandas as pd
import boto3
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

class DatabaseIntegrator:
    """Handles the handoff between Python processing and SQL database operations"""
    
    def __init__(self, connection_string: str = None, use_aws_credentials: bool = False):
        """
        Initialize database integrator.
        
        Args:
            connection_string: Direct connection string, or None to build from AWS credentials
            use_aws_credentials: If True, retrieve credentials from AWS Parameter Store
        """
        if use_aws_credentials:
            self.conn_str = self._build_connection_from_aws()
        else:
            self.conn_str = connection_string
            
        # Test connection on initialization
        if not self._test_connection():
            raise ConnectionError("Failed to connect to SQL Server")
    
    def _build_connection_from_aws(self) -> str:
        """Build connection string using AWS Parameter Store credentials"""
        try:
            ssm = boto3.client('ssm')
            
            server = ssm.get_parameter(Name='/myapp/db/server')['Parameter']['Value']
            database = ssm.get_parameter(Name='/myapp/db/database')['Parameter']['Value']
            username = ssm.get_parameter(Name='/myapp/db/username', WithDecryption=True)['Parameter']['Value']
            password = ssm.get_parameter(Name='/myapp/db/password', WithDecryption=True)['Parameter']['Value']
            
            return (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={server},1433;"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                "Encrypt=yes;"
                "TrustServerCertificate=no;"
            )
        except Exception as e:
            logger.error(f"Failed to retrieve AWS credentials: {e}")
            raise
    
    def _test_connection(self) -> bool:
        """Test database connection and OPENJSON functionality"""
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                
                # Test basic connection
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]
                logger.info(f"Connected to SQL Server: {version}")
                
                # Test OPENJSON functionality (required for data transfer)
                cursor.execute("""
                    SELECT * FROM OPENJSON('[{"name":"test","value":123}]') 
                    WITH (name NVARCHAR(50), value INT)
                """)
                result = cursor.fetchone()
                if result[0] != 'test' or result[1] != 123:
                    raise ValueError("OPENJSON test failed")
                
                logger.info("OPENJSON functionality verified")
                return True
                
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def insert_processed_data(self, owners_df: pd.DataFrame, locations_df: pd.DataFrame, 
                            region_id: int, upload_log_id: int) -> dict:
        """
        Insert Python-processed data into SQL database using streamlined procedure.
        
        Args:
            owners_df: DataFrame with pre-processed owner data
            locations_df: DataFrame with pre-processed location data  
            region_id: Region identifier (e.g., 40)
            upload_log_id: Upload log tracking ID
            
        Returns:
            dict: Processing results and statistics
        """
        try:
            logger.info(f"Starting database insert for upload_log_id: {upload_log_id}")
            logger.info(f"Processing {len(owners_df)} owners and {len(locations_df)} locations")
            
            # Validate DataFrames before processing
            self._validate_dataframes(owners_df, locations_df)
            
            # Convert DataFrames to JSON for SQL consumption
            owners_json = owners_df.to_json(orient='records', date_format='iso')
            locations_json = locations_df.to_json(orient='records', date_format='iso')
            
            # Log JSON size for monitoring
            logger.info(f"JSON sizes - Owners: {len(owners_json):,} chars, Locations: {len(locations_json):,} chars")
            
            # Execute streamlined SQL procedure
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    EXEC ProcessPythonEnhancedData 
                        @uploadLogId=?, 
                        @regionId=?, 
                        @OwnersJson=?, 
                        @LocationsJson=?
                """, upload_log_id, region_id, owners_json, locations_json)
                
                conn.commit()
                logger.info("Database insert completed successfully")
                
                # Get processing results
                cursor.execute("""
                    SELECT RowsProcessed, ProcessingState, ProcessingEnded 
                    FROM UploadLog 
                    WHERE UploadLogId = ?
                """, upload_log_id)
                
                result = cursor.fetchone()
                
                return {
                    'success': True,
                    'rows_processed': result[0] if result else 0,
                    'processing_state': result[1] if result else None,
                    'processing_ended': result[2] if result else None,
                    'owners_count': len(owners_df),
                    'locations_count': len(locations_df),
                    'upload_log_id': upload_log_id,
                    'region_id': region_id
                }
                
        except pyodbc.Error as e:
            logger.error(f"Database error: {e}")
            return {
                'success': False,
                'error': f"Database error: {str(e)}",
                'error_type': 'database'
            }
        except Exception as e:
            logger.error(f"Database integration error: {e}")
            return {
                'success': False,
                'error': str(e),
                'error_type': 'general'
            }
    
    def _validate_dataframes(self, owners_df: pd.DataFrame, locations_df: pd.DataFrame):
        """Validate DataFrames have required columns and data types"""
        
        required_owner_columns = [
            'OwnerSlug', 'Name', 'FirstName', 'LastName', 'Address1', 'City', 'State', 'Zip'
        ]
        required_location_columns = [
            'PrimaryId', 'RegionId', 'OwnerSlug', 'ListPriorityId', 'IsOwnerOccupied',
            'IsTrust', 'IsChurch', 'IsBusiness'
        ]
        
        # Check owners DataFrame
        missing_owner_cols = set(required_owner_columns) - set(owners_df.columns)
        if missing_owner_cols:
            raise ValueError(f"Owners DataFrame missing required columns: {missing_owner_cols}")
        
        # Check locations DataFrame  
        missing_location_cols = set(required_location_columns) - set(locations_df.columns)
        if missing_location_cols:
            raise ValueError(f"Locations DataFrame missing required columns: {missing_location_cols}")
        
        # Validate no null OwnerSlugs
        if owners_df['OwnerSlug'].isnull().any():
            raise ValueError("Owners DataFrame contains null OwnerSlug values")
        
        if locations_df['OwnerSlug'].isnull().any():
            raise ValueError("Locations DataFrame contains null OwnerSlug values")
        
        # Validate all location OwnerSlugs exist in owners DataFrame
        owner_slugs = set(owners_df['OwnerSlug'])
        location_slugs = set(locations_df['OwnerSlug'])
        orphaned_slugs = location_slugs - owner_slugs
        
        if orphaned_slugs:
            logger.warning(f"Found {len(orphaned_slugs)} location records with no matching owner")
            # Don't raise error - let SQL handle foreign key resolution
        
        logger.info("DataFrame validation passed")

# Example usage configurations
class DatabaseConfig:
    """Configuration helper for different deployment scenarios"""
    
    @staticmethod
    def local_development() -> str:
        """Local development connection string"""
        return (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=YourDatabase;"
            "UID=sa;"
            "PWD=YourPassword;"
            "TrustServerCertificate=yes;"
        )
    
    @staticmethod
    def aws_ec2_direct(server_ip: str, username: str, password: str, database: str) -> str:
        """Direct connection to AWS EC2 SQL Server"""
        return (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server_ip},1433;"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
        )
    
    @staticmethod
    def aws_ec2_with_ssl(server_dns: str, username: str, password: str, database: str) -> str:
        """Secure connection to AWS EC2 SQL Server with SSL"""
        return (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server_dns},1433;"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
            "Command Timeout=60;"
        )
```

## Enhanced PropertyProcessor Integration

```python
class PropertyProcessor:
    """Enhanced with database output capability"""
    
    def process_excel_file_for_database(self, file_path: str, region_id: int) -> dict:
        """
        Process Excel file and generate database-ready output.
        
        Returns:
            dict: Contains owners_df, locations_df, and processing statistics
        """
        # Existing processing logic
        processed_df = self.process_excel_file(file_path)
        
        # Generate database-ready output
        owners_df, locations_df = self.generate_database_ready_output(processed_df, region_id)
        
        return {
            'processed_df': processed_df,
            'owners_df': owners_df,
            'locations_df': locations_df,
            'total_records': len(processed_df),
            'unique_owners': len(owners_df),
            'processing_summary': self._generate_processing_summary(processed_df)
        }
```

## Migration Implementation Steps

### Phase 1: Extend Current Python System
1. **CRITICAL FIRST**: Create `ListPriorityManager` class for dynamic list code management
2. Add `generate_database_ready_output()` method to `PropertyProcessor` 
3. Implement `_create_owner_slugs()` with exact SQL formula
4. Create `DatabaseIntegrator` class
5. Integrate `ListPriorityManager` with niche list processing and enhanced code generation
6. Add comprehensive testing for OwnerSlug generation AND ListPriorityId creation

### Phase 2: Create Streamlined SQL Procedure
1. Create `ProcessPythonEnhancedData` stored procedure
2. Test with small dataset to verify MERGE logic
3. Validate that existing records update correctly
4. Performance test with full dataset

### Phase 3: Integration Testing
1. Run Python processing → SQL insert on test data
2. Compare results with current SQL-only processing
3. Verify monthly update scenarios work correctly
4. Test error handling and rollback scenarios

### Phase 4: Production Migration
1. Deploy side-by-side with existing system
2. Run parallel processing for validation period
3. Monitor performance and data quality
4. Switch over when validation complete

## Key Benefits

### Maintainability
- Business logic in readable, testable Python code
- Version control friendly (no more massive SQL procedures)
- Clear separation of concerns

### Performance  
- SQL MERGE operations remain highly efficient
- Python preprocessing eliminates complex SQL logic
- Reduced stored procedure execution time

### Data Quality
- Better validation and error handling in Python
- Comprehensive logging and audit trails  
- Data quality reporting before database insertion

### Monthly Processing
- Same update behavior as current system
- No changes to existing database structure
- Preserves all current functionality

## Risk Mitigation

### Critical Risk: ListPriorityId Management
**HIGH RISK**: The dynamic ListPriorityId creation is the most complex part of the migration. If this fails:
- New list code combinations won't get proper IDs
- Location records will reference non-existent ListPriorityId values
- Marketing frequency rules won't apply to new combinations
- Business operations will be disrupted

**Mitigation Strategy:**
1. Build and test `ListPriorityManager` in isolation FIRST
2. Compare Python-generated ListPriorityId values against SQL system on test data
3. Implement comprehensive logging of all new ListPriority record creation
4. Have rollback plan ready if ListPriorityId assignment fails

### Database Compatibility  
- Uses exact same unique identifiers as current system
- MERGE logic preserved exactly  
- Monthly update patterns unchanged
- No breaking changes to existing data
- **NEW**: ListPriorityId values must match exactly between Python and SQL systems

### Rollback Plan
- Keep existing stored procedure as backup
- Side-by-side deployment for validation
- Easy rollback if issues discovered
- Comprehensive testing before migration

This hybrid architecture provides the best of both worlds: maintainable Python business logic with efficient SQL database operations, while preserving all existing functionality and monthly update patterns.