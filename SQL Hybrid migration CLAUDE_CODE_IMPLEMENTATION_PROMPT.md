# Claude Code Implementation Prompt: Python-SQL Hybrid Architecture

## Context

This real estate direct mail processing system currently uses a 1,100-line SQL stored procedure (`ProcessUploadLog_Newtest`) for both data processing and database operations. We need to migrate to a hybrid architecture where Python handles complex business logic and SQL handles efficient database operations.

## Your Mission

Implement the Python-SQL hybrid architecture as detailed in `PYTHON_SQL_HYBRID_MIGRATION_GUIDE.md`. The critical requirement is preserving the existing monthly update functionality while moving business logic to Python.

## Key Files to Understand

### Current System Architecture
- `ProcessUploadLog_Newtest_storedProcedure.sql` - Current 1,100-line stored procedure (for reference)
- `property_processor.py` - Current Python processing (PropertyClassifier, PropertyPriorityScorer classes)
- `monthly_processing_v2.py` - Current Python orchestrator
- `multi_region_config.py` - Region configuration system

### Implementation Reference  
- `PYTHON_SQL_HYBRID_MIGRATION_GUIDE.md` - Detailed migration specification
- `CLAUDE.md` - System architecture overview

## Critical Requirements

### 1. Database Compatibility (MUST NOT BREAK)
The system processes the same properties monthly. Your implementation MUST:

**Owner Table Matching:**
- Use `OwnerSlug` as unique identifier (exact SQL formula replication)
- `OwnerSlug` format: `"123_MAIN_ST_24016"` (cleaned street + zip)
- SQL Formula to replicate:
```sql
dbo.ProperCase(UPPER(REPLACE(REPLACE(MAX(RTRIM(LTRIM(ss.Street))), '.', ''), ',',''))+'_'+UPPER(REPLACE(REPLACE(MAX(RTRIM(LTRIM(ss.ZipCode))), '.', ''), ',','')))
```

**Location Table Matching:**
- Use `LocationPrimaryId + RegionId` as unique identifier  
- `LocationPrimaryId` comes from source data (unchanged)
- `RegionId` is system parameter (e.g., 40)

### 2. Monthly Update Logic Preservation
```sql
-- Only updates recent records (within 6 months)
WHEN MATCHED AND (l.InsertedDate >= DATEADD(MM, -6, GETDATE()) 
                 or l.UpdatedDate >= DATEADD(MM, -6, GETDATE())) THEN
```

## Implementation Tasks

### Task 1: Extend PropertyProcessor Class

Add these methods to `property_processor.py`:

```python
def generate_database_ready_output(self, processed_df: pd.DataFrame, region_id: int) -> tuple:
    """
    Generate database-ready DataFrames for SQL MERGE operations.
    
    CRITICAL: Must use same unique identifiers as existing SQL system.
    
    Returns:
        tuple: (owners_df, locations_df) ready for SQL consumption
    """
    # Your implementation here
    pass

def _create_owner_slugs(self, df: pd.DataFrame) -> pd.Series:
    """
    Create OwnerSlug using EXACT same formula as SQL.
    
    CRITICAL: Must match SQL formula exactly for monthly updates to work.
    SQL Formula: street_cleaned + '_' + zip_cleaned (both uppercased, punctuation removed)
    """
    # Your implementation here
    pass
```

**Required Output Format:**

```python
# owners_df must contain:
owners_df = pd.DataFrame({
    'OwnerSlug': [...],          # EXACT SQL formula replication
    'Name': [...],
    'FirstName': [...], 
    'LastName': [...],
    'Address1': [...],
    'Address2': [...],
    'City': [...],
    'State': [...],
    'Zip': [...],
    'IsBusiness': [...],         # Python computed from PropertyClassifier
    'AlternateName': [...],      # Python computed: LastName + FirstName for individuals
    'DoNotMail': [...],
    'MailingZip4': [...],
    'MailingCounty': [...],
    'AccuzipRequest': [...],     # street_city_state_zip format
    'AccuzipResponse': [...]
})

# locations_df must contain:
locations_df = pd.DataFrame({
    'PrimaryId': [...],              # FROM SOURCE DATA - UNCHANGED
    'RegionId': region_id,           # PARAMETER (e.g., 40)
    'OwnerSlug': [...],              # Links to owners_df
    'ListPriorityId': [...],         # Python computed via PropertyPriorityScorer
    'IsOwnerOccupied': [...],        # Python computed via address comparison
    'IsTrust': [...],                # Python computed via PropertyClassifier
    'IsChurch': [...],               # Python computed via PropertyClassifier  
    'IsBusiness': [...],             # Python computed via PropertyClassifier
    'OwnerGrantorMatch': [...],      # Python computed via grantor comparison
    'Address1': [...],               # From source data
    'City': [...],                   # From source data
    'State': [...],                  # From source data
    'Zip': [...],                    # From source data
    'SellDate': [...],               # From source data
    'SellAmount': [...],             # From source data
    'Grantor': [...],                # From source data
    # ... ALL other location fields from existing system
})
```

### Task 2: Create DatabaseIntegrator Class

Create new file `database_integrator.py`:

```python
class DatabaseIntegrator:
    """Handles the handoff between Python processing and SQL database operations"""
    
    def __init__(self, connection_string: str):
        self.conn_str = connection_string
    
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
        # Your implementation here
        pass
```

### Task 3: Create Streamlined SQL Procedure

Create `ProcessPythonEnhancedData.sql` stored procedure that:

1. **Accepts JSON parameters** from Python DataFrames
2. **Parses JSON** into temp tables using `OPENJSON`  
3. **Executes MERGE operations** for Owner and Location tables
4. **Preserves exact same MERGE logic** as existing system
5. **Updates UploadLog** with processing status

**Key Requirements:**
- Use same MERGE conditions as current system
- Preserve 6-month update window logic
- Handle foreign key resolution (Owner.OwnerId → Location.OwnerId)
- Maintain all existing database constraints

### Task 4: Integration Testing

Create `test_hybrid_system.py`:

```python
def test_owner_slug_generation():
    """Verify OwnerSlug matches SQL formula exactly"""
    # Test various address/zip combinations
    # Compare with known SQL outputs
    pass

def test_monthly_update_compatibility():
    """Verify monthly updates work correctly"""
    # Test with existing records
    # Verify MERGE operations update correctly
    pass

def test_database_ready_output():
    """Verify DataFrames contain all required fields"""
    # Test owners_df and locations_df structure
    # Validate data types and required fields
    pass
```

### Task 5: Enhanced Monthly Processing Integration

Update `monthly_processing_v2.py` to include database integration:

```python
def process_region_with_database(region_key: str, config_manager: MultiRegionConfigManager, 
                               connection_string: str) -> Dict:
    """
    Enhanced region processing with database integration.
    
    Processes region data AND inserts into SQL database using hybrid architecture.
    """
    # Your implementation here
    pass
```

## Critical Implementation Notes

### OwnerSlug Formula Replication
The most critical part is replicating the SQL OwnerSlug formula EXACTLY:

```python
def _create_owner_slugs(self, df: pd.DataFrame) -> pd.Series:
    """
    SQL Formula: 
    dbo.ProperCase(UPPER(REPLACE(REPLACE(street, '.', ''), ',',''))+'_'+UPPER(REPLACE(REPLACE(zip, '.', ''), ',','')))
    
    Python equivalent must produce identical results.
    """
    street_clean = (df['OwnerAddress1']
                   .fillna('')
                   .astype(str)
                   .str.upper()                           # UPPER()
                   .str.replace('.', '', regex=False)     # REPLACE('.', '')
                   .str.replace(',', '', regex=False)     # REPLACE(',', '')
                   .str.strip()                           # RTRIM(LTRIM())
                   .str.replace(r'\s+', '_', regex=True)) # Convert spaces to underscores
    
    zip_clean = (df['OwnerZip']
                .fillna('')
                .astype(str)
                .str.upper()
                .str.replace('.', '', regex=False)
                .str.replace(',', '', regex=False)
                .str.strip())
    
    # dbo.ProperCase() converts to Title Case
    owner_slugs = (street_clean + '_' + zip_clean).str.title()
    
    return owner_slugs
```

### Testing Strategy

1. **Unit Testing**: Test OwnerSlug generation with known inputs/outputs
2. **Integration Testing**: Test Python → SQL handoff with small dataset  
3. **Compatibility Testing**: Verify monthly updates work with existing data
4. **Performance Testing**: Compare processing times vs current system

## Success Criteria

✅ **Database Compatibility**: Monthly updates work exactly as before  
✅ **Business Logic Migration**: All property classification and priority scoring moved to Python  
✅ **Performance**: Processing time equal or better than current system  
✅ **Data Quality**: Same or better data quality as current system  
✅ **Maintainability**: Business logic in readable, testable Python code  

## Risk Mitigation

- **Side-by-side deployment** for validation
- **Comprehensive testing** before production migration  
- **Rollback plan** with existing stored procedure as backup
- **Data validation** comparing Python vs SQL results

## Error Handling

Implement robust error handling for:
- Database connection failures
- JSON parsing errors in SQL
- Data validation failures
- MERGE operation conflicts
- Large dataset memory management

## Logging and Monitoring

Add comprehensive logging for:
- Python processing stages and timing
- Database integration success/failure
- Data quality metrics and validation results
- Performance benchmarks vs current system

Your implementation should be production-ready, thoroughly tested, and maintain backward compatibility with the existing monthly update process.

## Files to Create/Modify

### New Files:
- `database_integrator.py` - Python-SQL integration layer
- `ProcessPythonEnhancedData.sql` - Streamlined stored procedure
- `test_hybrid_system.py` - Comprehensive test suite

### Modified Files:
- `property_processor.py` - Add database output methods
- `monthly_processing_v2.py` - Add database integration option

Follow the detailed specifications in `PYTHON_SQL_HYBRID_MIGRATION_GUIDE.md` for exact implementation requirements.