USE [QuickFixRealEstate-REAL]
GO
/****** Object:  StoredProcedure [dbo].[ProcessUploadLog_Newtest]    Script Date: 9/3/2025 12:24:49 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--ProcessUploadLog_Newtest 1162                 
ALTER PROCEDURE [dbo].[ProcessUploadLog_Newtest] (@uploadLogId INT)                              
AS                              
BEGIN     
  DECLARE @regionId INT = (SELECT RegionId FROM UploadLog WHERE UploadLogId = @uploadLogId)                              
  DECLARE @dSql NVARCHAR(MAX) = ''                              
  DECLARE @dSql2 NVARCHAR(MAX) = ''                              
  DECLARE @dSqlTable NVARCHAR(MAX) = ''                             
  DECLARE @dSqlInsert NVARCHAR(MAX) = ''                              
  DECLARE @tableName NVARCHAR(16) = 'CommonRegion' + CONVERT(NVARCHAR, @regionId)                              
  --DECLARE @tableName NVARCHAR(16) = 'cheturegion'                              
  --SET @dSql = 'UPDATE UploadLog SET RowsProcessed = 0, ProcessingEnded = NULL, ProcessingStarted = GETDATE() WHERE UploadLogId = ' + CONVERT(NVARCHAR, @uploadLogId) EXEC(@dSql)                              
  Print 'dSql executed - ' + CONVERT( VARCHAR(24), GETDATE(), 121)                              
                              
  -- This part gets the master field mapping                              
                    
  DECLARE @regionFields TABLE                              
  (                              
   RowId INT IDENTITY PRIMARY KEY NOT NULL,                              
   RegionId INT NOT NULL,                              
   FieldName NVARCHAR(64) NOT NULL,                              
   MasterTableName NVARCHAR(64) NOT NULL,                              
   MasterFieldName NVARCHAR(64) NOT NULL,                              
   SqlDataType NVARCHAR(64) NOT NULL,                              
   SqlColName NVARCHAR(128) NOT NULL,                              
   SqlTryParse nvarchar(255) NOT NULL                              
  )                              
  INSERT INTO @regionFields                              
   SELECT                               
    rf.RegionId, rf.FieldName, mf.TableName, mf.[Name], mf.SqlType,                              
    mf.TableName + mf.[Name],                              
    'TRY_CONVERT(' + mf.SqlType + ', [' + rf.FieldName + ']) AS [' + mf.TableName + mf.[Name] + ']'                              
   FROM RegionFields rf                              
    LEFT OUTER JOIN MasterFields mf ON rf.MasterFieldId = mf.MasterFieldId                              
   WHERE (40 = RegionId) AND (NOT(rf.MasterFieldId IS NULL))                   
     --and  'TRY_CONVERT(' + mf.SqlType + ', [' + rf.FieldName + ']) AS [' + mf.TableName + mf.[Name] + ']'  like '%Region%'                           
   ORDER BY mf.SqlOrder                              
                                 
  --Select * from @regionFields                              
  Print 'Selected region fields - ' + CONVERT( VARCHAR(24), GETDATE(), 121)                              
                                
  DECLARE @row INT = 1                              
  DECLARE @rows INT = (SELECT MAX(RowId) FROM @regionFields)                              
  DECLARE @rowSqlTryParse NVARCHAR(max) = ''                              
  DECLARE @dynamicColumns NVARCHAR(MAX) = ''                              
  DECLARE @MasterColumn NVARCHAR(max) = ''                              
  SET @dSqlInsert = 'SELECT '                              
  Print 'while'                              
  WHILE(@row <= @rows)                              
  BEGIN                              
   SET @MasterColumn = (SELECT SqlColName FROM @regionFields WHERE RowId = @row)                              
   SET @rowSqlTryParse = (SELECT SqlTryParse FROM @regionFields WHERE RowId = @row)                              
   SET @dSqlInsert = @dSqlInsert + ' ' + @rowSqlTryParse                              
   IF(@row <= @rows)                               
   BEGIN                               
   SET @dSqlInsert = @dSqlInsert + ','                               
   SET @dynamicColumns =  @dynamicColumns +  @MasterColumn + ', '                              
   END                          
   SET @row = @row + 1                              
  END                              
  SET @dSqlInsert = @dSqlInsert + 'TRY_CONVERT(nvarchar(255), [Id]) AS [RegionRowId],'                              
  Set @dSqlInsert = @dSqlInsert +convert(varchar(20), @regionId)                              
  SET @dSqlInsert = @dSqlInsert + ' FROM ' + @tableName                              
 SET @dynamicColumns = @dynamicColumns + 'RegionRowId, RegionId'                              
  Print @dSqlInsert                              
  Print @dynamicColumns                              
Delete from Rawlocations                              
  Declare @MasterQuery NVARCHAR(MAX) = 'INSERT INTO [dbo].[Rawlocations] ('+@dynamicColumns+')  ('+@dSqlInsert+')';                         
                                
  print @masterquery                              
 
        Execute (@MasterQuery)     
		
Print 'while @rowslocs completed - ' + CONVERT( VARCHAR(24), GETDATE(), 121)            
            
Create TABLE #toProcess                               
(                     
RowId INT identity PRIMARY KEY NOT NULL,                 
[OwnerOccupied] nvarchar(255),                   
[LocationCounty] nvarchar(255),                    
[DoNotMail] nvarchar(255),                   
[PropertyClass] nvarchar(255),          
[LocationType] nvarchar(255),                 
[Bedrooms] int,                      
[TotalBathrooms] int,                   
[BuildingSqFt] nvarchar(255),                      
[LotSizeSqFt] nvarchar(255),                      
[YearBuilt] nvarchar(255),                      
[EffectiveYearBuilt] nvarchar(255),                      
[PoolType] nvarchar(255),                      
[Vacant] nvarchar(255),                      
[HOAPresent] nvarchar(255),                      
[NumberOfStories] DECIMAL(18, 2),                      
[LastCashBuyer] nvarchar(255),                      
[PriorSaleDate] datetime,                      
[PriorSaleAmount]  DECIMAL(18, 2),                      
[PriorSaleCashBuyer] nvarchar(255),                  
[PriorSaleCashBuyerName1] nvarchar(255),                      
[PriorSaleCashBuyerName2] nvarchar(255),                      
[OpenLoan1Date] datetime,                      
[OpenLoan1Balance]  DECIMAL(18, 2),                      
[OpenLoan1Type] nvarchar(255),                      
[OpenLoan1Lender] nvarchar(255),                        
[TotalOpenLoans] nvarchar(255),                      
[EstRemainingBalanceOfOpenLoans] nvarchar(255),                  
[EstValue] nvarchar(255),                      
[EstLoanToValue] decimal(18,2),                      
[EstEquity] nvarchar(255),                      
[MonthlyRent] nvarchar(255),                    
[GrossYieldPercentage] DECIMAL(18, 2),                   
[LienType] nvarchar(255),                      
[LienDate] datetime,                      
[LienAmount]  DECIMAL(18, 2),                      
[BKDate] datetime,                      
[DivorceDate] datetime,                      
[PreFcRecordingDate] datetime,                   
[PreFcRecordType] nvarchar(255),                      
[PreFcUnpaidBalance] nvarchar(255),                      
[PreFcDefaultAmount] DECIMAL(18, 2),                      
[PreFcAuctionDate] datetime,                    
[PreFcAuctionTime] nvarchar(255),                      
[PreFcTrusteeAttorneyName] nvarchar(255),                      
[PreFcTrusteeRefNumber] nvarchar(255),                   
[PreFcTrusteeAttorneyAddress] nvarchar(255),                      
[PreFcBorrower1Name] nvarchar(255),                      
[DateAddedToList] datetime,                      
[MethodToAdd] nvarchar(255),                  
--------------------------------------------                  
 [LocationPrimaryId] nvarchar(255),                            
 [LocationNeighborhood] nvarchar(255),                               
 [LocationAddress1] nvarchar(255),                                
 [LocationCity] nvarchar(255),                               
 [LocationState] nvarchar(255),                               
 [LocationZip] nvarchar(255),                        
 [LocationResponse] nvarchar(255),              
 [OwnerName] nvarchar(255),                 
 [OwnerFirstName] nvarchar(255),            
 [OwnerLastName] nvarchar(255),                               
 [OwnerAddress1] nvarchar(255),                               
 [OwnerAddress2] nvarchar(255),                          
 [OwnerAddressResponse] VARCHAR(500),                     
 [OwnerCity] nvarchar(255),                               
 [OwnerState] nvarchar(255),                               
 [OwnerZip] nvarchar(255),                     
 [LocationAcres] DECIMAL(18, 4),                                  
 [LocationZoneDesc] nvarchar(255),                           
 [LocationLandValue] DECIMAL(18, 2),                               
 [LocationPrevLandValue] DECIMAL(18, 2),                               
 [LocationDwellingValue] DECIMAL(18, 2),                    
 [LocationPrevDwellingValue] DECIMAL(18, 2),           
 [LocationTotalValue] DECIMAL(18, 2),                               
 [LocationPrevTotalValue] DECIMAL(18, 2),                               
 [LocationSellDate] DATETIME,                   
 [LocationPrevSellDate] DATETIME,                               
 [LocationSellAmount] DECIMAL(18, 2),                               
 [LocationPrevSellAmount] DECIMAL(18, 2),                  
 [LocationGrantor] nvarchar(255),                               
 [LocationPrevGrantor] nvarchar(255),                               
 [LocationDocNum] nvarchar(255),                               
 [LocationPrevDocNum] nvarchar(255),                               
 [LocationSquareFootage] DECIMAL(18, 2),                    
 [LocationTopography] nvarchar(255),                              
 LocationIsChurch BIT NOT NULL DEFAULT 0,                              
 LocationIsBusiness BIT NOT NULL DEFAULT 0,                              
 LocationIsTrust BIT NOT NULL DEFAULT 0,                    
 OwnerSlug NVARCHAR(MAX) NOT NULL,        
 OwnerId INT NOT NULL DEFAULT 0 ,                   
 [RequestStreet] nvarchar(255),                         
 [RequestCity] nvarchar(255),                      
 [RequestState] nvarchar(255),                        
 [RequestZip] nvarchar(255),                        
 [FullAccuzipResponse] NVARCHAR(max),                  
 [OwnerMailingZip4] nvarchar(50),      
 [ResponseNCOA] varchar(50) 
)                              
INSERT INTO #toProcess                              
 SELECT                      
        
Max(rl.LocationOwnerOccupied) AS OwnerOccupied,                 
Max(rl.LocationCounty) AS MailingCounty,                      
Max(rl.OwnerDoNotMail) AS DoNotMail,                      
Max(rl.LocationPropertyClass) AS PropertyClass,         
Max(rl.LocationLocationType) LocationLocationType,                    
Max(rl.LocationBedrooms) AS Bedrooms,                      
Max(rl.LocationTotalBathrooms) AS TotalBathrooms,                      
Max(rl.LocationBuildingSqFt) AS BuildingSqFt,                      
Max(rl.LocationLotSizeSqFt) AS LotSizeSqFt,                      
Max(rl.LocationYearBuilt) AS YearBuilt,                      
Max(rl.LocationEffectiveYearBuilt) AS EffectiveYearBuilt,                      
Max(rl.LocationPoolType) AS PoolType,                      
Max(rl.LocationVacant) AS Vacant,                      
Max(rl.LocationHOAPresent) AS HOAPresent,                      
--Max(rl.LocationNumberOfStories) AS NumberOfStories,          
isnull(ROUND(cast(MAX(rl.LocationNumberOfStories) as decimal(18,2)),2),0) AS NumberOfStories  ,                     
Max(rl.LocationLastCashBuyer) AS LastCashBuyer,                      
--MAX(case when rl.LocationPriorSaleDate='1900-01-01' then null else rl.LocationPriorSaleDate end) AS PriorSaleDate  ,          
MAX(rl.LocationPriorSaleDate) AS PriorSaleDate  ,                    
isnull(ROUND(cast(MAX(rl.LocationPriorSaleAmount) as decimal(18,2)),2),0) AS PriorSaleAmount  ,                           
Max(rl.LocationPriorSaleCashBuyer) AS PriorSaleCashBuyer,                      
Max(rl.LocationPriorSaleBuyerName1) AS PriorSaleCashBuyerName1,                      
Max(rl.LocationPriorSaleBuyerName2) AS PriorSaleCashBuyerName2,                      
MAX(case when rl.LocationOpenLoan1Date='1900-01-01' then null else rl.LocationOpenLoan1Date end) AS OpenLoan1Date,                              
ROUND(cast(MAX(rl.LocationOpenLoan1Balance) as decimal(18,2)),2) AS OpenLoan1Balance,                              
Max(rl.LocationOpenLoan1Type) AS OpenLoan1Type,                      
Max(rl.LocationOpenLoan1Lender) AS OpenLoan1Lender,                      
Max(rl.LocationTotalOpenLoans) AS TotalOpenLoans,                      
Max(rl.RemainingBalanceOfOpenLoans) AS EstRemainingBalanceOfOpenLoans,                      
Max(rl.LocationEstValue) AS EstValue,        
ROUND(cast(MAX(rl.LocationEstLoanToValue) as decimal(18,2)),2) AS EstLoanToValue,         
Max(rl.LocationEstEquity) AS EstEquity,                      
Max(rl.LocationMonthlyRent) AS MonthlyRent,                      
--Max(rl.LocationGrossYield) AS GrossYieldPercentage,         
ROUND(cast(MAX(rl.LocationGrossYield) as decimal(18,2)),2) AS GrossYieldPercentage,                      
Max(rl.LocationLienType) AS LienType,                      
max(case WHEN rl.LocationLienDate = '1900-01-01' THEN NULL ELSE rl.LocationLienDate END) as LienDate,                          
ROUND(cast(MAX(rl.LocationLienAmount) as decimal(18,2)),2) AS LienAmount,                              
 MAX(case when rl.LocationBKDate='1900-01-01' then null else rl.LocationBKDate end) AS BKDate,                              
 MAX(case when rl.LocationDivorceDate='1900-01-01' then null else rl.LocationDivorceDate end) AS DivorceDate,                              
 MAX(case when rl.LocationPreFcRecordingDate='1900-01-01' then null else rl.LocationPreFcRecordingDate end) AS PreFcRecordingDate,                              
MAX(rl.LocationPreFcRecordType) AS PreFcRecordType,                      
MAX(rl.LocationPreFcUnpaidBalance) AS PreFcUnpaidBalance,                      
 ROUND(cast(MAX(rl.LocationPreFcDefaultAmount) as decimal(18,2)),2) AS PreFcDefaultAmount,                              
MAX(case when rl.LocationPreFcAuctionDate='1900-01-01' then null else rl.LocationPreFcAuctionDate end) AS PreFcAuctionDate,                              
MAX(rl.LocationPreFcAuctionTime) AS PreFcAuctionTime,                      
MAX(rl.LocationPreFcTrusteeAttorneyName) AS PreFcTrusteeAttorneyName,                      
MAX(rl.LocationPreFcTrusteeRefNumber) AS PreFcTrusteeRefNumber,                      
MAX(rl.LocationPreFcTrusteeAttorneyAddress) AS PreFcTrusteeAttorneyAddress,                      
MAX(rl.LocationPreFcBorrower1Name) AS PreFcBorrower1Name,                      
MAX(case when rl.LocationDateAddedToList='1900-01-01' then null else rl.LocationDateAddedToList end) AS DateAddedToList,                             
MAX(rl.LocationMethodofAdd) AS MethodToAdd,                
                 
------------------------------------------------------------------------------------                             
MAX(rl.LocationPrimaryId) AS LocationPrimaryId,                             
MAX(rl.LocationNeighborhood) AS LocationNeighborhood,                  
MAX(sl.StreetResponse) AS LocationAddress1,                              
MAX(sl.CityResponse) As LocationCity,                              
MAX(sl.StateResponse) As LocationState,                              
MAX(sl.ZipcodeResponse) As LocationZip,                              
MAX(sl.response) As LocationResponse,           
dbo.ProperCase(Max(rl.OwnerLastName)+' '+Max(rl.OwnerName))OwnerName,          
MAX(rl.OwnerName) AS OwnerFirstName,                             
MAX(rl.OwnerLastName) AS OwnerLastName,             
MAX(ss.Street) AS OwnerAddress1,                              
MAX(ss.Street2) AS OwnerAddress2,               
MAX(ss.response) As OwnerResponse,                              
 dbo.ProperCase(UPPER(MAX(ss.City))) AS OwnerCity,                              
 UPPER(MAX(ss.State)) OwnerState,                              
 MAX(ss.Zipcode) AS OwnerZip,                              
 ROUND(cast(MAX(rl.LocationAcres) as decimal(18,4)),2) AS LocationAcres,            
 MAX(rl.LocationZoneDesc) AS LocationZoneDesc,                              
 ROUND(cast(MAX(rl.LocationLandValue) as decimal(18,2)),2) AS LocationLandValue,                              
 ROUND(cast(MAX(rl.LocationPrevLandValue) as decimal(18,2)),2) AS LocationPrevLandValue,                              
 ROUND(cast(MAX(REPLACE(rl.LocationDwellingValue,',',''))  as decimal(18,2)),2) AS LocationDwellingValue,                              
 ROUND(cast(MAX(rl.LocationPrevDwellingValue) as decimal(18,2)),2) AS LocationPrevDwellingValue,                              
 ROUND(cast(MAX(rl.LocationTotalValue) as decimal(18,2)),2) AS LocationTotalValue,                              
 ROUND(cast(MAX(rl.LocationPrevTotalValue) as decimal(18,2)),2) AS LocationPrevTotalValue,                              
 --MAX(case when rl.LocationSellDate is null then '1900/01/01' else rl.LocationSellDate end) AS LocationSellDate,           
 MAX(rl.LocationSellDate) AS LocationSellDate,                    
--isnull(ROUND(cast(MAX(rl.LocationSellDate) as decimal(18,2)),2),0) AS LocationSellDate  ,          
 MAX(case when rl.LocationPrevSellDate='' then null else rl.LocationPrevSellDate end) AS LocationPrevSellDate,                              
 ROUND(cast(MAX(ISNULL(rl.LocationSellAmount,0))as decimal(18,2)),2) AS LocationSellAmount,                              
 MAX(rl.LocationPrevSellAmount) AS LocationPrevSellAmount,                              
 MAX(rl.LocationGrantor) AS LocationGrantor,                             
 MAX(rl.LocationPrevGrantor) AS LocationPrevGrantor,                              
 MAX(rl.LocationDocNum) AS LocationDocNum,                              
 MAX(rl.LocationPrevDocNum) AS LocationPrevDocNum,                              
 ROUND(cast(MAX(rl.LocationSquareFootage)as decimal(18,2)),2) AS LocationSquareFootage,                              
 MAX(rl.LocationTopography) AS LocationTopography,                              
 0,                   
 0,                  
 0,                   
 dbo.ProperCase(UPPER(REPLACE(REPLACE(MAX(RTRIM(LTRIM(ss.Street))), '.', ''), ',',''))+'_'+UPPER(REPLACE(REPLACE(MAX(RTRIM(LTRIM(ss.ZipCode))), '.', ''), ',',''))) as OwnerSlug,                        
 0,                  
 MAX(ss.RequestStreet) as RequestStreet,                  
 MAX(ss.RequestCity) as RequestCity,                  
 MAX(ss.RequestState) as RequestState,                  
 MAX(ss.RequestZip) as RequestZip,                  
 MAX(ss.FullAccuzipResponse) as FullAccuzipResponse,              
 Max(rl.OwnerMailingZip4) as OwnerMailingZip4 ,      
 Max(ss.NCOA) as ResponseNCOA

  FROM Rawlocations rl                  
   left join TempOwnerListForAccuzip ss                   
   on  rl.RegionRowId = ss.RegionAddressId AND rl.RegionId = ss.RegionId                                
  --left join (SELECT * FROM (SELECT RegionAddressId, RegionId, street, city, response,                                
  --StreetResponse = CASE WHEN StreetResponse = '' THEN Street ELSE StreetResponse END, CityResponse, StateResponse, ZipCodeResponse,  ROW_NUMBER()                               
  --    over(partition by RegionAddressId  ORDER BY CASE WHEN response = 'Match-Mailable' THEN 0 ELSE 1 END,                              
  --          CASE WHEN response = 'Match-Vacant' THEN 0 ELSE 1 END,                               
  --          CASE WHEN response = 'Match-Inactive' THEN 0 ELSE 1 END,                              
  --          CASE WHEN response = 'No Match' THEN 0 ELSE 1 END) AS rownumber      FROM Rawlocations rl left join TempOwnerListForAccuzip ss on  rl.RegionRowId = ss.RegionAddressId AND rl.RegionId = ss.RegionId                              
  left join (SELECT * FROM (SELECT RegionAddressId, RegionId, street, city, response,                               
  StreetResponse = CASE WHEN StreetResponse = '' THEN Street ELSE StreetResponse END, CityResponse, StateResponse, ZipCodeResponse,  ROW_NUMBER()                               
  over(partition by RegionAddressId  ORDER BY CASE WHEN response = 'Matched' THEN 0 ELSE 1 END,                              
        CASE WHEN response = 'Vacant' THEN 0 ELSE 1 END,                    
            CASE WHEN response = 'Missing Information- Invalid Ste or #' THEN 0 ELSE 1 END,                              
              CASE WHEN response = 'Address not found in DPV Database' THEN 0 ELSE 1 END,         
      CASE WHEN response = 'Street not found' THEN 0 ELSE 1 END,        
      CASE WHEN response = 'Address not found' THEN 0 ELSE 1 END,         
         CASE WHEN response = 'City not found' THEN 0 ELSE 1 END,                              
 CASE WHEN response = 'Invalid Address' THEN 0 ELSE 1 END                              
            ) AS rownumber       
 FROM SmartyStreetLocationAddress where RegionId = @regionId) T                              
  WHERE T.rownumber < 2) sl  ON rl.RegionRowId = sl.RegionAddressId AND rl.RegionId = sl.RegionId             
                                    
 WHERE                               
  NOT(sl.StreetResponse IS NULL)         --Location address must have a value                              
  AND LEN(sl.StreetResponse) >= 1               --Location address must have length >= 1                              
  AND ISNUMERIC(LEFT(sl.StreetResponse, 1)) = 1           --First char of location address must be a digit                              
  AND LEFT(sl.StreetResponse, 1) != '0'             --First char of location must not be '0'                              
  AND NOT(ss.Street IS NULL)               --Owner address must have a value                              
  AND LEN(ss.Street) >= 1                --Owner address must have a length >= 1                              
  AND (ISNUMERIC(LEFT(ss.Street, 1)) = 1 OR LOWER(LEFT(Replace(LEFT(OwnerAddress1, 5), ' ',''), 2)) = 'po')  --First char of owner address must be a digit --Sometimes owner address starts with "PO"                              
  AND LEFT(ss.Street, 1) != '0'              --First char of owner address must not be '0'                              
  AND NOT(rl.LocationLocationType LIKE '%vacant%')           --Location Type must not contain the value 'vacant'                              
  AND rl.LocationDwellingValue > 0                --LocationDwellingValue must have value                        
  --AND NOT(ss.ZipCode IS NULL)     --Zip must have value                        
  --AND NOT ss.ZipCode = ''      --Zip should not be vlank or null                        
                        
 GROUP BY rl.LocationPrimaryId ,RowId                 
 ORDER BY rl.LocationPrimaryId  ,RowId                       
                                  
 --update #toProcess set LocationCity = '', LocationState = '', LocationZip = ''                               
 --where LTRIM(RTRIM(LocationResponse)) IN ('No Match', 'No Match - PO Box Only')                         
                         
    --DECLARE @temptable22 XML = (SELECT * FROM TempOwnerListForAccuzip FOR XML AUTO);                        
    --DECLARE @temptable XML = (SELECT * FROM #toProcess FOR XML AUTO);                        
    --DECLARE @temptable12 XML = (SELECT * FROM SmartyStreetLocationAddress FOR XML AUTO);                        
                        
--Insert into ToProcessTemp            
     
  Update #toProcess set LocationCity='', LocationZip='', LocationState= '' where                              
    LocationResponse IN ('Address Not Found','Address not found in DPV Database','Street not found','City not found', 'Invalid Address', 'Multiple Response')   
	
                              
Print 'insert into #toProcess completed - ' + CONVERT( VARCHAR(24), GETDATE(), 121)                               
Update UploadLog set ProcessingState = 6 where UploadLogId = @uploadLogId;                               
                                
UPDATE #toProcess SET LocationIsTrust = 1 WHERE                               
(OwnerName LIKE '%trus%' OR OwnerName LIKE '%estate%' OR OwnerName LIKE '%decl%' OR                               
OwnerName LIKE '%supplemental%' OR OwnerName LIKE '%living%' OR OwnerName LIKE '%amend%' OR                               
OwnerName LIKE '%life%' OR OwnerName LIKE '%TRS%' OR OwnerName LIKE '%execut%' OR OwnerName                               
LIKE '%revoc%' OR OwnerName LIKE '%irrev%') 
                              
                              
UPDATE #toProcess SET LocationIsChurch = 1 WHERE                               
(OwnerName LIKE '%church%' OR OwnerName LIKE '%evangel%' OR OwnerName LIKE '%presbyterian%'                               
OR OwnerName LIKE '%bible%' OR OwnerName LIKE '%episcopal%' OR OwnerName LIKE '%dioce%' OR                               
OwnerName LIKE '%protestant%' OR OwnerName LIKE '%trinity%' OR OwnerName LIKE '%holy%' OR                               
OwnerName LIKE '%jerusalum%' OR OwnerName LIKE '%baptist%' OR OwnerName LIKE '%lutheran%' OR                               
OwnerName LIKE '%nazar%' OR OwnerName LIKE '% god %' OR OwnerName LIKE '%convenant%' OR                               
OwnerName LIKE '%ministry%' OR OwnerName LIKE '% christ %' OR                              
 --Ends with                              
OwnerName LIKE '% christ' OR OwnerName LIKE '% god')                        
                              
UPDATE #toProcess SET LocationIsBusiness = 1 WHERE LocationIsChurch = 0                              
 AND (                               
  --Contains                              
  OwnerName LIKE '%roanoke%' OR OwnerName LIKE '%llc%' OR OwnerName LIKE '%housing%' OR                               
  OwnerName LIKE '%develop%' OR OwnerName LIKE '%author%' OR OwnerName LIKE '%planning%' OR                               
  OwnerName LIKE '%district%' OR OwnerName LIKE '%commiss%' OR OwnerName LIKE '%partner%' OR                               
  OwnerName LIKE '%group%' OR OwnerName LIKE '%condo%' OR OwnerName LIKE '%city%' OR                               
  OwnerName LIKE '%real%' OR OwnerName LIKE '%holding%' OR OwnerName LIKE '%company%' OR                               
  OwnerName LIKE '% inc %' OR OwnerName LIKE '% co %' OR OwnerName LIKE '% tc %' OR                               
  OwnerName LIKE '% bank %' OR OwnerName LIKE '%proprietor%' OR OwnerName LIKE '%propert%' OR                               
  OwnerName LIKE '%foundation%' OR OwnerName LIKE '%commonwealth%' OR OwnerName LIKE '%clinic%' OR                               
  OwnerName LIKE '% office%' OR OwnerName LIKE '%limit%' OR OwnerName LIKE '% ltd%' OR                               
  OwnerName LIKE '% health%' OR OwnerName LIKE '% llp%' OR OwnerName LIKE '% assoc%' OR                               
  OwnerName LIKE '% corp%' OR                               
  --Ends with                              
  OwnerName LIKE '% lc' OR OwnerName LIKE '% inc' OR OwnerName LIKE '% co' OR OwnerName LIKE '% tc' OR                               
  OwnerName LIKE '% bank' OR OwnerName LIKE '% ltd' OR OwnerName LIKE '% llp' OR 
  OwnerName LIKE '%Virginia%' OR OwnerName LIKE '%North Carolina%' OR OwnerName LIKE '%Enterprises%'
  OR OwnerName LIKE '%Attorney%' OR OwnerName LIKE '%Credit Union%' OR OwnerName LIKE '%Incorporated%'
  OR OwnerName LIKE '%Medical%' OR OwnerName LIKE '%Center%' OR
  --Trust logic                              
  (LocationIsTrust = 1 AND (OwnerName LIKE '% the %' OR OwnerName LIKE '% the' OR OwnerName LIKE 'the %'))                              
 )                              
                              
                              
Print 'Updated Is trust, church in #toProcess complete - ' + CONVERT( VARCHAR(24), GETDATE(), 121);                              
                              
                              
                              
                              
DECLARE @skipProcessCount INT = ((SELECT Count(RowId) FROM Rawlocations) - (SELECT Count(RowId) FROM #toProcess))                              
SET @dSql = 'UPDATE UploadLog SET RowsProcessed = ' + CONVERT(NVARCHAR, @skipProcessCount + 1) + ' WHERE UploadLogId = ' + CONVERT(NVARCHAR, @uploadLogId) EXEC(@dSql)                              
                              
                              
Print 'going to execute next- Insert owner and location while loop - ' + CONVERT( VARCHAR(24), GETDATE(), 121)                              
-- *********** Insert New Owners ******************                              
Update UploadLog set ProcessingState = 7 where UploadLogId = @uploadLogId;                     
;With cte as (                              
SELECT  *,ROW_NUMBER() OVER (PARTITION BY LocationPrimaryId ORDER BY CASE WHEN LocationAddress1 = '' THEN 1 ELSE 0 END,LocationSellDate ASC) AS Duplicate                              
 FROM #toProcess  WHERE (LocationAddress1 != '' OR LocationSellDate !='' OR LocationSellDate IS NOT NULL                               
  OR LocationAddress1 IS NOT NULL) )  
 Delete from cte where Duplicate > 1                              
                              
                          
    declare @T table                              
    (                              
      rowId int,                              
      OwnerId int,                              
      OwnerSlug nvarchar(255)                              
    );                              
       --  declare condition int=0                        
      -- select LocationIsBusiness from #toProcess                        
   --update                         
    MERGE OWNER AS o                              
    USING (                              
       SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY OwnerSlug                              
        ORDER BY OwnerSlug DESC) AS Duplicate FROM #toProcess) s where Duplicate=1                          
       ) as S                              
    on s.ownerSlug = o.Slug                         
    WHEN MATCHED THEN                              
      Update SET [Name] = s.OwnerName,          
   FirstName=s.OwnerFirstName,        
    LastName=s.OwnerLastName,                      
    AlternateName =case when s.LocationIsBusiness = 0  then  dbo.LastWord(s.OwnerName) + ' ' + dbo.FirstWord(s.OwnerName)         
  else  s.OwnerName end,        
        Address1 = s.OwnerAddress1,                           
     Address2 = s.OwnerAddress2,                               
        Response = s.OwnerAddressResponse,                              
        City = s.OwnerCity,                               
        [State]= s.OwnerState,                              
        Zip = s.OwnerZip,                              
        Slug = isnull(s.OwnerSlug,0),  --added isnull because of null error but it should not be null                      
  AccuzipRequest =s.RequestStreet+'_'+s.RequestCity+'_'+s.RequestState+'_'+s.RequestZip,                     
  AccuzipResponse=s.FullAccuzipResponse,                        
  ModifiedDate=GetDate()              
  ,UpdatedDate=GetDate() ,              
  MailingZip4=s.OwnerMailingZip4,              
  MailingCounty=s.LocationCounty,              
 DoNotMail=s.DoNotMail              
                   
    WHEN NOT MATCHED THEN                              
    INSERT ([Name],FirstName,LastName, Address1, Address2, Response, City, [State], Zip, Slug,AccuzipRequest,AccuzipResponse,InsertedDate,MailingZip4,MailingCounty,DoNotMail) VALUES                               
      (s.OwnerName,s.OwnerFirstName,s.OwnerLastName, s.OwnerAddress1, s.OwnerAddress2, s.OwnerAddressResponse, s.OwnerCity, s.OwnerState, s.OwnerZip, s.OwnerSlug,              
   s.RequestStreet+'_'+s.RequestCity+'_'+s.RequestState+'_'+s.RequestZip,s.FullAccuzipResponse,getdate(),s.OwnerMailingZip4,s.LocationCounty,s.DoNotMail)      
    OUTPUT S.rowId, inserted.OwnerId, S.OwnerSlug INTO @T;                              
                        
   --DECLARE @temptable1234 XML = (SELECT * FROM @T FOR XML AUTO);                        
                        
    Print 'Update owner using merge  in toprocess' + CONVERT( VARCHAR(24), GETDATE(), 121)                               
    UPDATE s           
    set s.OwnerId = T.OwnerId                              
    from @T as T, #toProcess s                              
    where T.rowId = s.RowId;                              
                              
    update t1 set t1.OwnerId=t2.OwnerId from #toProcess t1                              
    inner join (select OwnerId, OwnerSlug from #toProcess where OwnerId != 0 ) t2  on t1.OwnerSlug=t2.OwnerSlug                              
                           
                          
                                
 insert into uploaddetail(UploadlogId,NewPrimaryIdInserted,createddate)                        
 SELECT @uploadLogId  as logid,COUNT(distinct LocationPrimaryId),GETDATE() FROM                               
     #toProcess                          
 where LocationPrimaryId not in                        
 (                        
 Select primaryid from Location                        
 )                           
 and  OwnerId <> 0                        
                        
 declare @@updatedprimaryid int;                        
 SELECT @@updatedprimaryid=COUNT(distinct LocationPrimaryId) FROM                               
     #toProcess                          
 where LocationPrimaryId  in                        
 (                        
 Select primaryid from Location                        
 )                           
 and  OwnerId <> 0   
 
 update UploadDetail set PrimaryIdUpdated=@@updatedprimaryid                        
 where UploadlogId=@uploadLogId                        
                          
    --DECLARE @temptable123 XML = (SELECT * FROM @T FOR XML AUTO);                        
 --Insert into ToProcessTemp select * from #toProcess                        
                        
                        
  Print 'Inserted owner using merge ' + CONVERT( VARCHAR(24), GETDATE(), 121)                               
-- ********************Inserting New Location ****************************                              
 Update UploadLog set ProcessingState = 8 where UploadLogId = @uploadLogId;                              
  Print 'Start inserting Location  using merge ' + CONVERT( VARCHAR(24), GETDATE(), 121)                               
  MERGE Location AS l                              
   USING (                              
    SELECT * FROM (                              
                SELECT *, ROW_NUMBER() OVER (PARTITION BY  LocationPrimaryId ORDER BY LocationPrimaryId DESC) AS Duplicate                              
    FROM #toProcess  WHERE OwnerId <> 0 ) s where Duplicate=1) as S                              
    ON S.LocationPrimaryId = l.PrimaryId AND l.RegionId = @regionId                              
    WHEN MATCHED 
	AND (l.InsertedDate >= DATEADD(MM, -6, GETDATE()) or l.UpdatedDate >= DATEADD(MM, -6, GETDATE()) )
	THEN                              
         UPDATE SET                 
   RegionId = @regionId, OwnerId = S.OwnerId,                 
    SellDate = s.LocationSellDate, SellAmount = s.LocationSellAmount,                              
        Address1 = S.LocationAddress1, City = S.LocationCity, State = S.LocationState,                
   Zip = S.LocationZip  ,Response = S.LocationResponse, Neighborhood = S.LocationNeighborhood,                
    Acres = S.LocationAcres, ZoneDesc = S.LocationZoneDesc,                      
         LandValue = S.LocationLandValue,    PrevLandValue = S.LocationPrevLandValue,                
    DwellingValue = S.LocationDwellingValue, PrevDwellingValue = S.LocationPrevDwellingValue,                              
        TotalValue = S.LocationTotalValue, PrevTotalValue = S.LocationPrevTotalValue,                
   PrevSellDate = S.LocationPrevSellDate, PrevSellAmount = S.LocationPrevSellAmount,                              
        Grantor = S.LocationGrantor, PrevGrantor = S.LocationPrevGrantor, DocNum = S.LocationDocNum,                
   PrevDocNum = S.LocationPrevDocNum,  SquareFootage = S.LocationSquareFootage,                 
   Topography = S.LocationTopography,                           
        IsBusiness = S.LocationIsBusiness, IsChurch = S.LocationIsChurch, IsTrust = S.LocationIsTrust ,                        
  AccuzipLocationRequest=s.LocationAddress1+'_'+s.LocationCity+'_'+s.LocationState+'_'+s.LocationZip,                        
  AccuzipLocationResponse=s.LocationAddress1+'_'+s.LocationCity+'_'+s.LocationState+'_'+s.LocationZip,                        
  ModifiedDate=GetDate(),LocationOwnerName =s.OwnerName,ListPriorityId=11                  
  ,UpdatedDate=getdate(),                  
   BuildingSqft=s.BuildingSqft,LastCashBuyer=s.LastCashBuyer, PriorSaleDate=S.PriorSaleDate, PriorSaleAmount=S.PriorSaleAmount,                 
  PriorSaleCashBuyer=S.PriorSaleCashBuyer, PriorSaleBuyerName1=S.PriorSalecashBuyerName1, PriorSaleBuyerName2=S.PriorSalecashBuyerName2,                 
OpenLoan1Date=s.OpenLoan1Date, OpenLoan1Balance=S.OpenLoan1Balance, OpenLoan1Type=s.OpenLoan1Type, OpenLoan1Lender=s.OpenLoan1Lender                
, TotalOpenLoans=s.TotalOpenLoans, EstRemainingbalanceofOpenLoans=s.EstRemainingbalanceofOpenLoans, EstValue=s.EstValue,                
 EstLoantoValue=s.EstLoantoValue,EstEquity=s.EstEquity,MonthlyRent=s.MonthlyRent,GrossYield=s.GrossYieldPercentage,LienType=s.LienType,LienDate=s.LienDate,                
 LienAmount=s.LienAmount,BKDate=s.BKDate,DivorceDate=s.DivorceDate,                 
 PreFcRecordingDate=s.PreFcRecordingDate ,PreFcRecordType=s.PreFcRecordType ,PreFcUnpaidBalance=s.PreFcUnpaidBalance,                
 PreFcDefaultAmount=s.PreFcDefaultAmount ,PreFcAuctionDate=s.PreFcAuctionDate,PreFCAuctionTime=s.PreFCAuctionTime                 
,PreFCTrusteeAttorneyName=s.PreFCTrusteeAttorneyName ,PreFCTrusteeRefNumber=s.PreFCTrusteeRefNumber ,                
PreFCTrusteeAttorneyAddress=s.PreFCTrusteeAttorneyAddress ,PreFCBorrower1Name=s.PreFCBorrower1Name,                
DateAddedtoList=s.DateAddedtoList, MethodofAdd=s.MethodtoAdd, Bedrooms=s.Bedrooms, TotalBathrooms=s.TotalBathrooms,                 
LotSizeSqft=s.LotSizeSqft, YearBuilt=s.YearBuilt, EffectiveYearBuilt=s.EffectiveYearBuilt, PoolType=s.PoolType, Vacant=s.Vacant,                 
HOAPresent=s.HOAPresent, NumberofStories=s.NumberofStories,LocationType=s.LocationType, PropertyClass=s.PropertyClass, OwnerOccupied=s.OwnerOccupied,                     
  LocationOwnerNameModified=CASE LocationIsBusiness                        
 WHEN 1 THEN s.OwnerName                        
  WHEN 0 THEN                         
  (CASE CHARINDEX(' ', s.OwnerName, 1)                        
        WHEN 0 THEN  (dbo.LastWord(s.OwnerName))                        
 Else (dbo.LastWord(s.OwnerName) +' ' + dbo.FirstWord(s.OwnerName)) End)                        
   End                   
                   
  --CASE LocationIsBusiness WHEN 1 THEN s.OwnerName WHEN 0 THEN dbo.LastWord(s.OwnerName) + ' ' + dbo.FirstWord(s.OwnerName) End                        
       WHEN not matched
	   THEN                              
     INSERT (RegionId,  PrimaryId, SellDate, SellAmount, Address1, City, State, Zip,  Response ,                
Neighborhood, Acres, ZoneDesc, LandValue, PrevLandValue, DwellingValue, PrevDwellingValue, TotalValue, PrevTotalValue,                 
PrevSellDate, PrevSellAmount, Grantor, PrevGrantor, DocNum, PrevDocNum, SquareFootage, Topography, LocationType,                 
OwnerId, IsBusiness, IsChurch, IsTrust,LocationOwnerName, ListPriorityId, AccuzipLocationRequest, AccuzipLocationResponse, InsertedDate,                 
LocationOwnerNameModified,                
 BuildingSqft,LastCashBuyer, PriorSaleDate, PriorSaleAmount, PriorSaleCashBuyer, PriorSaleBuyerName1, PriorSaleBuyerName2,                 
OpenLoan1Date, OpenLoan1Balance, OpenLoan1Type, OpenLoan1Lender, TotalOpenLoans, EstRemainingbalanceofOpenLoans, EstValue,                
 EstLoantoValue,EstEquity,MonthlyRent,GrossYield,LienType ,LienDate ,LienAmount ,BKDate ,DivorceDate,                 
 PreFcRecordingDate ,PreFcRecordType ,PreFcUnpaidBalance ,PreFcDefaultAmount ,PreFcAuctionDate ,PreFCAuctionTime                 
,PreFCTrusteeAttorneyName ,PreFCTrusteeRefNumber ,PreFCTrusteeAttorneyAddress ,PreFCBorrower1Name ,                
DateAddedtoList, MethodofAdd, Bedrooms, TotalBathrooms, LotSizeSqft, YearBuilt, EffectiveYearBuilt, PoolType, Vacant,                 
HOAPresent, NumberofStories, PropertyClass, OwnerOccupied)                              
    values (@regionId,  LocationPrimaryId, S.LocationSellDate, S.LocationSellAmount, S.LocationAddress1, S.LocationCity,                              
    S.LocationState, S.LocationZip, S.LocationResponse, S.LocationNeighborhood, S.LocationAcres, S.LocationZoneDesc, S.LocationLandValue,      
    S.LocationPrevLandValue, S.LocationDwellingValue, S.LocationPrevDwellingValue, S.LocationTotalValue, S.LocationPrevTotalValue,                
    S.LocationPrevSellDate, S.LocationPrevSellAmount, S.LocationGrantor, S.LocationPrevGrantor, S.LocationDocNum, S.LocationPrevDocNum,                               
    S.LocationSquareFootage, S.LocationTopography, S.LocationType, OwnerId,S.LocationIsBusiness, S.LocationIsChurch, S.LocationIsTrust,s.OwnerName,11,                        
 s.LocationAddress1+'_'+s.LocationCity+'_'+s.LocationState+'_'+s.LocationZip,                
 s.LocationAddress1+'_'+s.LocationCity+'_'+s.LocationState+'_'+s.LocationZip,getdate(),                
 CASE LocationIsBusiness WHEN 1 THEN s.OwnerName                       
  WHEN 0 THEN                         
  (CASE CHARINDEX(' ', s.OwnerName, 1)                        
        WHEN 0 THEN  (dbo.LastWord(s.OwnerName))                        
  Else (dbo.LastWord(s.OwnerName) +' ' + dbo.FirstWord(s.OwnerName)) End)                      
   End        
   ,BuildingSqFt,LastCashBuyer,S.PriorSaleDate, S.PriorSaleAmount  , S.PriorSaleCashBuyer, S.PriorSaleCashBuyerName1, S.PriorSaleCashBuyerName2,OpenLoan1Date,       OpenLoan1Balance,                
    OpenLoan1Type,  OpenLoan1Lender,  TotalOpenLoans,  EstRemainingBalanceOfOpenLoans, EstValue,   EstLoanToValue, EstEquity,                 
  MonthlyRent,  GrossYieldPercentage,    LienType,  LienDate,        LienAmount,  BKDate,     DivorceDate,  PreFcRecordingDate,                 
  PreFcRecordType,       PreFcUnpaidBalance,      PreFcDefaultAmount,  PreFcAuctionDate,  PreFcAuctionTime, PreFcTrusteeAttorneyName, PreFcTrusteeRefNumber,                 
  PreFcTrusteeAttorneyAddress,  PreFcBorrower1Name, DateAddedToList, MethodToAdd, Bedrooms, TotalBathrooms,  LotSizeSqFt, YearBuilt,                
     EffectiveYearBuilt,  PoolType, Vacant, HOAPresent,NumberOfStories,PropertyClass,                
  OwnerOccupied);                        
 --CASE LocationIsBusiness WHEN 1 THEN s.OwnerName WHEN 0 THEN dbo.LastWord(s.OwnerName) + ' ' + dbo.FirstWord(s.OwnerName) End);                             
                              
Print 'Inserted Location  using merge ' + CONVERT( VARCHAR(24), GETDATE(), 121)          
                              
                                 
UPDATE [Owner] SET FullAddress = REPLACE(Address1, '''', '') + ' ' + REPLACE(City, '''', '') + ' ' + REPLACE([State], '''', '') + ' ' + REPLACE(Zip, '''', '')                               
WHERE OwnerId IN(SELECT DISTINCT OwnerId FROM [Location] WHERE RegionId = @regionId)                              
 Print 'updated IsOwnerOccupied in owner  - ' + CONVERT( VARCHAR(24), GETDATE(), 121)                              
                              
Update UploadLog set ProcessingState = 9 where UploadLogId = @uploadLogId;                              
                            
-- Set default owner occupant for new data.                         
UPDATE LOCATION SET IsOwnerOccupied = 0 WHERE RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )                           
UPDATE [Location] SET IsOwnerOccupied = 1                               
WHERE                               
 LocationId IN(                              
  SELECT l.LocationId FROM [Location] l LEFT OUTER JOIN [Owner] o ON o.OwnerId = l.OwnerId                               
  WHERE (o.Address1 NOT LIKE 'PO%' OR o.Address1 NOT LIKE 'P O%')  AND                               
                              
  (                              
   (SELECT TOP 1 string FROM dbo.[IgnoreDirections] (UPPER(l.Address1))) = (SELECT TOP 1 string FROM dbo.[IgnoreDirections] (UPPER(o.Address1)))                              
   OR                              
   (                              
    --(SELECT [value] FROM SplitWords(l.Address1) WHERE pos = 1) = (SELECT [value] FROM SplitWords(o.Address1) WHERE pos = 1) AND                              
    --(SELECT [value] FROM SplitWords(l.Address1) WHERE pos = 2) = (SELECT [value] FROM SplitWords(o.Address1) WHERE pos = 2)                              
          
    (SELECT [value] FROM SplitWords((SELECT TOP 1 string FROM dbo.[IgnoreDirections] (UPPER(l.Address1)))) WHERE rowNum = 1) = (SELECT [value] FROM SplitWords((SELECT TOP 1 string FROM dbo.[IgnoreDirections] (UPPER(o.Address1)))) WHERE rowNum = 1) AND   
                    
    (SELECT [value] FROM SplitWords((SELECT TOP 1 string FROM dbo.[IgnoreDirections] (UPPER(l.Address1)))) WHERE rowNum = 2) = (SELECT [value] FROM SplitWords((SELECT TOP 1 string FROM dbo.[IgnoreDirections] (UPPER(o.Address1)))) WHERE rowNum = 2)        
                           
   )                              
  )                              
                                 
  AND l.RegionId = @regionId ) and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )                             
                              
-- If Owner grantor first word(last name) matches but not the full name match.                              
Update UploadLog set ProcessingState = 10 where UploadLogId = @uploadLogId;          
                         
UPDATE LOCATION SET OwnerGrantorMatch = 0 WHERE RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) );                             
UPDATE [Location] SET OwnerGrantorMatch = 1                              
WHERE                               
LocationId IN(                              
 SELECT DISTINCT                              
  l.LocationId                              
 FROM [Location] l                             
  LEFT OUTER JOIN [Owner] o ON o.OwnerId = l.OwnerId                              
 WHERE                               
  l.RegionId = @regionId AND LEN(Grantor) > 0                              
  AND ((SELECT [value] FROM SplitWords(o.[Name]) WHERE pos = 1) = (SELECT [value] FROM SplitWords(l.Grantor) WHERE pos = 1))                              
  AND ((SELECT UPPER(l.Grantor)) <> (SELECT UPPER(o.Name)))                              
) and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )                               
--End additional translations                              
                              
DECLARE @regionInputDate1 DATETIME = (SELECT InputDate1 FROM [Region] WHERE RegionId = @regionId)                              
DECLARE @regionInputDate2 DATETIME = (SELECT InputDate2 FROM [Region] WHERE RegionId = @regionId)                              
DECLARE @regionInputAmount1 DECIMAL(18,2) = (SELECT InputAmount1 FROM [Region] WHERE RegionId = @regionId)                          
DECLARE @regionInputAmount2 DECIMAL(18,2) = (SELECT InputAmount2 FROM [Region] WHERE RegionId = @regionId)                              
                              
Print 'start List Priority - ' + CONVERT( VARCHAR(24), GETDATE(), 121)                              
Update UploadLog set ProcessingState = 11 where UploadLogId = @uploadLogId;          
--SET LIST PRIORITY - Only set on locations still set to default and in region                              
--TRS2 Trust: 5 - 8                              
UPDATE [Location] SET ListPriorityId = 5 WHERE IsTrust = 1 AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )                             
--CHURCH Church: 10 - 99                              
UPDATE [Location] SET ListPriorityId = 10 WHERE IsChurch = 1 AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )                                                        
--INH1 Absentee List 1: 6 - 1                              
UPDATE [Location] SET ListPriorityId = 6 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 0 and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )                               
AND OwnerGrantorMatch = 1 AND RegionId = @regionId                    
--OIN1 Owner-Occupant List 1: 1 - 2                              
UPDATE [Location] SET ListPriorityId = 1 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )        
AND OwnerGrantorMatch = 1 AND RegionId = @regionId                                                           
--ABS1 Absentee List 3: 7 - 3                              
UPDATE [Location] SET ListPriorityId = 7 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 0 and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )        
AND (NOT(SellDate IS NULL) AND SellDate <= @regionInputDate1) AND RegionId = @regionId ;                                                
--TRS1 Absentee List 4: 8 - 4                              
UPDATE [Location] SET ListPriorityId = 8 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 0 and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )        
AND (NOT(SellAmount IS NULL) AND SellAmount <= @regionInputAmount1) AND RegionId = @regionId  ;                                                        
--OWN20 Owner-Occupant List 20: NOTE: Sell Date less than or equal to Today's date.                              
--UPDATE [Location] SET ListPriorityId = 13 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND (NOT(SellDate IS NULL) AND SellDate <= CONVERT(date,DATEADD(yy,-20,GETDATE()))) AND RegionId = @regionId                              
UPDATE [Location] SET ListPriorityId = 13 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND (NOT(SellDate IS NULL) AND SellDate <= CONVERT(date,DATEADD(yy,-20,GETDATE()))) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                         
--OWN1 Owner-Occupant List 3: 2 - 5                              
--UPDATE [Location] SET ListPriorityId = 2 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND (NOT(SellDate IS NULL) AND SellDate <= @regionInputDate1) AND RegionId = @regionId                              
UPDATE [Location] SET ListPriorityId = 2 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND (NOT(SellDate IS NULL) AND SellDate <= CONVERT(date,DATEADD(yy,-13,GETDATE()))) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                    
--OON1 Owner-Occupant List 4: 3 - 6                              
UPDATE [Location] SET ListPriorityId = 3 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND (NOT(SellAmount IS NULL) AND SellAmount <= @regionInputAmount1) AND RegionId = @regionId  and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) )     ;                                                   
--BUY1 Absentee List 5: 9 - 9                              
UPDATE [Location] SET ListPriorityId = 9 WHERE IsOwnerOccupied = 0 AND (NOT(SellAmount IS NULL) AND SellAmount >= @regionInputAmount2) AND (NOT(SellDate IS NULL) AND SellDate >= @regionInputDate2) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                         
--BUY2 Owner-Occupant List 5: 4 - 10                              
UPDATE [Location] SET ListPriorityId = 4 WHERE IsOwnerOccupied = 1 AND (NOT(SellAmount IS NULL) AND SellAmount >= @regionInputAmount2) AND (NOT(SellDate IS NULL) AND SellDate >= @regionInputDate2) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                         
--END LIST PRIORITY--                              
                     
-- Checks whether region data is uploading for the first time or not.                              
IF((Select Count(*) from uploadlog where regionid = @regionId) > 1 )                              
BEGIN                              
--UPDATE [Location] SET ListPriorityId = 11 WHERE RegionId = @regionId  commented                     
                              
--SET LIST PRIORITY - Only set on locations still set to default and in region                              
--TRS2 Trust: 5 - 8                              
UPDATE [Location] SET ListPriorityId = 5 WHERE IsTrust = 1 AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) );                                                           
--CHURCH Church: 10 - 99                              
UPDATE [Location] SET ListPriorityId = 10 WHERE IsChurch = 1 AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                         
--INH1 Absentee List 1: 6 - 1                              
UPDATE [Location] SET ListPriorityId = 6 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 0 AND OwnerGrantorMatch = 1 AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                         
--OIN1 Owner-Occupant List 1: 1 - 2                              
UPDATE [Location] SET ListPriorityId = 1 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND OwnerGrantorMatch = 1 AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                        
--ABS1 Absentee List 3: 7 - 3                              
UPDATE [Location] SET ListPriorityId = 7 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 0 AND (NOT(SellDate IS NULL) AND SellDate <= @regionInputDate1) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;
--TRS1 Absentee List 4: 8 - 4                              
UPDATE [Location] SET ListPriorityId = 8 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 0 AND (NOT(SellAmount IS NULL) AND SellAmount <= @regionInputAmount1) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;
--OWN20 Owner-Occupant List 20: NOTE: Sell Date less than or equal to Today's date.                              
UPDATE [Location] SET ListPriorityId = 13 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND (NOT(SellDate IS NULL) AND SellDate <= CONVERT(date,DATEADD(yy,-20,GETDATE()))) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;
--OWN1 Owner-Occupant List 3: 2 - 5                              
UPDATE [Location] SET ListPriorityId = 2 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND (NOT(SellDate IS NULL) AND SellDate <= CONVERT(date,DATEADD(yy,-14,GETDATE()))) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) );                                                           
--OON1 Owner-Occupant List 4: 3 - 6                              
UPDATE [Location] SET ListPriorityId = 3 WHERE ListPriorityId = 11 AND IsOwnerOccupied = 1 AND (NOT(SellAmount IS NULL) AND SellAmount <= @regionInputAmount1) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;
--BUY1 Absentee List 5: 9 - 9                              
UPDATE [Location] SET ListPriorityId = 9 WHERE IsOwnerOccupied = 0 AND (NOT(SellAmount IS NULL) AND SellAmount >= @regionInputAmount2) AND (NOT(SellDate IS NULL) AND SellDate >= @regionInputDate2) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                          
--BUY2 Owner-Occupant List 5: 4 - 10                          
UPDATE [Location] SET ListPriorityId = 4 WHERE IsOwnerOccupied = 1 AND (NOT(SellAmount IS NULL) AND SellAmount >= @regionInputAmount2) AND (NOT(SellDate IS NULL) AND SellDate >= @regionInputDate2) AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()) ) ;                                                          
--END LIST PRIORITY--                              
     Print 'Niche List start'        
Create TABLE #NewTable                               
(                     
RowId INT PRIMARY KEY NOT NULL,                 
[OwnerOccupied] nvarchar(255),                   
[LocationCounty] nvarchar(255),                    
[DoNotMail] nvarchar(255),                   
[PropertyClass] nvarchar(255),          
[LocationType] nvarchar(255),                 
[Bedrooms] int,                      
[TotalBathrooms] int,                   
[BuildingSqFt] nvarchar(255),                      
[LotSizeSqFt] nvarchar(255),                      
[YearBuilt] nvarchar(255),                      
[EffectiveYearBuilt] nvarchar(255),                      
[PoolType] nvarchar(255),                      
[Vacant] nvarchar(255),                      
[HOAPresent] nvarchar(255),                      
[NumberOfStories] DECIMAL(18, 2),                      
[LastCashBuyer] nvarchar(255),                      
[PriorSaleDate] datetime,                      
[PriorSaleAmount]  DECIMAL(18, 2),                      
[PriorSaleCashBuyer] nvarchar(255),                  
[PriorSaleCashBuyerName1] nvarchar(255),                      
[PriorSaleCashBuyerName2] nvarchar(255),                      
[OpenLoan1Date] datetime,                      
[OpenLoan1Balance]  DECIMAL(18, 2),                      
[OpenLoan1Type] nvarchar(255),                      
[OpenLoan1Lender] nvarchar(255),                        
[TotalOpenLoans] nvarchar(255),                      
[EstRemainingBalanceOfOpenLoans] nvarchar(255),                  
[EstValue] nvarchar(255),                      
[EstLoanToValue] decimal(18,2),                      
[EstEquity] nvarchar(255),                      
[MonthlyRent] nvarchar(255),                    
[GrossYieldPercentage] DECIMAL(18, 2),                   
[LienType] nvarchar(255),                      
[LienDate] datetime,                      
[LienAmount]  DECIMAL(18, 2),                      
[BKDate] datetime,                      
[DivorceDate] datetime,                      
[PreFcRecordingDate] datetime,                   
[PreFcRecordType] nvarchar(255),                      
[PreFcUnpaidBalance] nvarchar(255),                      
[PreFcDefaultAmount] DECIMAL(18, 2),                      
[PreFcAuctionDate] datetime,                    
[PreFcAuctionTime] nvarchar(255),                      
[PreFcTrusteeAttorneyName] nvarchar(255),                      
[PreFcTrusteeRefNumber] nvarchar(255),        
[PreFcTrusteeAttorneyAddress] nvarchar(255),                      
[PreFcBorrower1Name] nvarchar(255),                      
[DateAddedToList] datetime,                      
[MethodToAdd] nvarchar(255),                  
--------------------------------------------                  
 [LocationPrimaryId] nvarchar(255),                            
 [LocationNeighborhood] nvarchar(255),                               
 [LocationAddress1] nvarchar(255),                               
 [LocationCity] nvarchar(255),                               
 [LocationState] nvarchar(255),                               
 [LocationZip] nvarchar(255),                        
 [LocationResponse] nvarchar(255),              
 [OwnerName] nvarchar(255),                 
 [OwnerFirstName] nvarchar(255),            
 [OwnerLastName] nvarchar(255),                               
 [OwnerAddress1] nvarchar(255),                               
 [OwnerAddress2] nvarchar(255),                          
 [OwnerAddressResponse] VARCHAR(500),                     
 [OwnerCity] nvarchar(255),                               
 [OwnerState] nvarchar(255),                               
 [OwnerZip] nvarchar(255),                     
 [LocationAcres] DECIMAL(18, 4),                                  
 [LocationZoneDesc] nvarchar(255),                               
 [LocationLandValue] DECIMAL(18, 2),                               
 [LocationPrevLandValue] DECIMAL(18, 2),                               
 [LocationDwellingValue] DECIMAL(18, 2),                    
 [LocationPrevDwellingValue] DECIMAL(18, 2),           
 [LocationTotalValue] DECIMAL(18, 2),                               
 [LocationPrevTotalValue] DECIMAL(18, 2),                               
 [LocationSellDate] DATETIME,                   
 [LocationPrevSellDate] DATETIME,                               
 [LocationSellAmount] DECIMAL(18, 2),                               
 [LocationPrevSellAmount] DECIMAL(18, 2),                  
 [LocationGrantor] nvarchar(255),                               
 [LocationPrevGrantor] nvarchar(255),                               
 [LocationDocNum] nvarchar(255),                               
 [LocationPrevDocNum] nvarchar(255),                               
 [LocationSquareFootage] DECIMAL(18, 2),                    
 [LocationTopography] nvarchar(255),                              
 LocationIsChurch BIT NOT NULL DEFAULT 0,                              
 LocationIsBusiness BIT NOT NULL DEFAULT 0,                              
 LocationIsTrust BIT NOT NULL DEFAULT 0,                    
 OwnerSlug NVARCHAR(MAX) NOT NULL,        
 OwnerId INT NOT NULL DEFAULT 0 ,                   
 [RequestStreet] nvarchar(255),                         
 [RequestCity] nvarchar(255),                      
 [RequestState] nvarchar(255),                        
 [RequestZip] nvarchar(255),                        
 [FullAccuzipResponse] NVARCHAR(max),                  
 [OwnerMailingZip4] nvarchar(50),      
 [ResponseNCOA] varchar(50)
)          
insert into #NewTable select * from #toProcess        
        
 update  #NewTable         
set FullAccuzipResponse= ''        
        
update  #NewTable set FullAccuzipResponse=lp.Code from #NewTable temptbl         
join Location loc        
on loc.PrimaryId=temptbl.LocationPrimaryId        
join ListPriority lp        
on loc.ListPriorityId=lp.ListPriorityId        
where loc.RegionId=@regionId          
        
update  #NewTable         
set FullAccuzipResponse= 'HE-' +FullAccuzipResponse        
where  EstLoantoValue<=Convert(decimal(18,2),50) and EstLoanToValue>Convert(decimal(18,2),0);         
        
update  #NewTable         
set FullAccuzipResponse= 'Liens-'+FullAccuzipResponse        
where LienType is not null and LienType!='' ;         
        
update  #NewTable         
set FullAccuzipResponse='Bankrupcy-'+FullAccuzipResponse        
where BKDate is not null and BKDate!='' ;        
        
update  #NewTable         
set FullAccuzipResponse='Divorce-'+FullAccuzipResponse        
where  DivorceDate  is not null and DivorceDate!='' ;       
        
update  #NewTable         
set FullAccuzipResponse='PreFor-'+FullAccuzipResponse        
where  PreFcRecordingDate is not null and PreFcRecordingDate!='' ; 
        
update  #NewTable         
set  FullAccuzipResponse= 'F&C-'+FullAccuzipResponse          
where  EstLoantoValue=Convert(decimal(18,2),0)   and EstEquity!='' and EstEquity is not null  ;      
        
update  #NewTable         
set   FullAccuzipResponse= 'Vacant-'+FullAccuzipResponse        
where   Vacant='Yes';          
      
update  #NewTable         
set   FullAccuzipResponse= 'NCOA_Moves-'+FullAccuzipResponse        
where  ( ResponseNCOA='A' or  ResponseNCOA='91' or ResponseNCOA='92')  ;     
      
update  #NewTable         
set   FullAccuzipResponse= 'NCOA_Drops-'+FullAccuzipResponse        
where  ( ResponseNCOA!='' and ResponseNCOA!='A' and ResponseNCOA!='91' and ResponseNCOA!='92')  ;     
         
 declare @ListDetails table                            
    (            
  lstpid int,                        
      lstname nvarchar(255)                            
);          
        
--MERGE ListPriority AS lstp                            
--    USING                             
--     (select distinct FullAccuzipResponse from  #NewTable where FullAccuzipResponse is not null and FullAccuzipResponse!='') as temptbl                            
--    ON (lstp.Code=temptbl.FullAccuzipResponse)                      
--    WHEN NOT MATCHED BY TARGET        
-- THEN         
-- insert (Code,Name,PriorityLevel,ProcessingOrder,IsAbsentee) values        
--    (FullAccuzipResponse,FullAccuzipResponse,0,0,2)        
--  OUTPUT inserted.ListPriorityId,temptbl.FullAccuzipResponse INTO @ListDetails;        
  
  
MERGE ListPriority AS lstp                            
    USING                             
     (select distinct FullAccuzipResponse from #NewTable temptbl    
join ListPriority lp  
on lp.Code=temptbl.FullAccuzipResponse  
join RegionListFrequency r        
on  r.ListId=lp.ListPriorityId    
where r.RegionId=@regionId and temptbl.FullAccuzipResponse is not null and temptbl.FullAccuzipResponse!='' and r.DividedBy!=2) as temptbl                  
    ON (lstp.Code=temptbl.FullAccuzipResponse)                      
    WHEN NOT MATCHED BY TARGET        
 THEN         
 insert (Code,Name,PriorityLevel,ProcessingOrder,IsAbsentee) values        
    (FullAccuzipResponse,FullAccuzipResponse,0,0,2)        
  OUTPUT inserted.ListPriorityId,temptbl.FullAccuzipResponse INTO @ListDetails;  
    
  
   
--MERGE ListPriority AS lstp                            
--    USING                             
--     (select distinct FullAccuzipResponse from  #NewTable where FullAccuzipResponse is not null and FullAccuzipResponse!='') as temptbl                  
--    ON (lstp.Code=temptbl.FullAccuzipResponse)                      
--    WHEN NOT MATCHED BY TARGET        
-- THEN         
-- insert (Code,Name,PriorityLevel,ProcessingOrder,IsAbsentee) values        
--    (FullAccuzipResponse,FullAccuzipResponse,0,0,2)        
--  OUTPUT inserted.ListPriorityId,temptbl.FullAccuzipResponse INTO @ListDetails;        
        
        
 MERGE RegionListFrequency AS regfreq                            
    USING                             
     (select lstpid from @ListDetails  ) as temptbl                            
    ON (regfreq.ListId=temptbl.lstpid and regfreq.regionid=@regionId and regfreq.DividedBy!=2)                      
    WHEN NOT MATCHED BY TARGET        
 THEN         
 insert (Frequency,ListId,RegionId,DividedBy) values        
 (26,temptbl.lstpid,@regionId,4);        
        
        
 insert  into RegionListFrequency(Frequency,ListId,RegionId,DividedBy)        
 select distinct 26,lst.ListPriorityId,@regionId,4 from #NewTable temtb        
 join ListPriority lst        
 on lst.Code=temtb.FullAccuzipResponse        
 left join RegionListFrequency regf        
 on lst.ListPriorityId=regf.ListId     
 and regf.RegionId=@regionId        
 where regf.ListId is null        
        
        
update  Location set ListPriorityId=lst.ListPriorityId          
from Location loc         
join #NewTable temptbl         
on loc.PrimaryId=temptbl.LocationPrimaryId        
join ListPriority lst        
on lst.Code=temptbl.FullAccuzipResponse        
where loc.RegionId=@regionId AND (loc.InsertedDate >= DATEADD(MM, -6, GETDATE()) or loc.UpdatedDate >= DATEADD(MM, -6, GETDATE()) )        
        
        
        
--select * from @ListDetails        
        
 --truncate table toProcess1        
 --insert into toProcess1 select * from #NewTable        
        
        
 ---New logic End        
Print 'Niche List End'        
End        
        
        
Print 'start Additional Absentee List Priority - ' + CONVERT( VARCHAR(24), GETDATE(), 121)                              
DECLARE @ListTypeRules TABLE                              
(                              
 RowId INT IDENTITY PRIMARY KEY NOT NULL,                              
 MasterFieldId INT NOT NULL,                              
 FieldName NVARCHAR(50) NOT NULL,                              
 AbsenteeListTypeId INT NOT NULL,                              
 OccupantListTypeId INT NOT NULL,                              
 RegionId INT NOT NULL,                              
 Keyword varchar(500)  NOT NULL,                              
 operation varchar(500)  NOT NULL                              
)                              
INSERT INTO @ListTypeRules                              
SELECT MasterFieldId,FieldName,AbsenteeListTypeId,OccupantListTypeId,RegionId,Keyword,operation from ListTypeAutomationRules                              
WHERE RegionId = @regionId;                              
                              
SET @row = (SELECT MIN(RowId) FROM @ListTypeRules)                              
SET @rows = (SELECT MAX(RowId) FROM @ListTypeRules)                              
                              
WHILE(@row <= @rows)                              
BEGIN                              
                              
Declare @IsRuleExists INT = (Select TOP 1 RowId from @ListTypeRules where RegionId = @regionId);                              
Print @IsRuleExists                              
IF(@IsRuleExists IS NOT NULL)                              
BEGIN                              
                              
  Update UploadLog set ProcessingState = 12 where UploadLogId = @uploadLogId;                              
  Declare @MasterColumnName Varchar(100) = (Select Name from MasterFields where MasterFieldId IN                               
         (Select MasterFieldId from @ListTypeRules where RegionId = @regionId and RowId = @row));                              
 Print @MasterColumnName;                              
   Declare @AbsenteeListTypeId INT = (Select AbsenteeListTypeId from @ListTypeRules where RegionId = @regionId and RowId = @row);                              
   Declare @OccupantListTypeId INT = (Select OccupantListTypeId from @ListTypeRules where RegionId = @regionId and RowId = @row);                              
   Declare @Keyword varchar(100) = (Select Keyword from @ListTypeRules where RegionId = @regionId and RowId = @row);                              
   Declare @Operation varchar(100) = (Select operation from @ListTypeRules where RegionId = @regionId and RowId = @row);                              
   Declare @sql nvarchar(1000);                              
   Declare @sql1 nvarchar(1000);                              
  Declare @sqlDate nvarchar(20);
   SET @sqlDate=Convert( nvarchar(20),DATEADD(MM, -6, GETDATE()),120);
  IF(@Operation = 'Equal to' OR @Operation = 'OR')                              
  BEGIN                              
  Set @sql = 'UPDATE [Location] SET ListPriorityId = '+CONVERT(VARCHAR(20),@AbsenteeListTypeId)+' WHERE IsOwnerOccupied = 0 AND (InsertedDate >= '''+@sqlDate+''' or UpdatedDate >= '''+@sqlDate+''' ) AND                              
  RegionId = '+CONVERT(VARCHAR(20),@regionId)+' AND '+@MasterColumnName+' IN                               
  (Select [value] from string_split(('''+ @Keyword +'''),'';''))';                              
                                
  Set @sql1 = 'UPDATE [Location] SET ListPriorityId = '+CONVERT(VARCHAR(20),@OccupantListTypeId)+' WHERE IsOwnerOccupied = 1 AND (InsertedDate >= '''+@sqlDate+''' or UpdatedDate >= '''+@sqlDate+''' )  AND                                                             
  RegionId = '+CONVERT(VARCHAR(20),@regionId)+' AND '+@MasterColumnName+' IN                               
  (Select [value] from string_split(('''+ @Keyword +'''),'';''))';                              
  Print @sql1;                              
  Print @sql;                              
  EXECUTE  sp_executesql @sql;                              
  EXECUTE  sp_executesql @sql1;                              
 END                              
 ELSE IF(@Operation = 'starts with')                              
 BEGIN                              
 Declare @cond varchar(500);                              
 DECLARE @key varchar(50);                              
 DECLARE @Counter INT                              
      SET @Counter = 1                              
                              
   --DECLARE THE CURSOR FOR A QUERY.                              
   DECLARE Keyword CURSOR READ_ONLY                              
      FOR                              
   Select [value] from string_split((select ''+@Keyword+'' from @ListTypeRules where                               
   RegionId=''+CONVERT(VARCHAR(20),@regionId)+'' and RowId= @row),';');                              
                              
    --OPEN CURSOR.                              
    OPEN keyword                              
    --FETCH THE RECORD INTO THE VARIABLES.                              
      FETCH NEXT FROM Keyword INTO @key                              
                                     
    --LOOP UNTIL RECORDS ARE AVAILABLE.                      
   WHILE @@FETCH_STATUS = 0                              
      BEGIN                              
      IF @Counter = 1                               
   BEGIN                              
    Set @cond = 'like '''+RTRIM(LTRIM(@key))+'%''';                              
    END                               
                              
       FETCH NEXT FROM Keyword INTO @key                              
       Set @cond += ' OR '+@MasterColumnName+' like '''+RTRIM(LTRIM(@key))+'%''';                              
       Set @Counter = @Counter + 1;                              
  END                              
   CLOSE Keyword                              
      DEALLOCATE Keyword                              
                                
  Set @sql = 'UPDATE [Location] SET ListPriorityId = '+CONVERT(VARCHAR(20),@AbsenteeListTypeId)+' WHERE IsOwnerOccupied = 0 AND  (InsertedDate >= '''+@sqlDate+''' or UpdatedDate >= '''+@sqlDate+''' )  AND                                                          
    RegionId = '+CONVERT(VARCHAR(20),@regionId)+' AND ('+@MasterColumnName+' '+ @cond+')';                               
  Set @sql1 = 'UPDATE [Location] SET ListPriorityId = '+CONVERT(VARCHAR(20),@OccupantListTypeId)+' WHERE IsOwnerOccupied = 1 AND (InsertedDate >= '''+@sqlDate+''' or UpdatedDate >= '''+@sqlDate+''' )  AND                                                           
    RegionId = '+CONVERT(VARCHAR(20),@regionId)+' AND ('+@MasterColumnName+' '+ @cond+')';                               
                               
  print @sql;                              
  print @sql1;                              
  EXEC (@sql);                              
  EXEC (@sql1);                              
 END                              
END                              
SET @row = @row + 1                              
END                              
                              
                              
Print 'End List Priority - ' + CONVERT( VARCHAR(24), GETDATE(), 121)                            
--START ASSIGN PRIMARY LOCATION BASED ON LIST PRIORITY--                              
Update UploadLog set ProcessingState = 13 where UploadLogId = @uploadLogId;                              
                              
Update Location set IsPrimaryLocation = 0 where RegionId = @regionId  and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE())) ;                              
                              
UPDATE Location                              
SET IsPrimaryLocation = 1                              
--SET PrimaryLocationId = l.pLocationId                              
FROM (                              
 SELECT * FROM (SELECT l.OwnerId locOwnerId,l.LocationId pLocationId,l.ListPriorityId,lp.PriorityLevel,                              
    ROW_NUMBER() OVER (PARTITION BY l.OwnerId ORDER BY lp.PriorityLevel,                               
    CASE WHEN l.ListPriorityId in (9,4) THEN l.SellDate END ASC,                              
    CASE WHEN l.ListPriorityId <> 7 THEN l.SellDate END DESC) AS Duplicate FROM [Location] l                              
  LEFT OUTER JOIN ListPriority lp ON lp.ListPriorityId = l.ListPriorityId                              
  WHERE l.RegionId = @regionId) s where Duplicate =1) AS l                              
WHERE OwnerId = l.locOwnerId ANd LocationId = l.pLocationId AND (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()))                                 
                              
                              
Update UploadLog set ProcessingState = 14 where UploadLogId = @uploadLogId;                              
--update l set l.ListPriorityId ='12'                               
--from Location l Left join Owner o                               
--On o.OwnerId = l.OwnerId LEFT  JOIN (SELECT OwnerId, COUNT(LocationId) AS NumLocations FROM [Location] where RegionId = @regionId                              
--GROUP BY OwnerId) lCnts ON lCnts.OwnerId = o.OwnerId where lCnts.NumLocations > 1 AND l.IsPrimaryLocation <> 1 AND                              
--l.RegionId = @regionId                              
                              
--Make sure address and names are in friendly case--                              
-- For non business owner                              
UPDATE [Owner] SET AlternateName = dbo.LastWord([Name]) + ' ' + dbo.FirstWord([Name]) WHERE AlternateName IS NULL AND OwnerId IN(SELECT DISTINCT OwnerId FROM [Location] WHERE RegionId = @regionId AND IsBusiness = 0)                              
--For business owner                              
UPDATE [Owner] SET AlternateName = [Name] WHERE AlternateName IS NULL AND OwnerId IN(SELECT DISTINCT OwnerId FROM [Location] WHERE RegionId = @regionId AND IsBusiness = 1)                              
                        
UPDATE [Owner] SET FirstName = dbo.FirstWord([Name]) WHERE LastName IS NULL AND OwnerId IN(SELECT DISTINCT OwnerId FROM [Location] WHERE RegionId = @regionId AND IsBusiness = 0)                             
UPDATE [Owner] SET LastName =  dbo.LastWord([Name]) WHERE FirstName  IS NULL AND OwnerId IN(SELECT DISTINCT OwnerId FROM [Location] WHERE RegionId = @regionId AND IsBusiness = 0)                               
--For business owner                              
UPDATE [Owner] SET FirstName = [Name] WHERE FirstName IS NULL AND OwnerId IN(SELECT DISTINCT OwnerId FROM [Location] WHERE RegionId = @regionId AND IsBusiness = 1)                              
         
    -- Checks whether region data is uploading for the first time or not.                              
IF((Select Count(*) from uploadlog where regionid = @regionId) > 1 )                              
BEGIN   
  
  ---Update the Address,City,State for NCOA Move ------------  
  
Update [Owner] SET Address1=temptbl.OwnerAddress1,city=temptbl.OwnerCity,State=temptbl.OwnerState,Zip=temptbl.OwnerZip   
from Owner o       
join #NewTable temptbl        
on o.OwnerId=temptbl.OwnerId  
where  ( temptbl.ResponseNCOA='A' or  temptbl.ResponseNCOA='91' or temptbl.ResponseNCOA='92')   
  
END  
  
UPDATE [Owner] SET                               
 Address1 = dbo.ProperCase(Address1),                              
 FullAddress = dbo.ProperCase(FullAddress)                              
 WHERE OwnerId IN(SELECT DISTINCT OwnerId FROM [Location] WHERE RegionId = @regionId)                              
UPDATE [Location] SET Address1 = dbo.ProperCase(Address1) WHERE RegionId = @regionId                              
                              
--UPDATE [Location] set City = '', State = '', Zip = ''                               
--WHERE LTRIM(RTRIM(Response)) IN ('No Match', 'No Match - PO Box Only') AND RegionId = @regionId                              
                            
UPDATE [Location] set City = '', State = '', Zip = ''                               
WHERE LTRIM(RTRIM(Response)) IN ('Address Not Found','Address not found in DPV Database','Street not found','City not found', 'Invalid Address', 'Multiple Response') AND RegionId = @regionId and (InsertedDate >= DATEADD(MM, -6, GETDATE()) or UpdatedDate >= DATEADD(MM, -6, GETDATE()));
                              
DECLARE @TotalProcessCount INT = ((SELECT Count(RowId) FROM Rawlocations))                              
SET @dSql = 'UPDATE UploadLog SET RowsProcessed = '+CONVERT(NVARCHAR, @TotalProcessCount)+', ProcessingState = 3, ProcessingEnded = GETDATE() WHERE UploadLogId = ' + CONVERT(NVARCHAR, @uploadLogId) EXEC(@dSql)                              
                               
    TRUNCATE TABLE #toProcess                        
    DROP TABLE  #toProcess 
END  
  
  