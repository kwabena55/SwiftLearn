/**

https://sldlgen2.blob.core.windows.net/data/NYCTripSmall.parquet
https://www.tablab.app/view/parquet?datatable-source=local-ddf6dc40-24eb-4bb0-a322-2c0c5c01c4b4

**/


SELECT SERVERPROPERTY ('IsPolyBaseInstalled') AS IsPolyBaseInstalled;

--  ###### STEP 01 ---Create master key Encrytion   ########
CREATE MASTER KEY ENCRYPTION BY PASSWORD='lE@RNaZURt556!'

--##### STEP 02 Database Scoped Credential   #####
--   ?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-10-14T08:43:46Z&st=2022-10-07T00:43:46Z&spr=https&sig=s5x2AYhgX3OTEf3%2BzI8lhFiu5UBv8eNJwriAY3lVAYE%3D
create DATABASE SCOPED CREDENTIAL [SLCred555]
WITH IDENTITY='SHARED ACCESS SIGNATURE',
SECRET='sp=r&st=2023-10-23T22:07:33Z&se=2023-11-29T07:07:33Z&spr=https&sv=2022-11-02&sr=c&sig=V4EnqsSpCHQta3o%2FqjO38gkTz9TJwBm0Qno%2F6uHd8RA%3D'
GO


---### STEP 03 Create Datasource ######################
EXEC sp_configure 'polybase enabled', 1
 RECONFIGURE

create EXTERNAL DATA SOURCE [Datasource5]
WITH ( LOCATION = N'https://sldlgen2.blob.core.windows.net/data',
CREDENTIAL=[SLCred555])

----########STEP 04 CREATE eXTERNAL fILE fORMAT ######
CREATE EXTERNAL FILE FORMAT FileFormat
WITH (
FORMAT_TYPE=PARQUET
)
GO

--- ####### STEP 05 CREATE EXT. TABLE ####
create EXTERNAL TABLE tbl_TaxiRidee(
 [DateID] int,
	 [MedallionID] int,
	 [HackneyLicenseID] int,
	 [PickupTimeID] int,
	 [DropoffTimeID] int,
	 [PickupGeographyID] int,
	 [DropoffGeographyID] int,
	 [PickupLatitude] float,
	 [PickupLongitude] float,
	 [PickupLatLong] nvarchar(4000),
	 [DropoffLatitude] float,
	 [DropoffLongitude] float,
	 [DropoffLatLong] nvarchar(4000),
	 [PassengerCount] int,
	 [TripDurationSeconds] int,
	 [TripDistanceMiles] float,
	 [PaymentType] nvarchar(4000),
	 [FareAmount] numeric(19,4),
	 [SurchargeAmount] numeric(19,4),
	 [TaxAmount] numeric(19,4),
	 [TipAmount] numeric(19,4),
	 [TollsAmount] numeric(19,4),
	 [TotalAmount] numeric(19,4)
)
WITH (
LOCATION='/NYCTripSmall.parquet',                          --'/*/*/*.parquet',
DATA_SOURCE=Datasource5,
FILE_FORMAT=FileFormat
);

select  * from [dbo].tbl_TaxiRidee




----######################## Second Half #####################################
---#### Creating Tables in MPP databases ..... Dedicated Pools)
A table with no index is called a Heap. Index improves I/O operations

--- Hash---
CREATE TABLE tbl_TaxiRide_Hash(
     [DateID] int,
	 [MedallionID] int,
	 [HackneyLicenseID] int,
	 [PickupTimeID] int,
	 [DropoffTimeID] int,
	 [PickupGeographyID] int,
	 [DropoffGeographyID] int,
	 [PickupLatitude] float,
	 [PickupLongitude] float,
	 [PickupLatLong] nvarchar(4000),
	 [DropoffLatitude] float,
	 [DropoffLongitude] float,
	 [DropoffLatLong] nvarchar(4000),
	 [PassengerCount] int,
	 [TripDurationSeconds] int,
	 [TripDistanceMiles] float,
	 [PaymentType] nvarchar(4000),
	 [FareAmount] numeric(19,4),
	 [SurchargeAmount] numeric(19,4),
	 [TaxAmount] numeric(19,4),
	 [TipAmount] numeric(19,4),
	 [TollsAmount] numeric(19,4),
	 [TotalAmount] numeric(19,4)
	 )
	 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=HASH([DateID])
	 );

--Round Robin----
CREATE TABLE tbl_TaxiRide_RB(
     [DateID] int,
	 [MedallionID] int,
	 [HackneyLicenseID] int,
	 [PickupTimeID] int,
	 [DropoffTimeID] int,
	 [PickupGeographyID] int,
	 [DropoffGeographyID] int,
	 [PickupLatitude] float,
	 [PickupLongitude] float,
	 [PickupLatLong] nvarchar(4000),
	 [DropoffLatitude] float,
	 [DropoffLongitude] float,
	 [DropoffLatLong] nvarchar(4000),
	 [PassengerCount] int,
	 [TripDurationSeconds] int,
	 [TripDistanceMiles] float,
	 [PaymentType] nvarchar(4000),
	 [FareAmount] numeric(19,4),
	 [SurchargeAmount] numeric(19,4),
	 [TaxAmount] numeric(19,4),
	 [TipAmount] numeric(19,4),
	 [TollsAmount] numeric(19,4),
	 [TotalAmount] numeric(19,4)
	 )
	 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=ROUND_ROBIN)
	

--- Replicated ----
CREATE TABLE tbl_TaxiRide_RP_NON(
     [DateID] int,
	 [MedallionID] int,
	 [HackneyLicenseID] int,
	 [PickupTimeID] int,
	 [DropoffTimeID] int,
	 [PickupGeographyID] int,
	 [DropoffGeographyID] int,
	 [PickupLatitude] float,
	 [PickupLongitude] float,
	 [PickupLatLong] nvarchar(4000),
	 [DropoffLatitude] float,
	 [DropoffLongitude] float,
	 [DropoffLatLong] nvarchar(4000),
	 [PassengerCount] int,
	 [TripDurationSeconds] int,
	 [TripDistanceMiles] float,
	 [PaymentType] nvarchar(4000),
	 [FareAmount] numeric(19,4),
	 [SurchargeAmount] numeric(19,4),
	 [TaxAmount] numeric(19,4),
	 [TipAmount] numeric(19,4),
	 [TollsAmount] numeric(19,4),
	 [TotalAmount] numeric(19,4)
	 )
	 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=REPLICATE)



--- ##CTAS -- CREATE TABLE AS -----
CREATE TABLE tbride_CTAS_RB
 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=ROUND_ROBIN)

	 AS

	 SELECT * FROM [dbo].[tbl_TaxiRidee]


	 SELECT * FROM tbride_CTAS_RB


	 CREATE TABLE tbride_CTAS_RP
 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=REPLICATE)

	 AS

	 SELECT * FROM [dbo].[tbl_TaxiRidee]



	  CREATE TABLE tbride_CTAS_HASH
	  WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=HASH([DateID])
	 )

	  AS

	 SELECT * FROM [dbo].[tbl_TaxiRidee]



-- #### Temp Tables In MPP ######
 #, ##

 IF OBJECT_ID(N'tempdb..#tbride_CTAS_RP',N'U') IS NOT NULL
 DROP TABLE #tbride_CTAS_RP
 	 CREATE TABLE #tbride_CTAS_RP
 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=ROUND_ROBIN)

	 AS

	 SELECT * FROM [dbo].[tbl_TaxiRidee]



-- ## CREATE MATERIALIZED VIEWS IN mpp/ SYNAPSE DED. POOL ####
 SELECT * FROM [dbo].[tbl_TaxiRidee]

 SELECT DATEID, COUNT(1) AS Trip_Count
 FROM [dbo].[tbl_TaxiRidee]
GROUP BY DATEID


CREATE MATERIALIZED VIEW dbo.Trip_Count
 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=ROUND_ROBIN
	 )

	 AS
SELECT DATEID, COUNT(1) AS Trip_Count
FROM dbo.tbride_CTAS_RP
GROUP BY DATEID


---#### Determine if a table has a right  distribution ###########
DBCC PDW_SHOWSPACEUSED('tbride_CTAS_RB')
DBCC PDW_SHOWSPACEUSED('tbride_CTAS_hash')
DBCC PDW_SHOWSPACEUSED('tbride_CTAS_RP')