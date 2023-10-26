
Move cars.csv to Azure SQL Database 
 1. Follow current ETL/ELT Process
 2. Full Load 
 3. Truncate Data in your Production table before you load 
 4. Truncate your Staging 

 Load data from Source ---> Staging ----> Production -----> Truncate Straging  1st stime 


insert into Cars_prod
select * from Cars_staging
truncate table cars_staging




create TABLE Cars_Staging (
	Make nvarchar(100),
	Model nvarchar(200),
	Type nvarchar(100),
	Origin nvarchar(10),
	DriveTrain nvarchar(100),
	Length int null,
	loaddate datetime default getdate()
)


select * from Cars_Staging

CREATE TABLE Cars_Prod (
	Make nvarchar(100),
	Model nvarchar(200),
	Type nvarchar(100),
	Origin nvarchar(10),
	DriveTrain nvarchar(100),
	Length int null,
	loaddate datetime default getdate()
)

--Full Load 
create proc   sp_Dim_Cars
as
truncate table cars_prod
insert into select * from Cars_prod
select * from Cars_staging
truncate table cars_staging




--StoredProc to Log Pipeline Runs
create table pipeline_logs(
pipelineId nvarchar(1000),
pipelinename nvarchar(2000),
pipelinestarttime datetime,
runid nvarchar(100)
)


create proc Pipeline_log
@pipelineId nvarchar(1000),
@pipelinename nvarchar(2000),
@pipelinestarttime datetime,
@runid nvarchar(100)
as 
insert into pipeline_logs
values ( @pipelineId,@pipelinename,@pipelinestarttime,@runid)



select * from pipeline_logs