# Managed Backup (MBU)


![MBU Jobs](/Users/amog2898/Desktop/mbujobs.png)

## Step 1: Stage_load

- Calls dtsx package:

  S:\ssis_packages\Data_Warehouse_Solution\Main_Control_Project\ManagedBackup_Stg_Loads\ManagedBackup_Stg_Loads\ **MBU_API_Stage_One_ParallelLoads_Prod.dtsx**

  ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 1.54.41 PM.png)


  - List of 14 APIs used:

    1. DF_LOAD_RS_Global_Filters_Lookup

      - https://production.mbuapi.storage.rackspace.com/v2/subclients?globalFilterState=ON,USE_CELL_LEVEL_POLICY&includeFields=csSubclientId,commcellId&per_page=1000

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.00.12 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.02.46 PM.png)

    2. DF_LOAD_Filter_info

      - https://production.mbuapi.storage.rackspace.com/v2/subclients?&includeFields=subclientId,commcellId,csSubclientId&per_page=1000

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.06.28 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.02.46 PM.png)

    3. DF_GlobalFilters_linux

      - https://production.mbuapi.storage.rackspace.com/v2/infrastructure/globalfilters/linux/

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.09.13 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.09.19 PM.png)

    4. DF_GlobalFilters_windows

      -   https://production.mbuapi.storage.rackspace.com/v2/infrastructure/globalfilters/windows/

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.12.05 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.12.10 PM.png)

    5. DF_Load_comcells_info

      -     https://production.mbuapi.storage.rackspace.com/v2/commcells/?&per_page=1000

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.14.29 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.14.46 PM.png)  

    6. DF_Load_vmr_backup_data

      -     https://production.mbuapi.storage.rackspace.com/v2/jobs/vsa/completed/?minEndTimeUnix={{User::Vmjobendtime}}&includeFields=commcellId,commcellName,csJobId,destVmClientName,encrypted,isAged,jobCategory,jobId,jobState,pseudoClientName,pseudoSubclientName,vmBackupType,vmCBTStatus,vmClientName,vmGUID,vmGuestSizeBytes,vmHost,vmIsGuestSizeValid,vmJobEndTime,vmJobFailedReason,vmJobStartTime,vmJobStatus,vmOperatingSystem,vmPoweredOffByCurrentJob,vmProxyName,vmSizeBytes,vmToolsVersion,vmTransportMode,vmUsedSpaceBytes&per_page=1000

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.17.09 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.17.15 PM.png)  

    7. DF_Load_SP_retention

      -     https://production.mbuapi.storage.rackspace.com/v2/storagepolicies

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 11.57.08 AM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 11.57.12 AM.png)

    8. DF_Load_Chbkphistory

      -     https://production.mbuapi.storage.rackspace.com/v2/jobs/completed?jobCategory=Backup&minEndTimeUnix={{User::MaxJobEndtime}}&includeFields=commcellId,backupsetName,csSubclientId,instanceName,subclientName,agentId,backupLevel,clientName,jobEndTime,jobStartTime,jobStatus,numobjects,uncompTrnsBytes,jobTransferTime,csJobId,jobId,agentName,csAgentId&per_page=1000

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 11.58.42 AM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 11.58.51 AM.png)  

   9. DF_Load_ChSubClient

      -     https://production.mbuapi.storage.rackspace.com/v2/subclients?&includeFields=commcellId,subclientId,data_storagepolicyName,csSubclientId&per_page=1000

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.01.02 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.01.10 PM.png)  

    10. DF_Load_rs_idata

       -     https://production.mbuapi.storage.rackspace.com/v2/agents?&includeFields=commcellId,clientName,agentId,agentName,agentStatus,commcellName,csAgentId&per_page=1000

       - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.03.15 PM.png)

       - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.03.20 PM.png)  

   11. DF_Load_filter_mssql

      -     https://production.mbuapi.storage.rackspace.com/v2/subclients?&includeFields=commcellId,subclientId,modifiedTime,clientName,agentName,instanceName,backupsetName,subclientName,csSubclientId&per_page=1000

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.06.05 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.06.10 PM.png)  

   12. DF_Load_Sched_info

      -     https://production.mbuapi.storage.rackspace.com/v2/subclients/schedules?&includeFields=clientName,agentName,backupTypeId,backupsetName,dayslist,instanceName,nexttime,occurrence,schedinterval,scheduleId,scheduleTask,starttime,subclientId,subclientName,createdTime,modifiedTime,commcellId,schedulepolicyName,csScheduleId,csSubclientId&per_page=1000

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.07.19 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.07.29 PM.png)

    13. DF_Load_storagepolicies

       -     https://production.mbuapi.storage.rackspace.com/v2/subclients/storagepolicies?&includeFields=commcellId,data_retentionCycles,data_retentionDays,data_storagepolicyName,data_strgpolcpyName,subclientId,csSubclientId&per_page=1000

       - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.09.19 PM.png)

       - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.09.23 PM.png)

   14. DF_Load_savesets

      -     https://production.mbuapi.storage.rackspace.com/v2/subclients/savesets?includeFields=clientName,subclientId,subclientName,modifiedTime,commcellId,clientId,commcellName,csSubclientId,createdTime,savesetCategory,savesetPath,savesetType,savesetTypeId,savesetId,csClientId&per_page=1000&minModTimeUnix={{User::SavesetsModtime}}

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.11.02 PM.png)

      - ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.11.06 PM.png)             




## Step 2: raw_cv_detail_migration

- Location:  S:\SSIS_Packages\Data_Warehouse_Solution\Main_Control_Project\RAW_Load_Project\ ** RAW_CV_MBU_Detail_Migration.dtsx **


Stage_One DB contains stored procedure:
[udsp_RAW_CV_MBU_Detail]
- ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.23.33 PM.png)  

- Insert into level two the backup tables, load into detail table  from stage_one.[dbo].[Stg_MBU_RAW_CHJobBkpHistory]


```sql
USE [Stage_One]
GO
/****** Object:  StoredProcedure [dbo].[udsp_RAW_CV_MBU_Detail]    Script Date: 7/17/2019 2:26:43 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/*
-- =======================================================================================================================
-- Author:		
-- Create date:
--
-- Modification History:
--14/05/2009	RV		concatenated insstance value to the subclient column
                       and also modified the where clause

5/19/2011		DT				change source to QNET(QNET3) with openquery update
05/19/2017      Vamsi Bala		Repointed the source to read data from new raw table Stg_MBU_RAW_CHJobBkpHistory
               loaded from MBU API

-- =========================================================================================================================
*/

ALTER PROC [dbo].[udsp_RAW_CV_MBU_Detail]
AS
truncate table RAW_CV_MBU_Detail
insert into RAW_CV_MBU_Detail ([CommCellID]
     ,[ClientName]
     ,[Backup_Set_Name]
     ,[SubClient]
     ,[AppTypeName]
     ,[Job_Start_Datetime]
     ,[Job_End_Datetime]
     ,[Backup_Level]
     ,[Status_ID]
     ,[Job_Status_Text]
     ,[UncompSize_Bytes]
     ,[Total_Size_MB]
     ,[Total_Size_GB]
     ,[Duration_Minutes]
     ,[Duration_Seconds]
     ,[Object_Count])

select
JBH.CommCellID,
JBH.ClientName,
JBH.Backup_Set_Name,
--SubClientName as SubClient,
JBH.SubClientName + ' ' + JBH.InstanceName as SubClient,
appTypeName,
(DATEADD(s,TimeStart,'1970-01-01 00:00:00')) as  Job_Start_Datetime,
(DATEADD(s,TimeEnd,'1970-01-01 00:00:00')) as Job_End_Datetime,

CASE
       WHEN bkplevel='Full' THEN 1
       WHEN bkplevel='Incremental' THEN 2
       WHEN bkplevel='Unknown' THEN 3
       WHEN bkplevel='Differential' THEN 4
       WHEN bkplevel='SyntheticFull' THEN 64
       WHEN bkplevel='Transaction Log w. NoTruncate'  THEN 256
       WHEN bkplevel='ASR'  THEN 512
       WHEN bkplevel='Offline Full'  THEN 1024
   ELSE 0         -- this is just a default value for the bkplevel that doesn't match the possible values from source
   END AS Backup_Level,
JBH.AppTypeID as Status_ID,
CASE

   WHEN Status ='Success' THEN 'Completed'		
   WHEN Status ='PartialSuccess' THEN 'Completed w/ one or more errors'
   ELSE Status
   END AS Job_Status_Text,
UncompSize as UncompSize_Bytes,
cast(cast(UncompSize as bigint) / 1048576.000 as numeric(38,10)) as Total_Size_MB,
cast(cast(UncompSize as bigint) / 1048576.0 /1024 as numeric(38,10)) as Total_Size_GB,
WriteTime/60 as Duration_Minutes,
WriteTime  as Duration_Seconds,
NumOfObjects as Object_Count
from
--	[QNET].QNet.dbo.CHJobBkpHistory JBH

(

   select
       distinct
         CommCellID,
         clientname,
         Backup_Set_Name,
         appid,
         instancename,
         timeend,		
         bkplevel,
         SubClientName,
         AppTypeID,
         Status,
         TimeStart,
         UncompSize,
         WriteTime,
         NumOfObjects,
         appTypeName
       from stage_one.[dbo].[Stg_MBU_RAW_CHJobBkpHistory]
       where commcellid is not null and Backup_Set_Name is not null
       --where
       --	TimeEnd >= DATEDIFF(s, '1970-01-01 00:00:00',dateadd(dd,-1, getdate()))
       -- corresponding stg table will be loaded incremental, hence time filter ignored

     ) JBH



 --inner join stage_one.dbo.[Stg_MBU_RAW_CV_MBU_rs_idata] ID
   --on ID.apiAppTypeId = JBH.AppTypeID     --------  getting appTypeName from ChJobBkpHistory table

 --inner join [QNET].QNet.dbo.CHJobStatus JS
   --on JS.StatusID = JBH.Status             ------- no need of this, passing completed in api call

--where
--TimeEnd >=  '2009-05-01'
--TimeEnd >= dateadd(dd,-1, getdate())
---and JS.StatusName not in ('Pending', 'Queued')
--and
/*JS.StatusName  not in ('Pending','Queued','Running','Waiting')*/ -- ------- no need of this, passing completed in api call

-- Insert Backup Levels
insert into stage_two_dw.dbo.wrk_backup_level (backup_level_name)
select distinct Backup_Level from RAW_CV_MBU_Detail

-- Insert backup server
insert into stage_two_dw.dbo.wrk_backup_server_name (backup_server_name)
select distinct clientname from RAW_CV_MBU_Detail

-- Insert backup status
insert into stage_two_dw.dbo.wrk_backup_status (backup_status_name)
select distinct Job_Status_Text from RAW_CV_MBU_Detail

-- Insert backup Target
insert into stage_two_dw.dbo.wrk_backup_target (backup_target_name)
select distinct substring(AppTypeName + ':  ' + LTRIM(RTRIM(SubClient)),1,224) from RAW_CV_MBU_Detail


```

- Inserts in ** stage_two_dw.dbo.wrk_backup_server_name,wrk_backup_target,wrk_backup_status etc ** tables

## Step 3: update_raw_cv_detail on retry

- ![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.37.15 PM.png)  
## Step 4: record_count_check_raw_cv_mbu_detail
-![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.39.09 PM.png)

```sql
USE SSIS_LoggingDB

declare
@COUNT INT,
@eMailAddress varchar(1024)='GET_ETL_SUPPORT@RACKSPACE.COM',
@eMailText varchar(4000)='Hi Team,

We are not recieving backup data from QNet Server. Please check the below procedure for source table QNet.dbo.CHJobBkpHistory
and Target table Stage_one.dbo.RAW_CV_MBU_Detail on EBI-ETL-DEV-02 for current load of Managed_Backup job.


SQL JOB: Managed_Backup
Procedure: Stage_one.dbo.udsp_RAW_CV_MBU_Detail

Thanks
EBI-OPS
';

select top 1 @COUNT= Table_Log_Final_Count FROM SSIS_LoggingDB.[dbo].[Table_Log] WHERE Table_Log_Record_Created_By= 'RAW_CV_MBU_Detail_Migration'
and CONVERT(VARCHAR(10),table_log_record_created_datetime,110)  = CONVERT(VARCHAR(10),GETDATE(),110)
ORDER BY table_log_record_created_datetime DESC

IF @COUNT = 0
BEGIN

exec msdb.dbo.sp_send_dbmail
    @profile_name = 'EBI-ETL',
    @recipients = @eMailAddress,
    @body = @eMailText,
    @subject = 'Urgent: NO record fetched from QNet.CHJobBkpHistory Source Table'
end
GO





```
## Step 5: raw_cv_mbu_configuration_migration

- Location: S:\SSIS_Packages\Data_Warehouse_Solution\Main_Control_Project\RAW_Load_Project\ ** RAW_CV_MBU_Configuration_Migration.dtsx **

- ssis package: ** udsp_RAW_CV_MBU_Configuration ** from stage one.
-![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 2.49.46 PM.png)


```sql

USE [Stage_One]
GO
/****** Object:  StoredProcedure [dbo].[udsp_RAW_CV_MBU_Configuration]    Script Date: 7/17/2019 2:51:18 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




ALTER PROC [dbo].[udsp_RAW_CV_MBU_Configuration]
--@MDate varchar(30)  
AS
/*  

=========================================================================================================
Created By:     
Created On:      
--EXEC udsp_RAW_CV_MBU_Configuration '2007-07-18',0  

Description:     
------------     
Modified on    Modified by    Description     
-----------    -----------    -------------------------------------------------------------------------  

1/21/2010    LP        Added UNION for Exchange Database to handle 'Unknown' Backup Type.    
3/3/19/2010     RV              Added isnull for the default values        
5/19/2011    DT        change source to QNET(QNET3) with openquery update      
6/14/2011    DT        updated Data_Agent to pull from AppTypeName          

---1/6/2011    DT - commented out CASE Statement for Exchange Database due to long running processes  
---          Case is set to 'unknown' for unknown backup level     
---          Solution will be implemented 1/9 - 1/13          
DT 2015-05-19 (some modification dates in qnet are NULL)     
DT 2015-05-19 (Database_Instance is some times '' and needs a default = 'defaultInstanceName')     
DT 2015-06-02 dba's requested that the original code be updated to complete openquery from source    
Ramu 2015-10-07 Added filter vlue ''Oracle RAC'' to pull this agent data.                                                                                                             
Philip Julian 2016-03-22  - Moved Exchange to separate union clause to fix DAG content issue     
Vamsi Bala       --30/05/2017--    source tables repointed to MBU API stage tables
Vamsi Bala      - 30/08/2017 --  added logic to prefix 0 for Schedule_Backup_Time
Vamsi Bala      - 06/14/2019  -- replaced union all with union to avoid duplicate data while inserting   
=========================================================================================================

*/
BEGIN
    TRUNCATE TABLE stage_one.dbo.raw_cv_mbu_configuration

    INSERT INTO stage_one.dbo.raw_cv_mbu_configuration
                ([commcell_id],
                 [schedule_name],
                 [schedule_id],
                 [schedule_task],
                 [schedule_backup_type],
                 [schedule_pattern],
                 [schedule_interval],
                 [schedule_backup_day],
                 [schedule_backup_time],
                 [schedule_next_backup_time],
                 [app_id],
                 [client_name],
                 [data_agent],
                 [database_instance],
                 [backup_set],
                 [subclient],
                 [componentnameid],
                 [filter_type],
                 [filtered_file_name],
                 [filter_create_dttm],
                 [filter_modified_dttm],
                 [storage_policy_name],
                 [copy_name],
                 [retention_days],
                 [cycles])
    SELECT Isnull(CNF.commcell_id, 0)                           AS
           [Commcell_ID]
           ,
           Isnull(CNF.schedule_name, 'N/A')                     AS
           Schedule_Name,
           Isnull(CNF.schedule_id, 0)                           AS Schedule_ID
           ,
           Isnull(CNF.schedule_task, 'Unknown')
           AS Schedule_Task
           ,
           Isnull(CNF.schedule_backup_type, 'Unknown')          AS
           Schedule_Backup_Type,
           Isnull(CNF.schedule_pattern, 'Unknown')              AS
           Schedule_Pattern,
           Isnull(CNF.schedule_interval, 'Unknown')             AS
           Schedule_Interval
           ,
           Isnull(CNF.schedule_backup_day, 'Unknown')           AS
           Schedule_Backup_Day,

           Isnull(CASE WHEN len(CNF.Schedule_Backup_Time) = 7 THEN  
     '0' + CNF.Schedule_Backup_Time
     ELSE CNF.Schedule_Backup_Time END, 'Unknown')          AS
           Schedule_Backup_Time,
    Isnull(convert(datetime, SUBSTRING(CNF.schedule_next_backup_time,6,20),121),'1900-01-01')  AS
           Schedule_Next_Backup_Time,
           Isnull(CNF.app_id, 0)                                AS App_ID,
           Isnull(CNF.client_name, 'N/A')                       AS Client_Name
           ,
           Isnull(CNF.data_agent, 'Unknown')
           AS Data_Agent,
           Isnull(CNF.database_instance, 'DefaultInstanceName') AS
           Database_Instance
           ,
           Isnull(CNF.backup_set, 'DefaultBackupSet')           AS
           Backup_Set,
           Isnull(CNF.subclient, 'All Subclients')              AS Subclient,
           Isnull(CNF.componentnameid, 0)                       AS
           componentNameId,
           Isnull(CNF.filter_type, 0)                           AS Filter_Type
           ,
           Isnull(CNF.filtered_file_name, 'N/A')
           AS
           Filtered_File_Name,
           Isnull(CNF.filter_create_dttm, 0)                    AS
           Filter_Create_DTTM,
           0 as Filter_Modified_DTTM,
           Isnull(CNF.storage_policyname, 'N/A')                AS
           Storage_Policy_Name,
           Isnull(CNF.copy_name, 'Primary')                     AS Copy_Name,
           Isnull(CNF.retention_days, 0)                        AS
           Retention_Days,
           Isnull(CNF.retentioncycles, 0)                       AS Cycles
    FROM   (SELECT SI.commcell_id,
                   schedule_name,
                   [schedule_id]              AS Schedule_ID,
                   schedule_task,
                   CASE
        WHEN schedule_backup_type=1 THEN 'Full'
        WHEN schedule_backup_type=2 THEN 'Incremental'
        WHEN schedule_backup_type=3 THEN 'Differential'
        WHEN schedule_backup_type=4 THEN 'Synthetic Full'
        WHEN schedule_backup_type=7 THEN 'Pre-Selected Backup Type'
        WHEN schedule_backup_type = 'Unknown' THEN 'Unknown'
                  END AS        
           Schedule_Backup_Type,
         schedule_pattern,
                   schedule_interval,
                   schedule_backup_day,
                   schedule_backup_time,
                   schedule_next_backup_time  AS Schedule_Next_Backup_Time,
                   SI.[app_id]                AS App_ID,
                   client_name,
                   si.data_agent              AS Data_Agent,
         CASE
                     WHEN Cast(Rtrim(Ltrim(database_instance)) AS VARCHAR(512)
                          )
                          = ''
                   THEN
                     'defaultInstanceName'
                     WHEN database_instance IS NULL THEN 'defaultInstanceName'
                     ELSE Cast(database_instance AS VARCHAR(512))
                   END                        AS Database_Instance,
         --updated DT 2015-05-19 (Database_Instance is some times '' and needs a default)
         backup_set,
                   subclient,
                   SI.componentnameid,
                   SS.savesettypeid           AS Filter_Type,
                   SS.savesetpath             AS Filtered_File_Name,
                   SS.createdTime				AS filter_create_dttm,
                   SS.modifiedTime			AS filter_modified_dttm,
                   SP.storage_policyname,
                   SP.storage_policy_copyname AS Copy_Name,
                   SP.[retention_days]        AS Retention_Days,
                   retentioncycles
            FROM   stage_one.dbo.stg_mbu_raw_sched_info SI
                   LEFT JOIN           
                   stage_one.dbo.stg_mbu_raw_chsubclient sc                              
                          ON SI.commcell_id = SC.childid
                             AND SI.app_id = SC.app_id
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_sp_retention R
                          ON SI.commcell_id = R.[childid]
                             AND SC.spname = R.[sp name]
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_filter_info FI
                          ON FI.childid = SI.commcell_id
                             AND FI.componentnameid = SC.app_id
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_savesets SS
                          ON SI.commcell_id = SS.commcellid
                             AND SI.app_id = SS.cssubclientid
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_storagepolicies SP
                          ON SI.commcell_id = SP.commcellid
                             AND SC.app_id = SP.subclientid

      WHERE  schedule_task = 'Backup'
                   AND SI.client_name <> 'qnet'
                   AND SI.data_agent NOT IN ( 'Exchange Database')
                   AND SI.client_name LIKE '%[0-9][0-9]-[0-9][0-9]%'


UNION

SELECT SI.commcell_id,
                   schedule_name,
                   [schedule_id]              AS Schedule_ID,
                   schedule_task,
                   CASE
        WHEN schedule_backup_type=1 THEN 'Full'
        WHEN schedule_backup_type=2 THEN 'Incremental'
        WHEN schedule_backup_type=3 THEN 'Differential'
        WHEN schedule_backup_type=4 THEN 'Synthetic Full'
        WHEN schedule_backup_type=7 THEN 'Pre-Selected Backup Type'
        WHEN schedule_backup_type = 'Unknown' THEN 'Unknown'
                  END AS        
           Schedule_Backup_Type,
         schedule_pattern,
                   schedule_interval,
                   schedule_backup_day,
                   schedule_backup_time,
                   schedule_next_backup_time  AS Schedule_Next_Backup_Time,
                   SI.[app_id]                AS App_ID,
                   client_name,
                   si.data_agent              AS Data_Agent,
         CASE
                     WHEN Cast(Rtrim(Ltrim(database_instance)) AS VARCHAR(512)
                          )
                          = ''
                   THEN
                     'defaultInstanceName'
                     WHEN database_instance IS NULL THEN 'defaultInstanceName'
                     ELSE Cast(database_instance AS VARCHAR(512))
                   END                        AS Database_Instance,
         --updated DT 2015-05-19 (Database_Instance is some times '' and needs a default)
         backup_set,
                   subclient,
                   SI.componentnameid,
                   SS.savesettypeid           AS Filter_Type,
                   SS.savesetpath             AS Filtered_File_Name,
                   SS.createdTime				AS filter_create_dttm,
                   SS.modifiedTime			AS filter_modified_dttm,
                   SP.storage_policyname,
                   SP.storage_policy_copyname AS Copy_Name,
                   SP.[retention_days]        AS Retention_Days,
                   retentioncycles
            FROM   stage_one.dbo.stg_mbu_raw_sched_info SI
                   LEFT JOIN           
                   stage_one.dbo.stg_mbu_raw_chsubclient sc                              
                          ON SI.commcell_id = SC.childid
                             AND SI.app_id = SC.app_id
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_sp_retention R
                          ON SI.commcell_id = R.[childid]
                             AND SC.spname = R.[sp name]
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_filter_info FI
                          ON FI.childid = SI.commcell_id
                             AND FI.componentnameid = SC.app_id
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_savesets SS
                          ON SI.commcell_id = SS.commcellid
                             AND SI.app_id = SS.cssubclientid
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_storagepolicies SP
                          ON SI.commcell_id = SP.commcellid
                             AND SC.app_id = SP.subclientid


        WHERE schedule_task = 'Backup'
          AND SI.client_name <> 'qnet'
          and SI.data_agent	IN ('SQL Server','Oracle Database')
          and SP.storage_policyname <> 'Suspended Clients'
          and Si.Schedule_Name <> 'Suspended Clients'
          and SI.client_name LIKE '%[0-9][0-9]-[0-9][0-9]%'



     ) CNF


    -- Added independent union section for Exchange since the full content for this Data_Agent is only available
    -- via the RS_Filter_Exchange view.  
    INSERT INTO stage_one.dbo.raw_cv_mbu_configuration
                ([commcell_id],
                 [schedule_name],
                 [schedule_id],
                 [schedule_task],
                 [schedule_backup_type],
                 [schedule_pattern],
                 [schedule_interval],
                 [schedule_backup_day],
                 [schedule_backup_time],
                 [schedule_next_backup_time],
                 [app_id],
                 [client_name],
                 [data_agent],
                 [database_instance],
                 [backup_set],
                 [subclient],
                 [componentnameid],
                 [filter_type],
                 [filtered_file_name],
                 [filter_create_dttm],
                 [filter_modified_dttm],
                 [storage_policy_name],
                 [copy_name],
                 [retention_days],
                 [cycles])
    SELECT Isnull(CN.commcell_id, 0)                           AS
           [Commcell_ID],
           Isnull(CN.schedule_name, 'N/A')                     AS
           Schedule_Name,
           Isnull(CN.schedule_id, 0)                           AS Schedule_ID,
           Isnull(CN.schedule_task, 'Unknown')                 AS
           Schedule_Task,
           Isnull(CN.schedule_backup_type, 'Unknown')          AS
           Schedule_Backup_Type,
           Isnull(CN.schedule_pattern, 'Unknown')              AS
           Schedule_Pattern,
           Isnull(CN.schedule_interval, 'Unknown')             AS
           Schedule_Interval,
           Isnull(CN.schedule_backup_day, 'Unknown')           AS
           Schedule_Backup_Day,
     Isnull(CASE WHEN len(CN.Schedule_Backup_Time) = 7 THEN  
     '0' + CN.Schedule_Backup_Time
     ELSE CN.Schedule_Backup_Time END, 'Unknown')          AS               
           Schedule_Backup_Time,
           Isnull(convert(datetime, SUBSTRING(CN.schedule_next_backup_time,6,20),121),'1900-01-01')  AS
           Schedule_Next_Backup_Time,
           Isnull(CN.app_id, 0)                                AS App_ID,
           Isnull(CN.client_name, 'N/A')                       AS Client_Name,
           Isnull(CN.data_agent, 'Unknown')                    AS Data_Agent,
           Isnull(CN.database_instance, 'DefaultInstanceName') AS
           Database_Instance,
           Isnull(CN.backup_set, 'DefaultBackupSet')           AS Backup_Set,
           Isnull(CN.subclient, 'All Subclients')              AS Subclient,
           Isnull(CN.componentnameid, 0)                       AS
           componentNameId,
           Isnull(CN.filter_type, 0)                           AS Filter_Type,
           Isnull(CN.filtered_file_name, 'N/A')                AS
           Filtered_File_Name
           ,
           Isnull(CN.filter_create_dttm, 0)                    AS
           Filter_Create_DTTM,
           0 as Filter_Modified_DTTM,
           Isnull(CN.storage_policyname, 'N/A')                AS
           Storage_Policy_Name,
           Isnull(CN.copy_name, 'Primary')                     AS Copy_Name,
           Isnull(CN.retention_days, 0)                        AS
           Retention_Days
           ,
           Isnull(CN.retentioncycles, 0)                       AS
           RetentionCycles
    FROM   (SELECT SI.commcell_id,
                   schedule_name,
                   [schedule_id]              AS Schedule_ID,
                   schedule_task,
                    CASE
        WHEN schedule_backup_type=1 THEN 'Full'
        WHEN schedule_backup_type=2 THEN 'Incremental'
        WHEN schedule_backup_type=3 THEN 'Differential'
        WHEN schedule_backup_type=4 THEN 'Synthetic Full'
        WHEN schedule_backup_type=7 THEN 'Pre-Selected Backup Type'
        WHEN schedule_backup_type = 'Unknown' THEN 'Unknown'
                  END AS        
           Schedule_Backup_Type,
                   schedule_pattern,
                   schedule_interval,
                   schedule_backup_day,
                   schedule_backup_time,
                   schedule_next_backup_time  AS Schedule_Next_Backup_Time,
                   SI.[app_id]                AS App_ID,
                   client_name,
                   sI.data_agent              AS Data_Agent,
                   CASE
                     WHEN Cast(Rtrim(Ltrim(database_instance)) AS VARCHAR(512)
                          )
                          = ''
                   THEN
                     'defaultInstanceName'
                     WHEN database_instance IS NULL THEN 'defaultInstanceName'
                     ELSE Cast(database_instance AS VARCHAR(512))
                   END                        AS Database_Instance,
                   --updated DT 2015-05-19 (Database_Instance is some times '' and needs a default)  
                   backup_set,
                   subclient,
                   SI.componentnameid,
                   SS.savesettypeid           AS Filter_Type,
                   SS.savesetpath             AS Filtered_File_Name,
                   SS.createdTime				AS filter_create_dttm,
                   SS.modifiedTime			AS filter_modified_dttm,
                   SP.storage_policyname,
                   SP.storage_policy_copyname AS Copy_Name,
                   [retention_days]           AS Retention_Days,
                   retentioncycles
            FROM   stage_one.dbo.stg_mbu_raw_sched_info SI

         LEFT JOIN stage_one.dbo.stg_mbu_raw_chsubclient sc                               
                          ON SI.commcell_id = SC.childid
                             AND SI.app_id = SC.app_id
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_sp_retention R
                          ON SI.commcell_id = R.[childid]
                             AND SC.spname = R.[sp name]
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_savesets SS
                          ON SI.commcell_id = SS.commcellid
                             AND SI.app_id = SS.cssubclientid
                   LEFT JOIN stage_one.dbo.stg_mbu_raw_storagepolicies SP
                          ON SI.commcell_id = SP.commcellid
                             AND SC.app_id = SP.subclientid
            WHERE  schedule_task = 'Backup'
                   AND SI.client_name <> 'qnet'
                   --                   AND Isnull(filter_modified_dttm, 0) = 0  
                   ---and modified = 0 updated DT 2015-05-19 (some modification dates in qnet are NULL)  
                   AND client_name LIKE '%[0-9][0-9]-[0-9][0-9]%'
                   AND SI.data_agent LIKE 'Exchange%') CN

    --added 1/16/2013 - DT (active and inactive configurations are being reported--- only pull active)  
    --get inactive configs from qnet that should not be reported  
    DECLARE @exclude_configs AS TABLE
      (
         commcell_id VARCHAR(100),
         client_name VARCHAR(100),
         active_flag VARCHAR(100)
      )

    INSERT INTO @exclude_configs
    SELECT commcellid,
           clientname,
           activeflag
    FROM
    --Stage_one.dbo.RAW_CV_MBU_Configuration a    
    stage_one.dbo.stg_mbu_raw_cv_mbu_rs_idata
    --on a.copy_name = b.csName  
    WHERE  activeflag != 'Installed'

    --delete from stage_table only Primary Copy    
    DELETE FROM a
    FROM   stage_one.dbo.raw_cv_mbu_configuration a
           --WHERE  a.copy_name = 'primary'  
           --and active_flag  = 0  
           INNER JOIN @exclude_configs b
                   ON a.commcell_id = b.commcell_id
                      AND a.client_name = b.client_name
                      AND a.copy_name = 'primary'
                      AND b.active_flag != 'Installed';


--- added 08/04/2017 - Vamsi Bala (to delete any duplicate records loaded)

WITH raw_cv_configuration
AS (
SELECT
  Commcell_ID, Schedule_ID,App_ID, Client_Name, Data_Agent, Backup_Set, Subclient, componentNameId, Filter_Type, Filtered_File_Name, Filter_Create_DTTM,
Filter_Modified_DTTM, Storage_Policy_Name,
    row_number() OVER (
    PARTITION BY Commcell_ID, Schedule_ID,App_ID, Client_Name, Data_Agent, Backup_Set, Subclient, componentNameId, Filter_Type, Filtered_File_Name, Filter_Create_DTTM,
Filter_Modified_DTTM, Storage_Policy_Name
     ORDER BY Schedule_ID
    ) AS duplicaterownum

FROM stage_one.dbo.[RAW_CV_MBU_Configuration]
)

DELETE FROM raw_cv_configuration where duplicaterownum > 1;

END






```

## Step 6: update_raw_cv_configuation on retry


-![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 4.07.19 PM.png)

## Step 7: RAW_CV_MBU_rs_idata
- Location: S:\SSIS_Packages\Data_Warehouse_Solution\Main_Control_Project\RAW_Load_Project\ ** RAW_CV_MBU_rs_idata.dtsx **


-![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 4.09.42 PM.png)
-SSIS package: udsp_raw_cv_mbu_rs_idata

```sql

USE [Stage_One]
GO
/****** Object:  StoredProcedure [dbo].[udsp_RAW_CV_MBU_rs_idata]    Script Date: 7/17/2019 4:10:51 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO








ALTER PROC [dbo].[udsp_RAW_CV_MBU_rs_idata]  

AS  


begin  
/*  
=========================================================================================================  
Created By:  david teniente  
Created On:  8/18/2009  

Description:  
------------  
load stage_one raw table from MBU API stage table: raw_cv_mbu_rs_idata  
then,  
load reference table in stage_three_dwmaint: ref_mbu_rs_idata  


Modified on  Modified by  Description  
-----------  -----------  -------------------------------------------------------------------------  
2011-05-17  DT    Switch Link Server from QNET2 "10.5.19.158\COMMVAULTQINETIX"  
        TO QNET3                                                                                                                                                                                                                                               

2012-01-25	DP				Added the  client name '.575058-400993' for exclusion as the data is incorrect.                                   
2012-02-13  LP			   Excluded client name like 'capplan-template%'
2012-02-14	DP				Excluded clientName .199283-200263 and '407531-thunderbolt%' as the data is not in correct format
2012-06-06	SC				Excluded clientName .24602-224661 as the data is not in correct format
2017-05-19  Vamsi Bala		Repointed the source to read data from new raw tables loaded from MBU API
udsp_RAW_cv_mbu_rs_idata  
=========================================================================================================  
*/  

------------------------------------------------------------  
--Truncate raw table in stage_one then insert data from QNET  
------------------------------------------------------------  
truncate table  stage_one.dbo.raw_cv_mbu_rs_idata  


 insert into stage_one.dbo.raw_cv_mbu_rs_idata  
  (  
  clientName,  
  appTypeName,  
  activeFlag,  
  csName  
  )  

 select  distinct
  s.clientName,  
  s.appTypeName,
  CASE WHEN S.ACTIVEFLAG = 'INSTALLED' THEN 1 ELSE 0 END  
  activeFlag,  
  s.csName  
 from (select clientName ,appTypeName  ,activeFlag  ,csName from stage_one.dbo.Stg_MBU_RAW_CV_MBU_rs_idata) s
 where(clientName not like 'capplan-template%' and clientName not like '407531-thunderbolt%')



------------------------------------------------------------  
--Truncate ref table in stage_three_dwmaint then insert data from raw  
------------------------------------------------------------  
delete from stage_three_dwmaint.dbo.ref_MBU_rs_idata  

insert into stage_three_dwmaint.dbo.ref_MBU_rs_idata  
 (  
 clientName,  
 account_number,  
 device_number,  
 appTypeName,  
 activeFlag,  
 csName  
 )  

select  
 clientName,  
 case  
     when PATINDEX('%-%',clientName)> 0 then Left(clientName,PATINDEX('%-%',clientName)-1)  
  else 0  
 end as account_number,  
 case  
  when PATINDEX('%-%',clientName) > 0 then (Right(clientName, Len(clientName) - PATINDEX('%-%',clientName)))  
  else 0  
 end as device_nubmer,  
 appTypeName,  
 activeFlag,  
 csName  
from  
 [stage_one].[dbo].[raw_cv_mbu_rs_idata]  
WHERE  
 isnumeric(left(clientName,4)) = 1  
and isnumeric(Right(clientName, Len(clientName) - PATINDEX('%-%',clientName))) = 1  
and clientName <> '.575058-400993' -- DP excluded 25012012
and clientName <> '.199283-200263' -- DP excluded 02142012
and clientName <> '.24602-224661'  -- SC excluded 06062012


end  


```

## Step 8: raw_cv_mbu_rs_filter_mssql

Location:
 - S:\SSIS_Packages\Data_Warehouse_Solution\Main_Control_Project\RAW_Load_Project\ ** RAW_CV_MBU_rs_filter_mssql.dtsx **

 - SSIS Package: udsp_raw_cv_mbu_rs_filter_mssql

 -![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 4.15.55 PM.png)


 ```sql

 USE [Stage_One]
 GO
 /****** Object:  StoredProcedure [dbo].[udsp_RAW_CV_MBU_rs_filter_mssql]    Script Date: 7/17/2019 4:17:12 PM ******/
 SET ANSI_NULLS ON
 GO
 SET QUOTED_IDENTIFIER ON
 GO





 ALTER PROC [dbo].[udsp_RAW_CV_MBU_rs_filter_mssql]

 AS

 begin
 /*
 =========================================================================================================
 Created By:  david teniente
 Created On:  8/18/2009

 Description:
 ------------
 load raw table from commvault qnet: raw_cv_mbu_rs_filter_mssql
 then,
 load reference table in stage_three_dwmaint: ref_mbu_rs_filter_mssql


 Modified on		Modified by		Description
 -----------		-----------		-------------------------------------------------------------------------
 2011-05-17		DT				Switch Link Server from QNET2 "10.5.19.158\COMMVAULTQINETIX"
 								TO QNET3                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               

 2017-05-19	    Vamsi Bala		Read data from raw stage tables loaded from MBU API instead of QNET link server

 udsp_RAW_cv_mbu_rs_filter_mssql
 =========================================================================================================
 */

 ------------------------------------------------------------
 --Truncate raw table in stage_one then insert data from QNET
 ------------------------------------------------------------


 truncate table stage_one.dbo.raw_cv_mbu_rs_filter_mssql

 insert into stage_one.dbo.raw_cv_mbu_rs_filter_mssql
 		(
 		childid,
 		componentNameId,
 		[type],
 		dbName,
 		created,
 		modified,
 		client,
 		agent,
 		instance,
 		backupset,
 		[name]
 		)

 	select
 		distinct
 		FM.childid,
 		SS.csSubclientId,
 		case
 		when SS.savesetType = 'content' then 1
 		when SS.savesetType = 'Except' then 2
 		when SS.savesetType = 'Exclude' then 3
 		else NULL
 		end as savesetType,
 		SS.savesetPath,
 		SS.createdTime,
 		modified,
 		client,
 		agent,
 		instance,
 		backupset,
 		[name]
 	from [dbo].[Stg_MBU_RAW_CV_MBU_rs_filter_mssql] FM
 	left outer join stage_one.dbo.Stg_MBU_RAW_SaveSets SS
 				on FM.childid= SS.commcellId
 				and FM.client =SS.clientname
 				and FM.componentNameId=SS.csSubclientId
 				where SS.savesetCategory like '%SQL Server Content%'
 ----------------------------------------------------------------------
 --Truncate ref table in stage_three_dwmaint then insert data from raw
 ----------------------------------------------------------------------

 delete  from  stage_three_dwmaint.dbo.ref_MBU_rs_filter_mssql

 insert into stage_three_dwmaint.dbo.ref_MBU_rs_filter_mssql
 	(
 	childid,
 	componentNameId,
 	[type],
 	dbName,
 	created,
 	modified,
 	client,
 	account_number,
 	device_number,
 	agent,
 	instance,
 	backupset,
 	[name]
 	)

 select
 	distinct
 	childid,
 	componentNameId,
 	[type],
 	dbName,
 	created,
 	modified,
 	client,
 	case
    		when PATINDEX('%-%',client)> 0 then Left(client,PATINDEX('%-%',client)-1)
 		else 0
 	end	as account_number,
 	case
 		when PATINDEX('%-%',client) > 0 then (Right(client, Len(client) - PATINDEX('%-%',client)))
 		else 0
 	end as device_number,
 	agent,
 	instance,
 	backupset,
 	[name]
 from
 	[stage_one].[dbo].[raw_cv_mbu_rs_filter_mssql]
 WHERE
 	isnumeric(left(client,4)) = 1
 and isnumeric(Right(client, Len(client) - PATINDEX('%-%',client))) = 1

 end

 ```


## Step 9: Check for oracle job completion


-![MBU Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-17 at 5.09.01 PM.png)


## Step 10: Dwmaint load managed backup

- Location:   S:\SSIS_Packages\Data_Warehouse_Solution\Main_Control_Project\DWMAINT_Load_Project\** DWMaint Load Managed Backup.dtsx **

** Steps involved sequentially: **

- DWMaint_WRK_CV_Child
\Package\DWMaint Managed Backup Container\DWMaint_WRK_CV_Child



![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-16 at 2.46.25 PM.png)

Path:


\Package\DWMaint Managed Backup Container\DWMaint_WRK_CV_Child


![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-16 at 5.24.35 PM.png)


Populate WRK child

DW_maint table is intended to be used to populate WRK_CV_Child table.

```sql

BEGIN TRAN

 -- insert new records found in the source table
INSERT stage_two_dwMaint.dbo.WRK_CV_Child
SELECT
	commcellId,
	2 as QNetId,
	commservSNetIP as QNetInterfaceName,
	commcellName,
	commservMgmtIP,
	commcellName as Description,
	getdate()
FROM stage_one.dbo.[Stg_MBU_RAW_COMMCELLS] c
WHERE NOT EXISTS
(
SELECT 'x'
FROM stage_two_dwMaint.dbo.WRK_CV_Child
WHERE ChildId = c.commcellid
--AND	QNetId = c.QNetId
AND	QNetInterfaceName = c.commservSNetIP
AND	DisplayName = c.commcellName
AND	InterfaceName = c.commservMgmtIP
AND	ISNULL(Description,'X') = ISNULL(c.commcellName,'X')
);

-- delete records no longer in the source table
DELETE stage_two_dwMaint.dbo.WRK_CV_Child
FROM  stage_two_dwMaint.dbo.WRK_CV_Child cvc
WHERE NOT EXISTS
(
SELECT 'x'
FROM stage_one.dbo.[Stg_MBU_RAW_COMMCELLS]
WHERE commcellid = cvc.ChildId
--AND	QNetId = cvc.QNetId
AND	commservSNetIP = cvc.QNetInterfaceName
AND	commcellName = cvc.DisplayName
AND	commservMgmtIP = cvc.InterfaceName
AND	ISNULL(commcellName,'X') = ISNULL(cvc.Description,'X')
);

COMMIT TRAN

```



- DWMaint_WRK_RS_Global_Filters_Lookup
 Locaation: \Package\DWMaint Managed Backup Container\DWMaint_WRK_RS_Global_Filters_Lookup


 ![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.25.21 PM.png)

```sql

 BEGIN TRAN

DELETE WRK_RS_Global_Filters_Lookup
GO
SELECT componentNameID,
 ChildID,
 GlobalFilters
 INTO #INTER_WRK_RS_Global_Filters_Lookup FROM stage_one.[dbo].[Stg_MBU_RS_Global_Filters_Lookup] Q

INSERT WRK_RS_Global_Filters_Lookup

SELECT componentNameID,
 ChildID,
 GlobalFilters,
 GETDATE() RECORD_CREATED_DATE FROM #INTER_WRK_RS_Global_Filters_Lookup

 DROP TABLE #INTER_WRK_RS_Global_Filters_Lookup


GO

COMMIT TRAN

 ```



- ref MBU Overage Fee History xref_M  222

\Package\DWMaint Managed Backup Container\ref MBU Overage Fee History xref_M  222


 ![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 12.25.21 PM.png)



- ref MBU Overage Fee xref_M 223
\Package\DWMaint Managed Backup Container\ref MBU Overage Fee xref_M 223
- Stored Procedure : EXEC udsp_ref_ETL_MBU_Overage_Fee_History_xref_M




 ![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.14.33 PM.png)


 ```sql


 USE [Stage_Two_DWMaint]
GO
/****** Object:  StoredProcedure [dbo].[udsp_ref_ETL_MBU_Overage_Fee_History_xref_M]    Script Date: 7/18/2019 2:22:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

ALTER  PROC [dbo].[udsp_ref_ETL_MBU_Overage_Fee_History_xref_M]
AS
/*
===========================================================================================
Created By:
Created On:

Description:
------------
Loads Curent Month default Overage amount for Managed and intensive segments.
===========================================================================================
*/
----------------------------------------------------------------------------
DECLARE @Date as DateTime
DECLARE @Time_Month_Key As int
----------------------------------------------------------------------------
SET @Date =Stage_Three_DW.dbo.udf_FirstDayOfMonth(getdate())
SET @Time_Month_Key = (SELECT Time_Month_KEY FROM Stage_Three_DW.dbo.Dim_Time_Month WHERE Stage_Three_DW.dbo.udf_YearMonth_NoHyphen(@Date)=Time_Month_KEY)
---------------------------------------------------------------------------------------------
---
-- Load it
---
SELECT DISTINCT
	@Time_Month_Key			AS Time_Month_Key,
	2				AS Business_Segment_ID, --Managed
	1.00				AS Overage_Fee
INTO
 	#MBU_Overage_Fee_Xref
------
UNION
------
SELECT
	@Time_Month_Key			AS Time_Month_Key,
	1				AS Business_Segment_ID, --Intensive
	2.50				AS Overage_Fee
----------------------------------------------------
Truncate table Stage_Two_DWMAINT.dbo.WRK_ref_MBU_Overage_Fee_History_xref

INSERT INTO Stage_Two_DWMAINT.dbo.WRK_ref_MBU_Overage_Fee_History_xref
	(
	Time_Month_Key, Business_Segment_ID, Overage_Fee
	)
SELECT
	Time_Month_Key,
	Business_Segment_ID,
	Overage_Fee
FROM
	#MBU_Overage_Fee_Xref
WHERE
	Time_Month_Key NOT IN (SELECT MAX(Time_Month_Key) FROM Stage_Three_DWMAINT.dbo.ref_MBU_Overage_Fee_History_xref)
-------------------------------------------------------------------------------------------
DROP TABLE #MBU_Overage_Fee_Xref
-------------------------------------------------------------------------------------------


--EXEC udsp_ref_ETL_MBU_Overage_Fee_History_xref_M



--select * from
--Stage_Two_DWMAINT.dbo.WRK_ref_MBU_Overage_Fee_History_xref


 ```


  ![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.27.16 PM.png)

 ![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.27.24 PM.png)

  ![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.27.31 PM.png)


   ![api2 Jobs](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.27.36 PM.png)



- DWMaint Managed Backup Container
\Package\DWMaint Managed Backup Container


## Step 11: dim_managed_backup_level_m


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.39.38 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.39.47 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.40.25 PM.png)

## Step 12: dim_managed_backup_status_m



Source: stage two DW

```sql

select distinct backup_status_name,
getdate() as effective_start,
'9999-12-31' as effective_end,
1 as current_record
from wrk_backup_status
where
backup_status_name not in (select managed_backup_status_name from
stage_three_dw.dbo.dim_managed_backup_status)

```

Goes into stage three.

as table dim_managed_backup_status table  which contains below columns.




output:

![api1](/Users/amog2898/Desktop/dim_managed_backup_status_m_mapping.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-16 at 2.25.39 PM.png)

![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-16 at 2.27.14 PM.png)


## Step 13: dim_managed_backup_server_m


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.44.58 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.45.09 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.45.14 PM.png)

## Step 14: dim_managed_backup_target_m


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-16 at 2.38.35 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-16 at 2.38.41 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-16 at 2.27.14 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-16 at 2.36.16 PM.png)


## Step 15: dim_managed_backup_cv_config_m

![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.49.47 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.50.17 PM.png)



![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.55.40 PM.png)




```sql
SELECT DISTINCT
	cast(isnull(
	isnull(cast(Commcell_ID as varchar(10)), 'N/A') +
	'-' + isnull(RTRIM(LTRIM(Data_Agent)),'N/A') +
	'-' + isnull(RTRIM(LTRIM(Backup_SET)),'N/A') +
	'-' + CASE WHEN isnull(RTRIM(LTRIM(Database_Instance)),'N/A') = ''
		       THEN 'N/A'
		       ELSE isnull(RTRIM(LTRIM(Database_Instance)),'N/A')
	      END +
	'-' + isnull(RTRIM(LTRIM(SubClient)),'N/A') +
	'-' + isnull(RTRIM(LTRIM(Filtered_File_Name)),'N/A') +
	'-' + cast(isnull(Filter_Type,-1) as varchar(10)) + 	
	'-' + isnull(Copy_Name,'N/A') +
	'-' + isnull(Storage_Policy_Name,'N/A') +
	'-' + isnull(Schedule_Backup_Time,'N/A') +
	'-' + isnull(Schedule_Name,'N/A') +
	'-' + isnull(Schedule_Backup_Type,'N/A') +
	'-' + isnull(Schedule_Backup_Day,'N/A') +
	'-' + cast(isnull(Retention_Days,-1) as varchar(10)),0) AS varchar(8000))  AS MBU_Config_NK
	,cast(CASE
		WHEN Filter_Type IN (1, 2) ---- Update 10/26/2007 DT: Include the file path if it is an Inclussion and Exceptions / was = 1
		THEN LTRIM(Data_Agent) + ' ' + isnull(RTRIM(LTRIM(Database_Instance)),'')
			+ ' ' + RTRIM(LTRIM(Backup_SET)) + ' ' + RTRIM(LTRIM(SubClient)) + ' - '
			+ RTRIM(LTRIM(Filtered_File_Name))
		ELSE LTRIM(Data_Agent) + ' ' + isnull(RTRIM(LTRIM(Database_Instance)),'')
			+ ' ' + RTRIM(LTRIM(Backup_SET)) + ' ' + RTRIM(LTRIM(SubClient))
	    END     as varchar(8000))                                               AS MBU_Inclusion_LST
	,cast(LTRIM(Data_Agent) as varchar(255))									AS MBU_Data_Agent
	,CASE
		WHEN cast(isnull(RTRIM(LTRIM(Database_Instance)),'N/A') as varchar(255)) = ''
		THEN 'N/A'
		ELSE cast(isnull(RTRIM(LTRIM(Database_Instance)),'N/A') as varchar(255))
	 END																		AS MBU_Database_Instance
	,cast(RTRIM(LTRIM(Backup_SET)) as varchar(255))								AS MBU_Backup_SET
	,cast(RTRIM(LTRIM(SubClient)) as varchar(255))								AS MBU_SubClient
	,Case
		WHEN Filter_Type IN (1,2) --ADD CASE 10/26/2007 DT: Include the file path if it is an Inclussion and Exceptions
		THEN cast(isnull(RTRIM(LTRIM(Filtered_File_Name)),'N/A') as varchar(255))
		ELSE 'N/A'
	END																			AS MBU_Filtered_File_Name
	,cast(CASE
			WHEN Filter_Type = 3 then ISNULL(RTRIM(LTRIM(Filtered_File_Name)), 'N/A') --Updated Filter_Type from 2 to 3 (2 = Exceptions, 3 = Exclusion)
			ELSE 'N/A' --Update No Enclusions to N/A
	     END as varchar(8000))  						    AS MBU_Exclusion_LST
	, isnull(Retention_Days, 14)							AS MBU_Days_Retained_NBR
	, isnull(Retention_Days, 14)							AS MBU_Rtntn_Period_NBR
	, 'Days'                                                AS MBU_Rtntn_Period_TXT
	, isnull(Storage_Policy_Name,'N/A')						AS MBU_Rtntn_SrcSys_TXT
	, left(replace(Schedule_Backup_Time, ':',''),4)			AS MBU_Scheduled_TM
	, CASE
		WHEN Storage_Policy_Name like '%Offsite%'
		THEN 1
		ELSE 0
	END														AS MBU_Send_Offsite_FLG
	, isnull(Schedule_Name, 'N/A')                          AS MBU_Group_SrcSys_TXT
	, CASE
		WHEN Schedule_Backup_Type in ('Differential', 'Incremental')
		THEN 'N/A'
		WHEN Schedule_Backup_Type = ('Full') and Schedule_Pattern = ('Daily')
		THEN 'All Fulls'
		Else cast(Schedule_Backup_Day as varchar(50))
	  END													AS MBU_Full_BU_DAY
	, CASE
		WHEN Schedule_Backup_Type = 'Differential'
		THEN 'DIFFERENTIAL'
		WHEN Schedule_Backup_Type = 'Incremental'
		THEN 'INCREMENTAL'
		ELSE 'N/A'
	  END                                                   AS MBU_NonFull_TYP
	, isnull(Schedule_Backup_Day, 'N/A')					AS MBU_Schedule_SrcSys_TXT
	, isnull(Commcell_ID, 'N/A')							AS MBU_Backup_Server_NM
	, 'Commvault'               							AS MBU_Config_SrcSys_TXT
	, isnull(cast(Copy_Name as varchar(100)),'N/A')			AS MBU_Copy_Name
	, isnull(cast(Filter_Modified_DTTM as int),-999999)		AS MBU_Modified_DTTM
	, getdate()												AS Rec_Added_DTTM
	, getdate()												AS Rec_Updated_DTTM
	,CASE
		WHEN MBUC.Data_Agent LIKE '%Linux File System%'
			THEN 'LINUX'
		 WHEN MBUC.Data_Agent LIKE '%Windows%File System%'
			THEN 'WINDOWS'
		   ELSE 'None' END	                                AS MBU_Linux_Windows
FROM
	Stage_One.dbo.RAW_CV_MBU_Configuration MBUC
LEFT OUTER JOIN
	Stage_Two_DWMaint.dbo.WRK_RS_Global_Filters_Lookup rsgflu
ON MBUC.componentNameId = rsgflu.componentNameId
AND MBUC.Commcell_ID = rsgflu.ChildID
WHERE isnumeric( SUBSTRING(Client_Name,PATINDEX('%-%',Client_Name) + 1, 6) ) = 1
```



![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.52.03 PM.png)


![api1](/Users/amog2898/Desktop/Screen Shot 2019-07-18 at 2.52.08 PM.png)

- truncate TRUNCATE TABLE Stage_Two_DW.dbo.WRK_CV_MBU_Config


- Source from raw tables

```sql



SELECT DISTINCT
	cast(isnull(
	isnull(cast(Commcell_ID as varchar(10)), 'N/A') +
	'-' + isnull(RTRIM(LTRIM(Data_Agent)),'N/A') +
	'-' + isnull(RTRIM(LTRIM(Backup_SET)),'N/A') +
	'-' + CASE WHEN isnull(RTRIM(LTRIM(Database_Instance)),'N/A') = ''
		       THEN 'N/A'
		       ELSE isnull(RTRIM(LTRIM(Database_Instance)),'N/A')
	      END +
	'-' + isnull(RTRIM(LTRIM(SubClient)),'N/A') +
	'-' + isnull(RTRIM(LTRIM(Filtered_File_Name)),'N/A') +
	'-' + cast(isnull(Filter_Type,-1) as varchar(10)) + 	
	'-' + isnull(Copy_Name,'N/A') +
	'-' + isnull(Storage_Policy_Name,'N/A') +
	'-' + isnull(Schedule_Backup_Time,'N/A') +
	'-' + isnull(Schedule_Name,'N/A') +
	'-' + isnull(Schedule_Backup_Type,'N/A') +
	'-' + isnull(Schedule_Backup_Day,'N/A') +
	'-' + cast(isnull(Retention_Days,-1) as varchar(10)),0) AS varchar(8000))  AS MBU_Config_NK
	,cast(CASE
		WHEN Filter_Type IN (1, 2) ---- Update 10/26/2007 DT: Include the file path if it is an Inclussion and Exceptions / was = 1
		THEN LTRIM(Data_Agent) + ' ' + isnull(RTRIM(LTRIM(Database_Instance)),'')
			+ ' ' + RTRIM(LTRIM(Backup_SET)) + ' ' + RTRIM(LTRIM(SubClient)) + ' - '
			+ RTRIM(LTRIM(Filtered_File_Name))
		ELSE LTRIM(Data_Agent) + ' ' + isnull(RTRIM(LTRIM(Database_Instance)),'')
			+ ' ' + RTRIM(LTRIM(Backup_SET)) + ' ' + RTRIM(LTRIM(SubClient))
	    END     as varchar(8000))                                               AS MBU_Inclusion_LST
	,cast(LTRIM(Data_Agent) as varchar(255))									AS MBU_Data_Agent
	,CASE
		WHEN cast(isnull(RTRIM(LTRIM(Database_Instance)),'N/A') as varchar(255)) = ''
		THEN 'N/A'
		ELSE cast(isnull(RTRIM(LTRIM(Database_Instance)),'N/A') as varchar(255))
	 END																		AS MBU_Database_Instance
	,cast(RTRIM(LTRIM(Backup_SET)) as varchar(255))								AS MBU_Backup_SET
	,cast(RTRIM(LTRIM(SubClient)) as varchar(255))								AS MBU_SubClient
	,Case
		WHEN Filter_Type IN (1,2) --ADD CASE 10/26/2007 DT: Include the file path if it is an Inclussion and Exceptions
		THEN cast(isnull(RTRIM(LTRIM(Filtered_File_Name)),'N/A') as varchar(255))
		ELSE 'N/A'
	END																			AS MBU_Filtered_File_Name
	,cast(CASE
			WHEN Filter_Type = 3 then ISNULL(RTRIM(LTRIM(Filtered_File_Name)), 'N/A') --Updated Filter_Type from 2 to 3 (2 = Exceptions, 3 = Exclusion)
			ELSE 'N/A' --Update No Enclusions to N/A
	     END as varchar(8000))  						    AS MBU_Exclusion_LST
	, isnull(Retention_Days, 14)							AS MBU_Days_Retained_NBR
	, isnull(Retention_Days, 14)							AS MBU_Rtntn_Period_NBR
	, 'Days'                                                AS MBU_Rtntn_Period_TXT
	, isnull(Storage_Policy_Name,'N/A')						AS MBU_Rtntn_SrcSys_TXT
	, left(replace(Schedule_Backup_Time, ':',''),4)			AS MBU_Scheduled_TM
	, CASE
		WHEN Storage_Policy_Name like '%Offsite%'
		THEN 1
		ELSE 0
	END														AS MBU_Send_Offsite_FLG
	, isnull(Schedule_Name, 'N/A')                          AS MBU_Group_SrcSys_TXT
	, CASE
		WHEN Schedule_Backup_Type in ('Differential', 'Incremental')
		THEN 'N/A'
		WHEN Schedule_Backup_Type = ('Full') and Schedule_Pattern = ('Daily')
		THEN 'All Fulls'
		Else cast(Schedule_Backup_Day as varchar(50))
	  END													AS MBU_Full_BU_DAY
	, CASE
		WHEN Schedule_Backup_Type = 'Differential'
		THEN 'DIFFERENTIAL'
		WHEN Schedule_Backup_Type = 'Incremental'
		THEN 'INCREMENTAL'
		ELSE 'N/A'
	  END                                                   AS MBU_NonFull_TYP
	, isnull(Schedule_Backup_Day, 'N/A')					AS MBU_Schedule_SrcSys_TXT
	, isnull(Commcell_ID, 'N/A')							AS MBU_Backup_Server_NM
	, 'Commvault'               							AS MBU_Config_SrcSys_TXT
	, isnull(cast(Copy_Name as varchar(100)),'N/A')			AS MBU_Copy_Name
	, isnull(cast(Filter_Modified_DTTM as int),-999999)		AS MBU_Modified_DTTM
	, getdate()												AS Rec_Added_DTTM
	, getdate()												AS Rec_Updated_DTTM
	,CASE
		WHEN MBUC.Data_Agent LIKE '%Linux File System%'
			THEN 'LINUX'
		 WHEN MBUC.Data_Agent LIKE '%Windows%File System%'
			THEN 'WINDOWS'
		   ELSE 'None' END	                                AS MBU_Linux_Windows
FROM
	Stage_One.dbo.RAW_CV_MBU_Configuration MBUC
LEFT OUTER JOIN
	Stage_Two_DWMaint.dbo.WRK_RS_Global_Filters_Lookup rsgflu
ON MBUC.componentNameId = rsgflu.componentNameId
AND MBUC.Commcell_ID = rsgflu.ChildID
WHERE isnumeric( SUBSTRING(Client_Name,PATINDEX('%-%',Client_Name) + 1, 6) ) = 1

```




Stage two DW
```sql

SELECT
       A.[MBU_Inclusion_LST]
      ,A.[MBU_Exclusion_LST]
      ,A.[MBU_Days_Retained_NBR]
      ,A.[MBU_Rtntn_Period_NBR]
      ,A.[MBU_Rtntn_Period_TXT]
      ,A.[MBU_Rtntn_SrcSys_TXT]
      ,A.[MBU_Scheduled_TM]
      ,A.[MBU_Send_Offsite_FLG]
      ,A.[MBU_Group_SrcSys_TXT]
      ,A.[MBU_Full_BU_DAY]
      ,A.[MBU_NonFull_TYP]
      ,A.[MBU_Schedule_SrcSys_TXT]
      ,A.[MBU_Config_SrcSys_TXT]
      ,A.[MBU_Backup_Server_NM]
      ,A.[Rec_Added_DTTM]
      ,A.[Rec_Updated_DTTM]
      ,A.[MBU_Data_Agent]
      ,A.[MBU_Database_Instance]
      ,A.[MBU_Backup_SET]
      ,A.[MBU_SubClient]
      ,A.[MBU_Filtered_File_Name]
      ,A.[MBU_Copy_Name]
      ,A.[MBU_Modified_DTTM]
      ,A.[MBU_Config_NK]
  FROM Stage_Two_DW.dbo.WRK_CV_MBU_Config A
LEFT OUTER JOIN
	Stage_Three_DW.dbo.Dim_Managed_Backup_Config B
ON A.MBU_Config_NK =B.MBU_Config_NK
AND B.Current_Record =1
WHERE B.MBU_Config_NK IS NULL
```

push data into stage three dw -> dim_managed_backup_config


```sql

UPDATE
	A
SET Type_Change ='Type2'
FROM
	Stage_Two_DW.dbo.WRK_CV_MBU_Config A
INNER JOIN
	Stage_Three_DW.dbo.Dim_Managed_Backup_Config B
ON A.MBU_Config_NK =B.MBU_Config_NK
WHERE B.Current_Record =1
AND (A.[MBU_Inclusion_LST] != B.[MBU_Inclusion_LST]
	 OR A.[MBU_Exclusion_LST] != B.[MBU_Exclusion_LST]
	 OR A.[MBU_Days_Retained_NBR] != B.[MBU_Days_Retained_NBR]
	 OR A.[MBU_Rtntn_Period_NBR] != B.[MBU_Rtntn_Period_NBR]
	 OR A.[MBU_Rtntn_Period_TXT] != B.[MBU_Rtntn_Period_TXT]
	 OR A.[MBU_Rtntn_SrcSys_TXT] != B.[MBU_Rtntn_SrcSys_TXT]
	 OR A.[MBU_Scheduled_TM] != B.[MBU_Scheduled_TM]
	 OR A.[MBU_Send_Offsite_FLG] != B.[MBU_Send_Offsite_FLG]
	 OR A.[MBU_Group_SrcSys_TXT] != B.[MBU_Group_SrcSys_TXT]
	 OR A.[MBU_Full_BU_DAY] != B.[MBU_Full_BU_DAY]
	 OR A.[MBU_NonFull_TYP] != B.[MBU_NonFull_TYP]
	 OR A.[MBU_Schedule_SrcSys_TXT] != B.[MBU_Schedule_SrcSys_TXT]
	 OR A.[MBU_Config_SrcSys_TXT] != B.[MBU_Config_SrcSys_TXT]
	 OR A.[MBU_Backup_Server_NM] != B.[MBU_Backup_Server_NM]
	 OR A.[MBU_Data_Agent] != B.[MBU_Data_Agent]
	 OR A.[MBU_Database_Instance] != B.[MBU_Database_Instance]
	 OR A.[MBU_Backup_SET] != B.[MBU_Backup_SET]
	 OR A.[MBU_SubClient] != B.[MBU_SubClient]
	 OR A.[MBU_Filtered_File_Name] != B.[MBU_Filtered_File_Name]
	 OR A.[MBU_Copy_Name] != B.[MBU_Copy_Name]
	 OR A.[MBU_Modified_DTTM] != B.[MBU_Modified_DTTM]
	)

```



```sql

SELECT
       A.[MBU_Inclusion_LST]
      ,A.[MBU_Exclusion_LST]
      ,A.[MBU_Days_Retained_NBR]
      ,A.[MBU_Rtntn_Period_NBR]
      ,A.[MBU_Rtntn_Period_TXT]
      ,A.[MBU_Rtntn_SrcSys_TXT]
      ,A.[MBU_Scheduled_TM]
      ,A.[MBU_Send_Offsite_FLG]
      ,A.[MBU_Group_SrcSys_TXT]
      ,A.[MBU_Full_BU_DAY]
      ,A.[MBU_NonFull_TYP]
      ,A.[MBU_Schedule_SrcSys_TXT]
      ,A.[MBU_Config_SrcSys_TXT]
      ,A.[MBU_Backup_Server_NM]
      ,A.[Rec_Added_DTTM]
      ,A.[Rec_Updated_DTTM]
      ,A.[MBU_Data_Agent]
      ,A.[MBU_Database_Instance]
      ,A.[MBU_Backup_SET]
      ,A.[MBU_SubClient]
      ,A.[MBU_Filtered_File_Name]
      ,A.[MBU_Copy_Name]
      ,A.[MBU_Modified_DTTM]
      ,A.[MBU_Config_NK]  
FROM Stage_Two_DW.dbo.WRK_CV_MBU_Config A
WHERE A.Type_Change ='Type2'

```

## Step 16: dim_mbu_exclusions
## Step 17: fact_cv_mbu_config_history_m
## Step 18: udsp_report_mbu_increased_backup
## Step 19: fact_cv_mbu_config_history_m
## Step 20: execute fact_mbu_config_current sporc







## **DIM to API mapping**
 - Dims

 1. dim_mananged_backup_level_m
 2. dim_managed_backup_status_m
 3. dim_managed_backup_server_m
 4. dim_managed_backup_target_m
 5. dim_managed_backup_cv_config_m
 6. dim_mbu_exclusions

 - Facts
 1. fact_cv_managed_backup_detail_m
 2. fact_cv_mbu_config_history_m
