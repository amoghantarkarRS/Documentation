# Monitoring ETL Workflow:

![Monitoring ETL](/Users/amog2898/Desktop/monitoring.png)


## Setup


  1. Create database and tables:
    https://github.com/racker/volga/blob/master/maas-extraction-for-up-time-caclulation/create-hive-tables-v2

## Workflow steps involved:


### 1. MaaS account download and load:

### 2. MaaS metrics download and load:

### 3. MaaS notifications download and load:

### 4. New Relic Load:


All the jobs above are Map Reduce ETL jobs.

They ingest accounts, metrics, notifications data from Cloud Storage Files.
New Relic data is ingested from the New Relic API.

### 5. MaaS Report Prep:
  Source: https://github.com/amoghantarkarRS/volga/blob/master/volga-etl/src/main/resources/maas/metric_report/extract_mar.sh

- **Prep_report.sh** : Calls monitoring HQL

  Build error_metrics_duration table from MaaS tables like check, entities etc

  - Eg: /hive -f /home/maas/metric_report/maas_report.hql -hiveconf start_date=2019-06-24 <br>  <t>  /hive -f /home/maas/metric_report/maas_report1.hql -hiveconf start_date=2019-06-24
  - **maas_report.hql**:

    create tmp_maas_configuration table from **maas.metrics**

    ```sql

    insert overwrite table tmp_maas_configuration
    select distinct check_id,SUBSTR(monitoring_zone_id,3)
    from maas.metrics where dt=${hiveconf:start_date};

    ```

    insert overwrite in bucketed table

    ```sql

    INSERT overwrite TABLE tmp_error_metrics_bucket
    SELECT check_id,
           check_zone,
           ts,
           dt
           FROM
        (
        SELECT DISTINCT check_id,
                       substr(monitoring_zone_id,3) AS check_zone,
                       ts,
                       dt
       FROM            maas.metrics
       WHERE           dt='${hiveconf:start_date}'
       AND             available=false
       UNION ALL
       SELECT a.check_id,
              a.check_monitoring_zone AS check_zone,
              a.ts,
              '${hiveconf:start_date}' AS dt
       FROM   tmp_open_errors a
       JOIN   maas.checks b
       ON     a.check_id=b.id
       WHERE  b.disabled=false
        ) m;
    ```

    ```sql
    insert overwrite table tmp_good_metrics_bucket
      select
         m.check_id,
         substr(m.monitoring_zone_id, 3),
         m.ts
      from
         (
            select
               check_id,
               monitoring_zone_id,
               ts
            from
               maas.metrics
            where
               dt = '${hiveconf:start_date}'
               and available = true
         )
         m
         JOIN
            (
               select distinct
                  check_id
               from
                  tmp_error_metrics_bucket
            )
            tmp
            on m.check_id = tmp.check_id;
    ```

    ```sql

                  INSERT overwrite TABLE up_time_calculation.tmp_error_metrics_duration
                    SELECT   b.check_id,
                             b.check_monitoring_zone,
                             b.ts,
                             min(g.ts)
                      FROM     up_time_calculation.tmp_error_metrics_bucket b
                      JOIN     up_time_calculation.tmp_good_metrics_bucket g
                      ON       b.check_id=g.check_id
                      AND      b.check_monitoring_zone=g.check_monitoring_zone
                      WHERE    g.ts >= b.ts
                      GROUP BY b.check_id,
                               b.check_monitoring_zone,
                               b.ts;
    ```

  + **maas_report1.hql**

   ```sql

   INSERT INTO table error_metrics_duration
    SELECT a.check_id,
           a.check_monitoring_zone ,
           CASE
                  WHEN ch.details['url'] IS NOT NULL THEN ch.details['url']
                  WHEN ch.details['target'] IS NOT NULL THEN ch.details['target']
                  ELSE NULL
           END AS target,
           ch.label,
           ch.type,
           en.label,
           en.managed,
           en.uri,
           reverse(split(reverse(en.uri),'/')[0]) AS device,
           ac.external_id,
           a.start_time,
           a.end_time,
           'MaaS' AS source_system_name
    FROM   (
                    SELECT   check_id,
                             check_monitoring_zone,
                             min(start_time)       AS start_time,
                             COALESCE(end_time, 0) AS end_time
                    FROM     tmp_error_metrics_duration
                    WHERE    end_time !=0
                    GROUP BY check_id,
                             check_monitoring_zone,
                             end_time) a,
           maas.checks ch,
           maas.entities en ,
           maas.accounts ac
    WHERE  a.check_id = ch.id
    AND    ch.entity_id=en.id
    AND    ch.account_id=ac.id;
  ```

    ```sql

    INSERT INTO table up_time_calculation.tmp_error_metrics_duration
      SELECT   z.check_id,
               z.check_monitoring_zone,
               min(z.ts),
               z.ts_end
      FROM     (
                               SELECT          b.check_id,
                                               b.check_monitoring_zone,
                                               b.ts,
                                               COALESCE(g.ts, 0) AS ts_end
                               FROM            up_time_calculation.tmp_error_metrics_bucket b
                               LEFT OUTER JOIN up_time_calculation.tmp_good_metrics_bucket g
                               ON              (
                                                               b.check_id=g.check_id
                                               AND             b.check_monitoring_zone=g.check_monitoring_zone)) z
      WHERE    z.ts_end=0
      GROUP BY z.check_id,
               z.check_monitoring_zone,
               z.ts_end;

            ```

     ```sql

      INSERT overwrite TABLE open_errors
            SELECT a.check_id,
             a.check_monitoring_zone ,
             CASE
                    WHEN ch.details['url'] IS NOT NULL THEN ch.details['url']
                    WHEN ch.details['target'] IS NOT NULL THEN ch.details['target']
                    ELSE NULL
             END AS target,
             ch.label,
             ch.type,
             en.label,
             en.managed,
             en.uri,
             reverse(split(reverse(en.uri),'/')[0]) AS device,
             ac.external_id,
             a.start_time,
             'MaaS' AS source_system_name
      FROM   (
              SELECT   check_id,
                       check_monitoring_zone,
                       min(start_time) AS start_time
              FROM     tmp_error_metrics_duration
              WHERE    end_time=0
              GROUP BY check_id,
                       check_monitoring_zone,
                       end_time) a,
             maas.checks ch,
             maas.entities en ,
             maas.accounts ac
      WHERE  a.check_id = ch.id
      AND    ch.entity_id=en.id
      AND    ch.account_id=ac.id;
```


1. Prepare the downloaded data and prepare tables:

  a. <b>error_metrics_duration</b>  : checkid, check_monitoring_zone,url, target,

```sql
      SELECT          a.check_id,
                      a.start_time,
                      a.end_time,
                      COALESCE(b.rcount,1) AS rcount
      FROM            (
                             SELECT check_id ,
                                    start_time,
                                    end_time
                             FROM   up_time_calculation.error_metrics_duration
                             WHERE  "\   \    "(start_time >= {0}
                             AND    start_time <{1})
                             OR     (
                                           start_time < {2}
                                    AND    end_time > {3})
                             UNION ALL
                             SELECT check_id ,
                                    start_time,
                                    7227103875000
                             FROM   up_time_calculation.open_errors "\         "
                             WHERE  start_time < {4}) a
      LEFT OUTER JOIN
                      (
                               SELECT   a.id,
                                        Count(*) AS rcount
                               FROM     (
                                               SELECT id,
                                                      Substr(mz,3)                                                                AS mzone
                                               FROM   maas.checks lateral view explode(monitoring_zones_poll)"\         " mztable AS mz) AS a
                               group BY a.id
                               UNION ALL
                               SELECT DISTINCT monitor_id,
                                               1
                               FROM            new_relic.monitors
                               WHERE           dt >= {5}) b"\         "
      ON              a.check_id=b.id
      ORDER BY        a.check_id,
                      a.start_time


    ```


### 6. New Relic report prep:

### 7. Availability Calculation

  Source: https://github.com/amoghantarkarRS/volga/blob/master/volga-etl/src/main/resources/maas/metric_report/up_time_calculation.sh

  Steps in **up_time_calculation.sh**:
  - device_status_to_suppression.py
  - service_availability.py
  - monitor_availability.py





  ### device_status_to_suppression.py

  Calculate uptime availability up_time_calculation.sh

    - scoop import **operational_reporting_core.server_status_transition_log** to hive database up_time_calculation.server_status_transition_log
  spark submit job: device_status_to_suppression.py
  ```sql

    SELECT computer_number,
           old_status_number,
           new_status_number,
           Unix_timestamp(To_utc_timestamp(tranistion_timestamp, 'CST')) AS
           tranistion_timestamp
    FROM   up_time_calculation.server_status_transition_log
    ORDER  BY computer_number,
              tranistion_timestamp

  ```

    - Get computer_number, old_status, new status, timestamp from up_time_calculation.server_status_transition_log order by computer_number,tranistion_timestamp

    Build device status rdd from the hive table: devStatusRDD.
    - Build tuple(device (start_time,end_time, timestamp). Sort by timestamp comparator.
    - Group all statuses by computer id and reduce it.
    - Read line by line and check if  start time is in ONLINE status. if yes update the first start time and last end time from entries. Filter only online statuses.
    - Create Dataframe from list in step 2.
    - Truncate and Load device start time and end time from Dataframe into up_time_calculation.suppression_intervals with columns ( device, start_time, end_time)


  1st step: Load monitors.

  - Read Date_marker file, use the supression month interval as 1 month.
  - Insert overwrite  create table up_time_calculation.monitor_details with data from up_time_calculation.vw_monitor_details and up_time_calculation.vw_new_relic_monitor_details.
  - build suppressions map: up_time_calculation.monitor_details(a.device, a.start_time, a.end_time) suppression intervals outside for the planned file time
  - build device monitor map from up_time_calculation.monitor_details (a.check_id, a.device)
  - build error map: Get (checkid, monitoring_zone, start_time, end_time, rowCount) from up_time_calculation.error_metrics_duration  out of time range union with up_time_calculation.open_errors.


  ```sql

  INSERT overwrite TABLE up_time_calculation.monitor_monthly_availability partition
         (
                dt='{0}'
         )
  SELECT a.monitor,
         a.monitoring_zone,
         b.check_target,
         "\	"b.check_label,
         b.check_type,
         b.device_label,
         b.device_managed,
         b.device_uri,
         b.device,
         b.account ,
         a.downtime_duration,
         a.downtime_duration_with_suppression,
         a.up_time,
         "\	"a.up_time_percent,
         a.up_time_with_suppression_percent,
         b.source_system_name
  FROM   monitor_up_time                     AS a,
         up_time_calculation.monitor_details AS b "\	"
  WHERE  a.monitor=b.check_id

  ```

  Monitoring Availability:

  Create spark conf.
  Set driver memory 5 gig

  get start time.
  end date

  suppression_start
  supression_end

  get monitor data =


  ```sql

  SELECT   a.check_id,
           a.check_monitoring_zone,
           a.start_time,
           a.end_time
  FROM     (
                  SELECT check_id ,
                         check_monitoring_zone,
                         start_time,
                         end_time
                  FROM   "\                 "up_time_calculation.error_metrics_duration
                  WHERE  (
                                start_time >= {0}
                         AND    start_time <{1})
                  or     (
                                start_time < {2}
                         AND    end_time > {3})
                  UNION ALL
                            "\                 "SELECT check_id ,
                         check_monitoring_zone,
                         start_time,
                         7227103875000
                  FROM   up_time_calculation.open_errors
                  WHERE  start_time < {4} ) a "\                 "
  ORDER BY a.check_id,
           a.check_monitoring_zone,
           a.start_time


  ```


  get_dev_monitor:


  ```sql
  SELECT DISTINCT a.check_id,
                  a.device
  FROM   up_time_calculation.monitor_details a

  ```


  get now time:

  while now time > start_date:
      - initialize suppression end_time
      - drop partition:

  ```SQL
      ALTER TABLE up_time_calculation.monitor_monthly_availability DROPIF EXISTS partition (dt='{0}')

      ```

      get suppresions:


  ``` SQL

      SELECT   a.device,
               a.start_time,
               a.end_time
      FROM     up_time_calculation.suppression_intervals a
      WHERE    (
                        a.start_time < {0}
               AND      a.end_time > {1})"\                "
      OR       (
                        a.start_time >={2}
               AND      a.start_time < {3})
      ORDER BY a.device,
               a.start_time

               ```

    a.device, a.start_time, a.end_time => a.device , [a.start_time,a.end_time]



    # service_availability.py


  now_time = current time  ...
    now date = now_date_ts* 1000
  while now_time > start_date:
    print getdata


  1. drop partitions <b>uptime_calculation.service_downtime_with_suppression</b> and **up_time_calculation.service_monthly_availability**
  for current month.


  2. get_suppressions =

  ```sql
      SELECT   a.device,
               a.start_time,
               a.end_time
      FROM     up_time_calculation.suppression_intervals a
      WHERE    (
                        a.start_time < {0}
               AND      a.end_time > {1})
      OR       (
                        a.start_time >={2} "\         "
               AND      a.start_time < {3})
      ORDER BY a.device,
               a.start_time

  ```
      broadcast()=> device,[start_time, end_time]

      **_suppressionMapBC_** -> device, [(start_time,end_time), (start_time,end_time)]

      get_dev_monitor :
      ```sql

        SELECT DISTINCT a.check_id,
                      a.device
        FROM   up_time_calculation.monitor_details a
      ```
    deviceMonitorMapBC broadcast = check_id,device

    start_ts,end_ts,supresion_start,supresion_end

    current_statement = get_data();



  3. Find the overlap and find the supression overlap.

  errorRDD =>

  get_data =
  ```SQL

  SELECT  a.check_id,
          a.start_time,
          a.end_time,
          COALESCE(b.rcount,1) AS rcount
  FROM            (
                         SELECT check_id ,
                                start_time,
                                end_time
                         FROM   up_time_calculation.error_metrics_duration
                         WHERE  "\   \    "(start_time >= {0}
                         AND    start_time <{1})
                         OR     (
                                       start_time < {2}
                                AND    end_time > {3})
                         UNION ALL
                         SELECT check_id ,
                                start_time,
                                7227103875000
                         FROM   up_time_calculation.open_errors "\         "
                         WHERE  start_time < {4}) a
  LEFT OUTER JOIN
                  (
                           SELECT   a.id,
                                    Count(*) AS rcount
                           FROM     (
                                           SELECT id,
                                                  Substr(mz,3)                                                                AS mzone
                                           FROM   maas.checks lateral view explode(monitoring_zones_poll)"\         " mztable AS mz) AS a
                           group BY a.id
                           UNION ALL
                           SELECT DISTINCT monitor_id,
                                           1
                           FROM            new_relic.monitors
                           WHERE           dt >= {5}) b"\         "
  ON              a.check_id=b.id
  ORDER BY        a.check_id,
                  a.start_time
  ```
  (check_id, start_time,end_time,rcount) => check_id,
  [start_time,end_time,rcount]
  =>check_id, ([start_time,end_time,rcount],

     [start_time,end_time,rcount],
     [start_time,end_time,rcount])

  - sort by end_time

  find_overlap() => monitor, start_time, end_time, duration
  find overlap with removing suppresion.

  **find_overlap_with_supression() =>
  (checkId, start_time, end_time,duration_with_suppression_removed)**

  ```sql

  SELECT   a.device,
           a.start_time,
           a.end_time
  FROM     up_time_calculation.suppression_intervals a
  WHERE    (
                    a.start_time < {0}
           AND      a.end_time > {1})
  OR       (
                    a.start_time >={2} "\         "
           AND      a.start_time < {3})
  ORDER BY a.device,
           a.start_time

  ```


  ```SQL

    INSERT overwrite TABLE up_time_calculation.service_downtime_with_suppression partition
         (
                dt='{0}'
         )
    SELECT a.monitor,
         b.check_target,
         b.check_label,
         b.check_type,
         "\           "b.device_label,
         b.device_managed,
         b.device_uri,
         b.device,
         b.account ,
         a.start_time,
         a.end_time,
         a.duration ,
         a.duration_with_suppression,
         b.source_system_name "\           "
    FROM   dt_durations                        AS a,
         up_time_calculation.monitor_details AS b
    WHERE  a.monitor=b.check_id

  ```

  ['monitor','start_time','end_time','duration','duration_with_suppression'] ->
  calculateuptime(monitor, duration, duration_with_suppression) =>
  (monitor, downtime_duration, downtime_with_supression, up_time, up_time_percent) => uptimeRDD


  ```SQL

  INSERT overwrite TABLE up_time_calculation.service_monthly_availability partition
         (
                dt='{0}'
         )
  SELECT a.monitor,
         b.check_target,
         b.check_label,
         b.check_type,
         "\           "b.device_label,
         b.device_managed,
         b.device_uri,
         b.device,
         b.account ,
         a.downtime_duration,
         a.downtime_duration_with_suppression,
         a.up_time,
         a.up_time_percent,
         "\           "a.up_time_with_suppression_percent,
         b.source_system_name
  FROM   dt_up_time                          AS a,
         up_time_calculation.monitor_details AS b "\           "
  WHERE  a.monitor=b.check_id

  ```


### 8. MAR Extraction

  Generates the tsv file for new relic and maas report for MAR Extraction
  https://github.com/amoghantarkarRS/volga/blob/master/volga-etl/src/main/resources/maas/metric_report/extract_mar.sh


### 9. Daily availability


  **1. Create temp tables.**

tmp_checks_mz table contains attributes from maas.checks_unique and tmp_monitor_min_max_dates. Attributes like check id , entity id, type, details, created_at, updated_at

  ```sql

  CREATE TABLE r_db.tmp_checks_mz AS
  SELECT checks.id,
         checks.entity_id,
         checks.account_id,
         checks.details,
         checks.type,
         checks.disabled,
         checks.created_at,
         checks.updated_at,
         mm.min_date,
         mm.max_date
  FROM   maas.checks_unique checks,
         r_db.tmp_monitor_min_max_dates mm
  WHERE  checks.id = mm.id


  ```
  Create tmp min max dates table from checks_history table.



  ```SQL

  CREATE TABLE r_db.tmp_monitor_min_max_dates AS
  SELECT id,
        Min(Concat(Substr(dt, 1, 4),
        '-',
        Substr(dt, 5, 2),
        '-',
        Substr(dt, 7, 2))) AS min_date,
        Max(Concat(Substr(dt, 1, 4),
        '-',
        Substr(dt, 5, 2),
        '-',
        Substr(dt, 7, 2))) AS max_date

        FROM maas.checks_history
        WHERE dt >='{0}' AND dt<'{1}' GROUP BY id'''.format(z['start_date_format'],z['end_date_format]


  ```

**2. get suppression data** :  (returns device, start_time, end_time)


```sql

  SELECT Cast(deviceid AS VARCHAR(20))                     AS device,
       Datediff(second, '1970-01-01', suppression_start) AS start_time,
       Datediff(second, '1970-01-01', suppression_end)   AS end_time
  FROM
  Openquery([RBASQLACT_01],
  '
      SELECT DISTINCT a.suppressionid AS suppression_id,
                    c.deviceid,
                    b.actualstartdate AS suppression_start,
                    b.actualenddate   AS suppression_end
      FROM            [SuppressionManager.3.0].[dbo].[Suppression_Audit] A WITH (nolock)
      INNER JOIN      [SuppressionManager.3.0].[dbo].[SuppressionsCompleted_Audit] B WITH (nolock)
      ON              a.suppressionid = b.suppressionid
      INNER JOIN      [SuppressionManager.3.0].dbo.[SM_Device_Status_ChangeLog] C WITH (nolock)
      ON              a.suppressionid = c.suppressionid
      INNER JOIN      [SuppressionManager.3.0].dbo.suppressionstatus D WITH (nolock)
      ON              b.[SuppressionStatusId] = d.[SuppressionStatusId]
      WHERE           d.statusname = ''ended''
      AND             CONVERT(varchar(6),b.actualstartdate,112) = {0} ;
  '
          )) AS suppressions.format(z['calculate_ym'])
```

- Transform SQL RBA data to suppression:

  + device, ([start_time, end_time],[start_time, end_time])
  + broadcast suppressionMapBC

```python

  suppressions_by_device=all_suppressions.map(lambda(a,b,c):(a,[(b,c)])).reduceByKey(lambda a,b:a+b).collectAsMap()

    print suppressions_by_device

    suppressionMapBC=sc.broadcast(suppressions_by_device)
```

**3. Downtime_Query **

 Join uptime table, service_downtime_with_suppression table with min_max dates for source system as MaaS

  ```sql

  SELECT em.check_id,
        em.device,
        Cast(em.start_time / 1000 AS INT)                   AS dt_start,
        Cast(em.end_time / 1000 AS INT)                     AS dt_end,
        Cast(( em.end_time - em.start_time ) / 1000 AS INT) AS error_duration_sec
        ,
        mm.min_date,
        mm.max_date
  FROM   up_time_calculation.service_downtime_with_suppression em,
        r_db.tmp_monitor_min_max_dates mm
  WHERE  em.check_id = mm.id
        AND dt = '{0}'
        AND em.source_system_name = 'MaaS'

  ```

 _Returns_:
 
  ```

  +----------+--------------------+----------+----------+------------------+----------+----------+
  |  check_id|              device|  dt_start|    dt_end|error_duration_sec|  min_date|  max_date|
  +----------+--------------------+----------+----------+------------------+----------+----------+
  |ch0AVpc2ry|              825351|1533081600|1535760000|           2678400|2018-08-01|2018-08-31|
  ```

```python
  errors_planned_u_c1 = errors_by_day.map(isPlanned).filter(lambda x: len(x) > 0)
      aggregate_planned_not_planned=errors_planned_u_c1.map(lambda(a,b,c,d,e,f,g,h,i,j):((a,b,h,i,j),[(e,f,g)])).reduceByKey(lambda a,b:a+b).map(aggregate_planned_not_planned)
      print aggregate_planned_not_planned.take(100)


      schema = StructType([
   StructField('check_id', StringType(), True),
   StructField('device', StringType(), True),
   StructField('plan_for_day', StringType(), True),
   #StructField('start_time', IntegerType(),True),
   #StructField('end_time', IntegerType(),True),
   #StructField('error_duration', IntegerType(),True),
   StructField('not_planned', IntegerType(),True),
   StructField('planned', IntegerType(),True),
   StructField('total_error_duration',IntegerType(),True),
   StructField('min_date',StringType(),True),
   StructField('max_date',StringType(),True)
   ])

   errors_planned_df_u_c1 = sqlContext.createDataFrame(aggregate_planned_not_planned,schema)
   error_days=aggregate_planned_not_planned.map(lambda(a,c,d,e,g,h,i,j):((a,c,i,j),[d])).reduceByKey(lambda a,b:a+b)
      print error_days.take(10)
```
 get the errors that were planned.

 flatmap operation on result for function resolve multiple days.

resolveMultipleDays:

  - if dt_start  == dt_end

  - else start error = month

  start_error = new_end_start_date

  return error_list


  aggregate planned and unplanned:
  ( monitors)


```sql

INSERT INTO table r_db.daily_availability_100_service partition
          (
                      dt='{0}'
          )
SELECT DISTINCT a.account_number,
              a.account_name,
              a.account_type,
              a.account_manager,
              a.account_bdc,
              a.account_lead_tech,
              a.account_primary_contact,
              a.account_service_level,
              a.account_team_name,
              a.customer_number,
              a.device_number,
              a.device_assigned_account_number,
              a.first_device_online_date,
              a.last_device_offline_date ,
              a.device_status,
              a.check_id,
              a.target_url ,
              a.check_type,
              a.label,
              a.check_disabled_flag ,
              a.check_created_date ,
              a.check_updated_date,
              a.uptime_date
FROM            good_records_with_dates a

```

(check_id, start_time,end_time, duration)


1. supressions

Recap:

```sql

SELECT computer_number,
   old_status_number,
   new_status_number,
   Unix_timestamp(To_utc_timestamp(tranistion_timestamp, 'CST')) AS
   tranistion_timestamp
FROM   up_time_calculation.server_status_transition_log
ORDER  BY computer_number,
      tranistion_timestamp

```

```sql


CREATE TABLE r_db.tmp_config_service AS
  SELECT DISTINCT account,
                  account_type,
                  device,
                  check_id,
                  target_url,
                  check_type,
                  label,
                  check_disabled_flag,
                  check_created_date,
                  check_updated_date
  FROM   (SELECT Regexp_replace(ac.external_id, 'hybrid:', '') AS account,
                 CASE
                   WHEN ac.rackspace_managed = TRUE THEN 'Dedicated'
                   ELSE 'Cloud'
                 END                                           AS account_type,
                 em.device,
                 em.check_id,
                 em.check_target                               AS target_url,
                 em.check_type,
                 ch.label,
                 ch.disabled                                   AS
                 check_disabled_flag,
                 ch.created_at                                 AS
                 check_created_date,
                 ch.updated_at                                 AS
                 check_updated_date
          FROM   up_time_calculation.service_downtime_with_suppression em,
                 maas.checks_unique ch,
                 maas.accounts_unique ac
          WHERE  em.dt = '{0}'
                 AND em.source_system_name = 'MaaS'
                 AND em.check_id = ch.id
                 AND ch.account_id = ac.id)

a'''.format(z['start_date])

```

```sql

 SELECT DISTINCT Regexp_replace(ac.external_id,'hybrid:','') AS account,
             dwa.account_name,
             dwa.account_type,
             dwa.account_manager,
             dwa.account_bdc,
             dwa.account_lead_tech,
             dwa.account_primary_contact,
             dwa.account_service_level,
             dwa.account_team_name,
             dwa.customer_number,
             reverse(Split(Reverse(en.uri),'/')[0]) AS device,
             dwa.device_assigned_account_number,
             dwa.first_device_online_date,
             dwa.last_device_offline_date,
             dwa.device_status,
             ch.id,
             CASE
                             WHEN ch.details['url'] IS NOT NULL THEN ch.details['url']
                             WHEN ch.details['target'] IS NOT NULL THEN ch.details['target']
                             ELSE NULL
             END AS target_url,
             ch.type,
             ch.label,
             ch.disabled   AS check_disabled_flag,
             ch.created_at AS check_created_date,
             ch.updated_at AS check_updated_date,
             ch.min_date,
             ch.max_date
FROM            r_db.tmp_checks_mz_100 ch,
             maas.accounts_unique ac ,
             maas.entities_unique en,
             r_db.tmp_config1 dwa
WHERE           ch.account_id=ac.id
AND             ch.entity_id=en.id
AND             regexp_replace(ac.external_id,'hybrid:','')=cast(dwa.account_number AS string)
AND             reverse(split(reverse(en.uri),'/')[0])=cast(dwa.device_number AS string)



```

**Daily Availability table:**
```sql

INSERT INTO table r_db.daily_availability_100_service partition
            (
                        dt='{0}'
            )
SELECT DISTINCT a.account_number,
                a.account_name,
                a.account_type,
                a.account_manager,
                a.account_bdc,
                a.account_lead_tech,
                a.account_primary_contact,
                a.account_service_level,
                a.account_team_name,
                a.customer_number,
                a.device_number,
                a.device_assigned_account_number,
                a.first_device_online_date,
                a.last_device_offline_date ,
                a.device_status,
                a.check_id,
                a.target_url ,
                a.check_type,
                a.label,
                a.check_disabled_flag ,
                a.check_created_date ,
                a.check_updated_date,
                a.uptime_date
FROM            good_records_with_dates a".format(first_month_str)

    ```

```python

schema = StructType([
    StructField('account_number', StringType(), True),
    StructField('account_name', StringType(),True),
    StructField('account_type', StringType(), True),
    StructField('account_manager', StringType(),True),
    StructField('account_bdc', StringType(),True),
    StructField('account_lead_tech', StringType(),True),
    StructField('account_primary_contact', StringType(),True),
    StructField('account_service_level', StringType(),True),
    StructField('account_team_name', StringType(),True),
    StructField('customer_number', StringType(),True),
    StructField('device_number', StringType(),True),
    StructField('device_assigned_account_number', StringType(),True),
    StructField('first_device_online_date', LongType(),True),
    StructField('last_device_offline_date', LongType(),True),
    StructField('device_status', StringType(),True),
    StructField('check_id', StringType(),True),
    StructField('target_url', StringType(),True),
    StructField('check_type', StringType(),True),
    StructField('label', StringType(),True),
    StructField('check_disabled_flag', StringType(),True),
    StructField('check_created_date', LongType(),True),
    StructField('check_updated_date', LongType(),True),
    StructField('min_date',StringType(),True),
    StructField('max_date', StringType(), True),
    StructField('uptime_date', ArrayType(StringType()),True)
    ])
    print '*************************'
    print schema
    print '**************************'
    good_rec_with_dates_df = sqlContext.createDataFrame(good_rec_with_dates,schema)
    good_rec_with_dates_df.printSchema()
    #good_rec_with_dates_df.show()
    good_rec_with_dates_df.registerTempTable('good_records_with_dates')

    sql_stmt="insert into table r_db.daily_availability_100_service partition (dt='{0}') select  distinct a.account_number, a.account_name, a.account_type,a.account_manager, a.account_bdc, a.account_lead_tech, a.account_primary_contact, a.account_service_level,a.account_team_name, a.customer_number, a.device_number, a.device_assigned_account_number,a.first_device_online_date,a.last_device_offline_date ,  a.device_status, a.check_id,  a.target_url , a.check_type, a.label, a.check_disabled_flag ,a.check_created_date ,  a.check_updated_date, a.uptime_date from good_records_with_dates a".format(first_month_str)

    ```
