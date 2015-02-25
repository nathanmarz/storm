# Storm Hive Bolt & Trident State

  Hive offers streaming API that allows data to be written continuously into Hive. The incoming data 
  can be continuously committed in small batches of records into existing Hive partition or table. Once the data
  is committed its immediately visible to all hive queries. More info on Hive Streaming API 
  https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest
  
  With the help of Hive Streaming API , HiveBolt and HiveState allows users to stream data from storm into hive directly.
  To use Hive streaming API users need to create a bucketed table with ORC format.  Example below
  
  ```code
  create table test_table ( id INT, name STRING, phone STRING, street STRING) partitioned by (city STRING, state STRING) stored as orc tblproperties ("orc.compress"="NONE");
  ```
  

## HiveBolt (org.apache.storm.hive.bolt.HiveBolt)

HiveBolt streams tuples directly into hive. Tuples are written using Hive Transactions. 
Partiions to which HiveBolt will stream to can either created or pre-created or optionally
HiveBolt  can create them if they are missing. Fields from Tuples are mapped to table columns.
User should make sure that Tuple filed names are matched to the table column names.

```java
DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames));
HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper);
HiveBolt hiveBolt = new HiveBolt(hiveOptions);
```

### RecordHiveMapper
   This class maps Tuple filed names to Hive table column names.
   There are two implementaitons available
 
   
   + DelimitedRecordHiveMapper (org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper)
   + JsonRecordHiveMapper (org.apache.storm.hive.bolt.mapper.JsonRecordHiveMapper)
   
   ```java
   DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
    or
   DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withTimeAsPartitionField("YYYY/MM/DD");
   ```

|Arg | Description | Type
|--- |--- |---
|withColumnFields| field names in a tuple to be mapped to table column names | Fileds (required) |
|withPartitionFields| field names in a tuple can be mapped to hive table partitions | Fields |
|withTimeAsPartitionField| users can select system time as partition in hive table| String . Date format|

### HiveOptions (org.apache.storm.hive.common.HiveOptions)
  
HiveBolt takes in HiveOptions as a constructor arg.

  ```java
  HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                                .withTxnsPerBatch(10)
                				.withBatchSize(1000)
                	     		.withIdleTimeout(10)
  ```


HiveOptions params

|Arg  |Description | Type
|---	|--- |---
|metaStoreURI | hive meta store URI (can be found in hive-site.xml) | String (required) |
|dbName | database name | String (required) |
|tblName | table name | String (required) |
|mapper| Mapper class to map Tuple field names to Table column names | DelimitedRecordHiveMapper or JsonRecordHiveMapper (required) |
|withTxnsPerBatch | Hive grants a *batch of transactions* instead of single transactions to streaming clients like HiveBolt.This setting configures the number of desired transactions per Transaction Batch. Data from all transactions in a single batch end up in a single file. Flume will write a maximum of batchSize events in each transaction in the batch. This setting in conjunction with batchSize provides control over the size of each file. Note that eventually Hive will transparently compact these files into larger files.| Integer . default 100 |
|withMaxOpenConnections| Allow only this number of open connections. If this number is exceeded, the least recently used connection is closed.| Integer . default 100|
|withBatchSize| Max number of events written to Hive in a single Hive transaction| Integer. default 15000|
|withCallTimeout| (In milliseconds) Timeout for Hive & HDFS I/O operations, such as openTxn, write, commit, abort. | Integer. default 10000|
|withHeartBeatInterval| (In seconds) Interval between consecutive heartbeats sent to Hive to keep unused transactions from expiring. Set this value to 0 to disable heartbeats.| Integer. default 240 |
|withAutoCreatePartitions| HiveBolt will automatically create the necessary Hive partitions to stream to. |Boolean. defalut true |
|withKerberosPrinicipal| Kerberos user principal for accessing secure Hive | String|
|withKerberosKeytab| Kerberos keytab for accessing secure Hive | String |


 
## HiveState (org.apache.storm.hive.trident.HiveTrident)

Hive Trident state also follows similar pattern to HiveBolt it takes in HiveOptions as an arg.

```code
   DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withTimeAsPartitionField("YYYY/MM/DD");
            
   HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                                .withTxnsPerBatch(10)
                				.withBatchSize(1000)
                	     		.withIdleTimeout(10)
                	     		
   StateFactory factory = new HiveStateFactory().withOptions(hiveOptions);
   TridentState state = stream.partitionPersist(factory, hiveFields, new HiveUpdater(), new Fields());
 ```
   
 
## Committer Sponsors
 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
 * Bobby Evans ([bobby@apache.org](mailto:bobby@apache.org))






 
