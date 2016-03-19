---
title: Storm HBase Integration
layout: documentation
documentation: true
---

Storm/Trident integration for [Apache HBase](https://hbase.apache.org)

## Usage
The main API for interacting with HBase is the `org.apache.storm.hbase.bolt.mapper.HBaseMapper`
interface:

```java
public interface HBaseMapper extends Serializable {
    byte[] rowKey(Tuple tuple);

    ColumnList columns(Tuple tuple);
}
```

The `rowKey()` method is straightforward: given a Storm tuple, return a byte array representing the
row key.

The `columns()` method defines what will be written to an HBase row. The `ColumnList` class allows you
to add both standard HBase columns as well as HBase counter columns.

To add a standard column, use one of the `addColumn()` methods:

```java
ColumnList cols = new ColumnList();
cols.addColumn(this.columnFamily, field.getBytes(), toBytes(tuple.getValueByField(field)));
```

To add a counter column, use one of the `addCounter()` methods:

```java
ColumnList cols = new ColumnList();
cols.addCounter(this.columnFamily, field.getBytes(), toLong(tuple.getValueByField(field)));
```

When the remote HBase is security enabled, a kerberos keytab and the corresponding principal name need to be
provided for the storm-hbase connector. Specifically, the Config object passed into the topology should contain
{(“storm.keytab.file”, “$keytab”), ("storm.kerberos.principal", “$principal”)}. Example:

```java
Config config = new Config();
...
config.put("storm.keytab.file", "$keytab");
config.put("storm.kerberos.principal", "$principle");
StormSubmitter.submitTopology("$topologyName", config, builder.createTopology());
```

##Working with Secure HBASE using delegation tokens.
If your topology is going to interact with secure HBase, your bolts/states needs to be authenticated by HBase. 
The approach described above requires that all potential worker hosts have "storm.keytab.file" on them. If you have 
multiple topologies on a cluster , each with different hbase user, you will have to create multiple keytabs and distribute
it to all workers. Instead of doing that you could use the following approach:

Your administrator can configure nimbus to automatically get delegation tokens on behalf of the topology submitter user.
The nimbus need to start with following configurations:

nimbus.autocredential.plugins.classes : ["org.apache.storm.hbase.security.AutoHBase"] 
nimbus.credential.renewers.classes : ["org.apache.storm.hbase.security.AutoHBase"] 
hbase.keytab.file: "/path/to/keytab/on/nimbus" (This is the keytab of hbase super user that can impersonate other users.)
hbase.kerberos.principal: "superuser@EXAMPLE.com"
nimbus.credential.renewers.freq.secs : 518400 (6 days, hbase tokens by default expire every 7 days and can not be renewed, 
if you have custom settings for hbase.auth.token.max.lifetime in hbase-site.xml than you should ensure this value is 
atleast 1 hour less then that.)

Your topology configuration should have:
topology.auto-credentials :["org.apache.storm.hbase.security.AutoHBase"] 

If nimbus did not have the above configuration you need to add it and then restart it. Ensure the hbase configuration 
files(core-site.xml,hdfs-site.xml and hbase-site.xml) and the storm-hbase jar with all the dependencies is present in nimbus's classpath. 
Nimbus will use the keytab and principal specified in the config to authenticate with HBase. From then on for every
topology submission, nimbus will impersonate the topology submitter user and acquire delegation tokens on behalf of the
topology submitter user. If topology was started with topology.auto-credentials set to AutoHBase, nimbus will push the
delegation tokens to all the workers for your topology and the hbase bolt/state will authenticate with these tokens.

As nimbus is impersonating topology submitter user, you need to ensure the user specified in storm.kerberos.principal 
has permissions to acquire tokens on behalf of other users. To achieve this you need to follow configuration directions 
listed on this link

http://hbase.apache.org/book/security.html#security.rest.gateway

You can read about setting up secure HBase here:http://hbase.apache.org/book/security.html.

### SimpleHBaseMapper
`storm-hbase` includes a general purpose `HBaseMapper` implementation called `SimpleHBaseMapper` that can map Storm
tuples to both regular HBase columns as well as counter columns.

To use `SimpleHBaseMapper`, you simply tell it which fields to map to which types of columns.

The following code create a `SimpleHBaseMapper` instance that:

1. Uses the `word` tuple value as a row key.
2. Adds a standard HBase column for the tuple field `word`.
3. Adds an HBase counter column for the tuple field `count`.
4. Writes values to the `cf` column family.

```java
SimpleHBaseMapper mapper = new SimpleHBaseMapper() 
        .withRowKeyField("word")
        .withColumnFields(new Fields("word"))
        .withCounterFields(new Fields("count"))
        .withColumnFamily("cf");
```
### HBaseBolt
To use the `HBaseBolt`, construct it with the name of the table to write to, an a `HBaseMapper` implementation:

 ```java
HBaseBolt hbase = new HBaseBolt("WordCount", mapper);
 ```

The `HBaseBolt` will delegate to the `mapper` instance to figure out how to persist tuple data to HBase.

###HBaseValueMapper
This class allows you to transform the HBase lookup result into storm Values that will be emitted by the `HBaseLookupBolt`.

```java
public interface HBaseValueMapper extends Serializable {
    public List<Values> toTuples(Result result) throws Exception;
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
```

The `toTuples` method takes in a HBase `Result` instance and expects a List of `Values` instant. 
Each of the value returned by this function will be emitted by the `HBaseLookupBolt`.

The `declareOutputFields` should be used to declare the outputFields of the `HBaseLookupBolt`.

There is an example implementation in `src/test/java` directory.

###HBaseProjectionCriteria
This class allows you to specify the projection criteria for your HBase Get function. This is optional parameter
for the lookupBolt and if you do not specify this instance all the columns will be returned by `HBaseLookupBolt`.

```java
public class HBaseProjectionCriteria implements Serializable {
    public HBaseProjectionCriteria addColumnFamily(String columnFamily);
    public HBaseProjectionCriteria addColumn(ColumnMetaData column);
```    
`addColumnFamily` takes in columnFamily. Setting this parameter means all columns for this family will be included
 in the projection.
 
`addColumn` takes in a columnMetaData instance. Setting this parameter means only this column from the column familty 
 will be part of your projection.
The following code creates a projectionCriteria which specifies a projection criteria that:

1. includes count column from column family cf.
2. includes all columns from column family cf2.

```java
HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria()
    .addColumn(new HBaseProjectionCriteria.ColumnMetaData("cf", "count"))
    .addColumnFamily("cf2");
```

###HBaseLookupBolt
To use the `HBaseLookupBolt`, Construct it with the name of the table to write to, an implementation of `HBaseMapper` 
and an implementation of `HBaseRowToStormValueMapper`. You can optionally specify a `HBaseProjectionCriteria`. 

The `HBaseLookupBolt` will use the mapper to get rowKey to lookup for. It will use the `HBaseProjectionCriteria` to 
figure out which columns to include in the result and it will leverage the `HBaseRowToStormValueMapper` to get the 
values to be emitted by the bolt.

You can look at an example topology LookupWordCount.java under `src/test/java`.
## Example: Persistent Word Count
A runnable example can be found in the `src/test/java` directory.

### Setup
The following steps assume you are running HBase locally, or there is an `hbase-site.xml` on the
classpath pointing to your HBase cluster.

Use the `hbase shell` command to create the schema:

```
> create 'WordCount', 'cf'
```

### Execution
Run the `org.apache.storm.hbase.topology.PersistenWordCount` class (it will run the topology for 10 seconds, then exit).

After (or while) the word count topology is running, run the `org.apache.storm.hbase.topology.WordCountClient` class
to view the counter values stored in HBase. You should see something like to following:

```
Word: 'apple', Count: 6867
Word: 'orange', Count: 6645
Word: 'pineapple', Count: 6954
Word: 'banana', Count: 6787
Word: 'watermelon', Count: 6806
```

For reference, the sample topology is listed below:

```java
public class PersistentWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";


    public static void main(String[] args) throws Exception {
        Config config = new Config();

        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")
                .withColumnFields(new Fields("word"))
                .withCounterFields(new Fields("count"))
                .withColumnFamily("cf");

        HBaseBolt hbase = new HBaseBolt("WordCount", mapper);


        // wordSpout ==> countBolt ==> HBaseBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(HBASE_BOLT, hbase, 1).fieldsGrouping(COUNT_BOLT, new Fields("word"));


        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}
```

