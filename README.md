#Storm HBase

Storm/Trident integration for [Apache HBase]()

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

### SimpleHBaseMapper
`storm-hdfs` includes a general purpose `HBaseMapper` implementation called `SimpleHBaseMapper` that can map Storm
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