#Storm HBase

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


## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

## Committer Sponsors

 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
 * Sriharsha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org))
 