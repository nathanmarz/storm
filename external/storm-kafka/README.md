Storm Kafka
====================

Provides core storm and Trident spout implementations for consuming data from Apache Kafka 0.8.x.


## Usage Example

```java
TridentTopology topology = new TridentTopology();
BrokerHosts zk = new ZkHosts("localhost");
TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "test-topic");
spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
```


## Using storm-kafka with different versions of Scala

Storm-kafka's Kafka dependency is defined as `provided` scope in maven, meaning it will not be pulled in
as a transitive dependency. This allows you to use a version of Kafka built against a specific Scala version.

When building a project with storm-kafka, you must explicitly add the Kafka dependency. For example, to
use Kafka 0.8.1.1 built against Scala 2.10, you would use the following dependency in your `pom.xml`:

```xml
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.10</artifactId>
            <version>0.8.1.1</version>
            <scope>provided</scope>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.zookeeper</groupId>
                    <artifactId>zookeeper</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

Note that the ZooKeeper and log4j dependencies are excluded to prevent version conflicts with Storm's dependencies.

##Writing to Kafka as part of your topology
You can create an instance of storm.kafka.bolt.KafkaBolt and attach it as a component to your topology or if you 
are using trident you can use storm.kafka.trident.TridentState, storm.kafka.trident.TridentStateFactory and
storm.kafka.trident.TridentKafkaUpdater.

You need to provide implementation of following 2 interfaces

###TupleToKafkaMapper and TridentTupleToKafkaMapper
These interfaces have 2 methods defined:

```java
    K getKeyFromTuple(Tuple/TridentTuple tuple);
    V getMessageFromTuple(Tuple/TridentTuple tuple);
```

as the name suggests these methods are called to map a tuple to kafka key and kafka message. If you just want one field
as key and one field as value then you can use the provided FieldNameBasedTupleToKafkaKeyAndMessageMapper.java 
implementation. In the KafkaBolt the implementation always looks for a field with field name "key" and "message" for 
backward compatibility reasons. 
In the TridentKafkaState you can specify what is the field name for key and message. These should be specified while
constructing and instance of FieldNameBasedTupleToKafkaKeyAndMessageMapper.

###KafkaTopicSelector and trident KafkaTopicSelector
This interface has only one method
```java
public interface KafkaTopicSelector {
    List<String> getTopics(Tuple/TridentTuple tuple);
}
```
The implementation of this interface should return topics to which the tuple's key/message mapping needs to be published 
You can return an empty list but null is not acceptable. If you have one static topic name then you can use 
DefaultTopicSelector.java and set the name of the topic in the constructor.

### Specifying kafka producer properties
You can provide all the produce properties , see http://kafka.apache.org/07/configuration.html 
section "Important configuration properties for the producer", in your storm topology config by setting the properties
map with key kafka.broker.properties.

###Putting it all together

For the bolt :
```java
        TopologyBuilder builder = new TopologyBuilder();
    
        Fields fields = new Fields("key", "message");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                    new Values("storm", "1"),
                    new Values("trident", "1"),
                    new Values("needs", "1"),
                    new Values("javadoc", "1")
        );
        spout.setCycle(true);
        builder.setSpout("spout", spout, 5);
        KafkaBolt bolt = new KafkaBolt()
                .withKafkaTopicSelector(new DefaultTopicSelector("test"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("spout");
        
        Config conf = new Config();
        //set producer properties.
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        
        StormSubmitter.submitTopology("kafkaboltTest", conf, builder.createTopology());
```

For Trident:

```java
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", "1"),
                new Values("trident", "1"),
                new Values("needs", "1"),
                new Values("javadoc", "1")
        );
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector("test"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
        stream.partitionPersist(stateFactory, fields, new TridentKafkaUpdater(), new Fields());

        Config conf = new Config();
        //set producer properties.
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());
```

## Committer Sponsors

 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
