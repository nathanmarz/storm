Storm Kafka
====================

Provides core storm and Trident spout implementations for consuming data from Apache Kafka 0.8.x.

##Spouts
We support both trident and core storm spouts. For both spout implementation we use a BrokerHost interface that
tracks kafka broker host to partition mapping and kafkaConfig that controls some kafka related parameters.
 
###BrokerHosts
In order to initialize your kafka spout/emitter you need to construct and instance of the marker interface BrokerHosts. 
Currently we support following two implementations:

####ZkHosts
ZkHosts is what you should use if you want to dynamically track kafka broker to partition mapping. This class uses 
Kafka's zookeeper's entries to track brokerHost -> partition mapping. You can instantiate an object by calling
```java
    public ZkHosts(String brokerZkStr, String brokerZkPath) 
    public ZkHosts(String brokerZkStr)
```
Where brokerZkStr is just ip:port e.g. localhost:9092. brokerZkPath is the root directory under which all the topics and
partition information is stored. by Default this is /brokers which is what default kafka implementation uses.

By default the broker-partition mapping is refreshed every 60 seconds from zookeeper. If you want to change it you
should set host.refreshFreqSecs to your chosen value.

####StaticHosts
This is an alternative implementation where broker -> partition information is static. In order to construct an instance
of this class you need to first construct an instance of GlobalPartitionInformation.

```java
    Broker brokerForPartition0 = new Broker("localhost");//localhost:9092
    Broker brokerForPartition1 = new Broker("localhost", 9092);//localhost:9092 but we specified the port explicitly
    Broker brokerForPartition2 = new Broker("localhost:9092");//localhost:9092 specified as one string.
    GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
    partitionInfo.add(0, brokerForPartition0)//mapping form partition 0 to brokerForPartition0
    partitionInfo.add(1, brokerForPartition1)//mapping form partition 1 to brokerForPartition1
    partitionInfo.add(2, brokerForPartition2)//mapping form partition 2 to brokerForPartition2
    StaticHosts hosts = new StaticHosts(partitionInfo);
```

###KafkaConfig
The second thing needed for constructing a kafkaSpout is an instance of KafkaConfig. 
```java
    public KafkaConfig(BrokerHosts hosts, String topic)
    public KafkaConfig(BrokerHosts hosts, String topic, String clientId)
```

The BorkerHosts can be any implementation of BrokerHosts interface as described above. the Topic is name of kafka topic.
The optional ClientId is used as a part of the zookeeper path where the spout's current consumption offset is stored.

There are 2 extensions of KafkaConfig currently in use.

Spoutconfig is an extension of KafkaConfig that supports 2 additional fields, zkroot and id. The Zkroot will be used
as root to store your consumer's offset. The id should uniquely identify your spout.
```java
public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id);
```
Core KafkaSpout only accepts an instance of SpoutConfig.

TridentKafkaConfig is another extension of KafkaConfig.
```java
public SpoutConfig(BrokerHosts hosts, String topic, String id);
```
TridentKafkaEmitter only accepts TridentKafkaConfig.

The KafkaConfig class also has bunch of public variables that controls your application's behavior. Here are defaults:
```java
    public int fetchSizeBytes = 1024 * 1024;
    public int socketTimeoutMs = 10000;
    public int fetchMaxWait = 10000;
    public int bufferSizeBytes = 1024 * 1024;
    public MultiScheme scheme = new RawMultiScheme();
    public boolean forceFromStart = false;
    public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
    public long maxOffsetBehind = Long.MAX_VALUE;
    public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
    public int metricsTimeBucketSizeInSecs = 60;
```

Most of them are self explanatory except MultiScheme.
###MultiScheme
MultiScheme is an interface that dictates how the byte[] consumed from kafka gets transformed into a storm tuple. It
also controls the naming of your output field.

```java
  public Iterable<List<Object>> deserialize(byte[] ser);
  public Fields getOutputFields();
```

The default RawMultiScheme just takes the byte[] and returns a tuple with byte[] as is. The name of the outputField is
"bytes". There are alternative implementation like SchemeAsMultiScheme and KeyValueSchemeAsMultiScheme which can convert
the byte[] to String. 
### Examples
####Core Spout
```java
BrokerHosts hosts = new ZkHosts(zkConnString);
SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
```
####Trident Spout
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

## Committer Sponsors

 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
