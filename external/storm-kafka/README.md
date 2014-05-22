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


## Building storm-kafka for Scala 2.10

By default storm-kafka is built against Scala 2.9.2 (see [pom.xml](pom.xml)).  You can build storm-kafka for Scala 2.10
by activating the `Scala-2.10` maven profile:

    $ mvn install -P Scala-2.10


## Committer Sponsors

 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
