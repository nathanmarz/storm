---
title: Documentation
layout: documentation
documentation: true
---
### Basics of Storm

* [Javadoc](javadocs/index.html)
* [Concepts](Concepts.html)
* [Configuration](Configuration.html)
* [Guaranteeing message processing](Guaranteeing-message-processing.html)
* [Daemon Fault Tolerance](Daemon-Fault-Tolerance.html)
* [Command line client](Command-line-client.html)
* [REST API](STORM-UI-REST-API.html)
* [Understanding the parallelism of a Storm topology](Understanding-the-parallelism-of-a-Storm-topology.html)
* [FAQ](FAQ.html)

### Layers on Top of Storm

* [Flux Data Driven Topology Builder](flux.html)
* [SQL](storm-sql.html)

#### Trident

Trident is an alternative interface to Storm. It provides exactly-once processing, "transactional" datastore persistence, and a set of common stream analytics operations.

* [Trident Tutorial](Trident-tutorial.html)     -- basic concepts and walkthrough
* [Trident API Overview](Trident-API-Overview.html) -- operations for transforming and orchestrating data
* [Trident State](Trident-state.html)        -- exactly-once processing and fast, persistent aggregation
* [Trident spouts](Trident-spouts.html)       -- transactional and non-transactional data intake
* [Trident RAS API](Trident-RAS-API.html)     -- using the Resource Aware Scheduler with Trident.

### Setup and Deploying

* [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html)
* [Local mode](Local-mode.html)
* [Troubleshooting](Troubleshooting.html)
* [Running topologies on a production cluster](Running-topologies-on-a-production-cluster.html)
* [Building Storm](Maven.html) with Maven
* [Setting up a Secure Cluster](SECURITY.html)
* [CGroup Enforcement](cgroups_in_storm.html)
* [Pacemaker reduces load on zookeeper for large clusters](Pacemaker.html)
* [Resource Aware Scheduler](Resource_Aware_Scheduler_overview.html)
* [Daemon Metrics/Monitoring](storm-metrics-profiling-internal-actions.html)
* [Windows users guide](windows-users-guide.html)

### Intermediate

* [Serialization](Serialization.html)
* [Common patterns](Common-patterns.html)
* [Clojure DSL](Clojure-DSL.html)
* [Using non-JVM languages with Storm](Using-non-JVM-languages-with-Storm.html)
* [Distributed RPC](Distributed-RPC.html)
* [Transactional topologies](Transactional-topologies.html)
* [Direct groupings](Direct-groupings.html)
* [Hooks](Hooks.html)
* [Metrics](Metrics.html)
* [State Checkpointing](State-checkpointing.html)
* [Windowing](Windowing.html)
* [Blobstore(Distcahce)](distcache-blobstore.html)

### Debugging
* [Dynamic Log Level Settings](dynamic-log-level-settings.html)
* [Searching Worker Logs](Logs.html)
* [Worker Profiling](dynamic-worker-profiling.html)

### Integration With External Systems, and Other Libraries
* [Apache Kafka Integration](storm-kafka.html)
* [Apache HBase Integration](storm-hbase.html)
* [Apache HDFS Integration](storm-hdfs.html)
* [Apache Hive Integration](storm-hive.html)
* [Apache Solr Integration](storm-solr.html)
* [JDBC Integration](storm-jdbc.html)
* [Redis Integration](storm-redis.html) 
* [Cassandra Integration](storm-cassandra.html)
* [Event Hubs Intergration](storm-eventhubs.html)
* [Elasticsearch Integration](storm-elasticsearch.html)
* [MQTT Integration](storm-mqtt.html)
* [Mongodb Integration](storm-mongodb.html)
* [Kestrel Integration](Kestrel-and-Storm.html)

### Advanced

* [Defining a non-JVM language DSL for Storm](Defining-a-non-jvm-language-dsl-for-storm.html)
* [Multilang protocol](Multilang-protocol.html) (how to provide support for another language)
* [Implementation docs](Implementation-docs.html)

