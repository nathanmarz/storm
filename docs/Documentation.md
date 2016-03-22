---
layout: documentation
---
### Basics of Storm

* [Javadoc](javadocs/index.html)
* [Concepts](Concepts.html)
* [Configuration](Configuration.html)
* [Guaranteeing message processing](Guaranteeing-message-processing.html)
* [Fault-tolerance](Fault-tolerance.html)
* [Command line client](Command-line-client.html)
* [Understanding the parallelism of a Storm topology](Understanding-the-parallelism-of-a-Storm-topology.html)
* [FAQ](FAQ.html)

### Trident

Trident is an alternative interface to Storm. It provides exactly-once processing, "transactional" datastore persistence, and a set of common stream analytics operations.

* [Trident Tutorial](Trident-tutorial.html)     -- basic concepts and walkthrough
* [Trident API Overview](Trident-API-Overview.html) -- operations for transforming and orchestrating data
* [Trident State](Trident-state.html)        -- exactly-once processing and fast, persistent aggregation
* [Trident spouts](Trident-spouts.html)       -- transactional and non-transactional data intake

### Setup and deploying

* [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html)
* [Local mode](Local-mode.html)
* [Troubleshooting](Troubleshooting.html)
* [Running topologies on a production cluster](Running-topologies-on-a-production-cluster.html)
* [Building Storm](Maven.html) with Maven

### Intermediate

* [Serialization](Serialization.html)
* [Common patterns](Common-patterns.html)
* [Clojure DSL](Clojure-DSL.html)
* [Using non-JVM languages with Storm](Using-non-JVM-languages-with-Storm.html)
* [Distributed RPC](Distributed-RPC.html)
* [Transactional topologies](Transactional-topologies.html)
* [Kestrel and Storm](Kestrel-and-Storm.html)
* [Direct groupings](Direct-groupings.html)
* [Hooks](Hooks.html)
* [Metrics](Metrics.html)
* [Lifecycle of a trident tuple]()

### Advanced

* [Defining a non-JVM language DSL for Storm](Defining-a-non-jvm-language-dsl-for-storm.html)
* [Multilang protocol](Multilang-protocol.html) (how to provide support for another language)
* [Implementation docs](Implementation-docs.html)
