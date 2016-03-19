---
title: Structure of the Codebase
layout: documentation
documentation: true
---
There are three distinct layers to Storm's codebase.

First, Storm was designed from the very beginning to be compatible with multiple languages. Nimbus is a Thrift service and topologies are defined as Thrift structures. The usage of Thrift allows Storm to be used from any language.

Second, all of Storm's interfaces are specified as Java interfaces. So even though there's a lot of Clojure in Storm's implementation, all usage must go through the Java API. This means that every feature of Storm is always available via Java.

Third, Storm's implementation is largely in Clojure. Line-wise, Storm is about half Java code, half Clojure code. But Clojure is much more expressive, so in reality the great majority of the implementation logic is in Clojure. 

The following sections explain each of these layers in more detail.

### storm.thrift

The first place to look to understand the structure of Storm's codebase is the [storm.thrift]({{page.git-blob-base}}/storm-core/src/storm.thrift) file.

Storm uses [this fork](https://github.com/nathanmarz/thrift/tree/storm) of Thrift (branch 'storm') to produce the generated code. This "fork" is actually Thrift 7 with all the Java packages renamed to be `org.apache.thrift7`. Otherwise, it's identical to Thrift 7. This fork was done because of the lack of backwards compatibility in Thrift and the need for many people to use other versions of Thrift in their Storm topologies.

Every spout or bolt in a topology is given a user-specified identifier called the "component id". The component id is used to specify subscriptions from a bolt to the output streams of other spouts or bolts. A [StormTopology]({{page.git-blob-base}}/storm-core/src/storm.thrift#L91) structure contains a map from component id to component for each type of component (spouts and bolts).

Spouts and bolts have the same Thrift definition, so let's just take a look at the [Thrift definition for bolts]({{page.git-blob-base}}/storm-core/src/storm.thrift#L79). It contains a `ComponentObject` struct and a `ComponentCommon` struct.

The `ComponentObject` defines the implementation for the bolt. It can be one of three types:

1. A serialized java object (that implements [IBolt]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/task/IBolt.java))
2. A `ShellComponent` object that indicates the implementation is in another language. Specifying a bolt this way will cause Storm to instantiate a [ShellBolt]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/task/ShellBolt.java) object to handle the communication between the JVM-based worker process and the non-JVM-based implementation of the component.
3. A `JavaObject` structure which tells Storm the classname and constructor arguments to use to instantiate that bolt. This is useful if you want to define a topology in a non-JVM language. This way, you can make use of JVM-based spouts and bolts without having to create and serialize a Java object yourself.

`ComponentCommon` defines everything else for this component. This includes:

1. What streams this component emits and the metadata for each stream (whether it's a direct stream, the fields declaration)
2. What streams this component consumes (specified as a map from component_id:stream_id to the stream grouping to use)
3. The parallelism for this component
4. The component-specific [configuration](Configuration.html) for this component

Note that the structure spouts also have a `ComponentCommon` field, and so spouts can also have declarations to consume other input streams. Yet the Storm Java API does not provide a way for spouts to consume other streams, and if you put any input declarations there for a spout you would get an error when you tried to submit the topology. The reason that spouts have an input declarations field is not for users to use, but for Storm itself to use. Storm adds implicit streams and bolts to the topology to set up the [acking framework](https://github.com/apache/storm/wiki/Guaranteeing-message-processing), and two of these implicit streams are from the acker bolt to each spout in the topology. The acker sends "ack" or "fail" messages along these streams whenever a tuple tree is detected to be completed or failed. The code that transforms the user's topology into the runtime topology is located [here]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/common.clj#L279).

### Java interfaces

The interfaces for Storm are generally specified as Java interfaces. The main interfaces are:

1. [IRichBolt](javadocs/org/apache/storm/topology/IRichBolt.html)
2. [IRichSpout](javadocs/org/apache/storm/topology/IRichSpout.html)
3. [TopologyBuilder](javadocs/org/apache/storm/topology/TopologyBuilder.html)

The strategy for the majority of the interfaces is to:

1. Specify the interface using a Java interface
2. Provide a base class that provides default implementations when appropriate

You can see this strategy at work with the [BaseRichSpout](javadocs/org/apache/storm/topology/base/BaseRichSpout.html) class.

Spouts and bolts are serialized into the Thrift definition of the topology as described above. 

One subtle aspect of the interfaces is the difference between `IBolt` and `ISpout` vs. `IRichBolt` and `IRichSpout`. The main difference between them is the addition of the `declareOutputFields` method in the "Rich" versions of the interfaces. The reason for the split is that the output fields declaration for each output stream needs to be part of the Thrift struct (so it can be specified from any language), but as a user you want to be able to declare the streams as part of your class. What `TopologyBuilder` does when constructing the Thrift representation is call `declareOutputFields` to get the declaration and convert it into the Thrift structure. The conversion happens [at this portion]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/topology/TopologyBuilder.java#L205) of the `TopologyBuilder` code.


### Implementation

Specifying all the functionality via Java interfaces ensures that every feature of Storm is available via Java. Moreso, the focus on Java interfaces ensures that the user experience from Java-land is pleasant as well.

The implementation of Storm, on the other hand, is primarily in Clojure. While the codebase is about 50% Java and 50% Clojure in terms of LOC, most of the implementation logic is in Clojure. There are two notable exceptions to this, and that is the [DRPC](https://github.com/apache/storm/wiki/Distributed-RPC) and [transactional topologies](https://github.com/apache/storm/wiki/Transactional-topologies) implementations. These are implemented purely in Java. This was done to serve as an illustration for how to implement a higher level abstraction on Storm. The DRPC and transactional topologies implementations are in the [org.apache.storm.coordination]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/coordination), [org.apache.storm.drpc]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/drpc), and [org.apache.storm.transactional]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/transactional) packages.

Here's a summary of the purpose of the main Java packages and Clojure namespace:

#### Java packages

[org.apache.storm.coordination]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/coordination): Implements the pieces required to coordinate batch-processing on top of Storm, which both DRPC and transactional topologies use. `CoordinatedBolt` is the most important class here.

[org.apache.storm.drpc]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/drpc): Implementation of the DRPC higher level abstraction

[org.apache.storm.generated]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/generated): The generated Thrift code for Storm (generated using [this fork](https://github.com/nathanmarz/thrift) of Thrift, which simply renames the packages to org.apache.thrift7 to avoid conflicts with other Thrift versions)

[org.apache.storm.grouping]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/grouping): Contains interface for making custom stream groupings

[org.apache.storm.hooks]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/hooks): Interfaces for hooking into various events in Storm, such as when tasks emit tuples, when tuples are acked, etc. User guide for hooks is [here](https://github.com/apache/storm/wiki/Hooks).

[org.apache.storm.serialization]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/serialization): Implementation of how Storm serializes/deserializes tuples. Built on top of [Kryo](http://code.google.com/p/kryo/).

[org.apache.storm.spout]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/spout): Definition of spout and associated interfaces (like the `SpoutOutputCollector`). Also contains `ShellSpout` which implements the protocol for defining spouts in non-JVM languages.

[org.apache.storm.task]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/task): Definition of bolt and associated interfaces (like `OutputCollector`). Also contains `ShellBolt` which implements the protocol for defining bolts in non-JVM languages. Finally, `TopologyContext` is defined here as well, which is provided to spouts and bolts so they can get data about the topology and its execution at runtime.

[org.apache.storm.testing]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/testing): Contains a variety of test bolts and utilities used in Storm's unit tests.

[org.apache.storm.topology]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/topology): Java layer over the underlying Thrift structure to provide a clean, pure-Java API to Storm (users don't have to know about Thrift). `TopologyBuilder` is here as well as the helpful base classes for the different spouts and bolts. The slightly-higher level `IBasicBolt` interface is here, which is a simpler way to write certain kinds of bolts.

[org.apache.storm.transactional]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/transactional): Implementation of transactional topologies.

[org.apache.storm.tuple]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/tuple): Implementation of Storm's tuple data model.

[org.apache.storm.utils]({{page.git-tree-base}}/storm-core/src/jvm/org/apache/storm/tuple): Data structures and miscellaneous utilities used throughout the codebase.


#### Clojure namespaces

[org.apache.storm.bootstrap]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/bootstrap.clj): Contains a helpful macro to import all the classes and namespaces that are used throughout the codebase.

[org.apache.storm.clojure]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/clojure.clj): Implementation of the Clojure DSL for Storm.

[org.apache.storm.cluster]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/cluster.clj): All Zookeeper logic used in Storm daemons is encapsulated in this file. This code manages how cluster state (like what tasks are running where, what spout/bolt each task runs as) is mapped to the Zookeeper "filesystem" API.

[org.apache.storm.command.*]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/command): These namespaces implement various commands for the `storm` command line client. These implementations are very short.

[org.apache.storm.config]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/config.clj): Implementation of config reading/parsing code for Clojure. Also has utility functions for determining what local path nimbus/supervisor/daemons should be using for various things. e.g. the `master-inbox` function will return the local path that Nimbus should use when jars are uploaded to it.

[org.apache.storm.daemon.acker]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/acker.clj): Implementation of the "acker" bolt, which is a key part of how Storm guarantees data processing.

[org.apache.storm.daemon.common]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/common.clj): Implementation of common functions used in Storm daemons, like getting the id for a topology based on the name, mapping a user's topology into the one that actually executes (with implicit acking streams and acker bolt added - see `system-topology!` function), and definitions for the various heartbeat and other structures persisted by Storm.

[org.apache.storm.daemon.drpc]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/drpc.clj): Implementation of the DRPC server for use with DRPC topologies.

[org.apache.storm.daemon.nimbus]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/nimbus.clj): Implementation of Nimbus.

[org.apache.storm.daemon.supervisor]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/supervisor.clj): Implementation of Supervisor.

[org.apache.storm.daemon.task]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/task.clj): Implementation of an individual task for a spout or bolt. Handles message routing, serialization, stats collection for the UI, as well as the spout-specific and bolt-specific execution implementations.

[org.apache.storm.daemon.worker]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/worker.clj): Implementation of a worker process (which will contain many tasks within). Implements message transferring and task launching.

[org.apache.storm.event]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/event.clj): Implements a simple asynchronous function executor. Used in various places in Nimbus and Supervisor to make functions execute in serial to avoid any race conditions.

[org.apache.storm.log]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/log.clj): Defines the functions used to log messages to log4j.

[org.apache.storm.messaging.*]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/messaging): Defines a higher level interface to implementing point to point messaging. In local mode Storm uses in-memory Java queues to do this; on a cluster, it uses ZeroMQ. The generic interface is defined in protocol.clj.

[org.apache.storm.stats]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/stats.clj): Implementation of stats rollup routines used when sending stats to ZK for use by the UI. Does things like windowed and rolling aggregations at multiple granularities.

[org.apache.storm.testing]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/testing.clj): Implementation of facilities used to test Storm topologies. Includes time simulation, `complete-topology` for running a fixed set of tuples through a topology and capturing the output, tracker topologies for having fine grained control over detecting when a cluster is "idle", and other utilities.

[org.apache.storm.thrift]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/thrift.clj): Clojure wrappers around the generated Thrift API to make working with Thrift structures more pleasant.

[org.apache.storm.timer]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/timer.clj): Implementation of a background timer to execute functions in the future or on a recurring interval. Storm couldn't use the [Timer](http://docs.oracle.com/javase/1.4.2/docs/api/java/util/Timer.html) class because it needed integration with time simulation in order to be able to unit test Nimbus and the Supervisor.

[org.apache.storm.ui.*]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/ui): Implementation of Storm UI. Completely independent from rest of code base and uses the Nimbus Thrift API to get data.

[org.apache.storm.util]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/util.clj): Contains generic utility functions used throughout the code base.
 
[org.apache.storm.zookeeper]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/zookeeper.clj): Clojure wrapper around the Zookeeper API and implements some "high-level" stuff like "mkdirs" and "delete-recursive".
