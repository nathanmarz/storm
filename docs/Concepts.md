---
title: Concepts
layout: documentation
documentation: true
---

This page lists the main concepts of Storm and links to resources where you can find more information. The concepts discussed are:

1. Topologies
2. Streams
3. Spouts
4. Bolts
5. Stream groupings
6. Reliability
7. Tasks
8. Workers

### Topologies

The logic for a realtime application is packaged into a Storm topology. A Storm topology is analogous to a MapReduce job. One key difference is that a MapReduce job eventually finishes, whereas a topology runs forever (or until you kill it, of course). A topology is a graph of spouts and bolts that are connected with stream groupings. These concepts are described below.

**Resources:**

* [TopologyBuilder](javadocs/org/apache/storm/topology/TopologyBuilder.html): use this class to construct topologies in Java
* [Running topologies on a production cluster](Running-topologies-on-a-production-cluster.html)
* [Local mode](Local-mode.html): Read this to learn how to develop and test topologies in local mode.

### Streams

The stream is the core abstraction in Storm. A stream is an unbounded sequence of tuples that is processed and created in parallel in a distributed fashion. Streams are defined with a schema that names the fields in the stream's tuples. By default, tuples can contain integers, longs, shorts, bytes, strings, doubles, floats, booleans, and byte arrays. You can also define your own serializers so that custom types can be used natively within tuples.

Every stream is given an id when declared. Since single-stream spouts and bolts are so common, [OutputFieldsDeclarer](javadocs/org/apache/storm/topology/OutputFieldsDeclarer.html) has convenience methods for declaring a single stream without specifying an id. In this case, the stream is given the default id of "default".


**Resources:**

* [Tuple](javadocs/org/apache/storm/tuple/Tuple.html): streams are composed of tuples
* [OutputFieldsDeclarer](javadocs/org/apache/storm/topology/OutputFieldsDeclarer.html): used to declare streams and their schemas
* [Serialization](Serialization.html): Information about Storm's dynamic typing of tuples and declaring custom serializations

### Spouts

A spout is a source of streams in a topology. Generally spouts will read tuples from an external source and emit them into the topology (e.g. a Kestrel queue or the Twitter API). Spouts can either be __reliable__ or __unreliable__. A reliable spout is capable of replaying a tuple if it failed to be processed by Storm, whereas an unreliable spout forgets about the tuple as soon as it is emitted.

Spouts can emit more than one stream. To do so, declare multiple streams using the `declareStream` method of [OutputFieldsDeclarer](javadocs/org/apache/storm/topology/OutputFieldsDeclarer.html) and specify the stream to emit to when using the `emit` method on [SpoutOutputCollector](javadocs/org/apache/storm/spout/SpoutOutputCollector.html).

The main method on spouts is `nextTuple`. `nextTuple` either emits a new tuple into the topology or simply returns if there are no new tuples to emit. It is imperative that `nextTuple` does not block for any spout implementation, because Storm calls all the spout methods on the same thread.

The other main methods on spouts are `ack` and `fail`. These are called when Storm detects that a tuple emitted from the spout either successfully completed through the topology or failed to be completed. `ack` and `fail` are only called for reliable spouts. See [the Javadoc](javadocs/org/apache/storm/spout/ISpout.html) for more information.

**Resources:**

* [IRichSpout](javadocs/org/apache/storm/topology/IRichSpout.html): this is the interface that spouts must implement.
* [Guaranteeing message processing](Guaranteeing-message-processing.html)

### Bolts

All processing in topologies is done in bolts. Bolts can do anything from filtering, functions, aggregations, joins, talking to databases, and more. 

Bolts can do simple stream transformations. Doing complex stream transformations often requires multiple steps and thus multiple bolts. For example, transforming a stream of tweets into a stream of trending images requires at least two steps: a bolt to do a rolling count of retweets for each image, and one or more bolts to stream out the top X images (you can do this particular stream transformation in a more scalable way with three bolts than with two). 

Bolts can emit more than one stream. To do so, declare multiple streams using the `declareStream` method of [OutputFieldsDeclarer](javadocs/org/apache/storm/topology/OutputFieldsDeclarer.html) and specify the stream to emit to when using the `emit` method on [OutputCollector](javadocs/org/apache/storm/task/OutputCollector.html).

When you declare a bolt's input streams, you always subscribe to specific streams of another component. If you want to subscribe to all the streams of another component, you have to subscribe to each one individually. [InputDeclarer](javadocs/org/apache/storm/topology/InputDeclarer.html) has syntactic sugar for subscribing to streams declared on the default stream id. Saying `declarer.shuffleGrouping("1")` subscribes to the default stream on component "1" and is equivalent to `declarer.shuffleGrouping("1", DEFAULT_STREAM_ID)`.

The main method in bolts is the `execute` method which takes in as input a new tuple. Bolts emit new tuples using the [OutputCollector](javadocs/org/apache/storm/task/OutputCollector.html) object. Bolts must call the `ack` method on the `OutputCollector` for every tuple they process so that Storm knows when tuples are completed (and can eventually determine that its safe to ack the original spout tuples). For the common case of processing an input tuple, emitting 0 or more tuples based on that tuple, and then acking the input tuple, Storm provides an [IBasicBolt](javadocs/org/apache/storm/topology/IBasicBolt.html) interface which does the acking automatically.

Its perfectly fine to launch new threads in bolts that do processing asynchronously. [OutputCollector](javadocs/org/apache/storm/task/OutputCollector.html) is thread-safe and can be called at any time.

**Resources:**

* [IRichBolt](javadocs/org/apache/storm/topology/IRichBolt.html): this is general interface for bolts.
* [IBasicBolt](javadocs/org/apache/storm/topology/IBasicBolt.html): this is a convenience interface for defining bolts that do filtering or simple functions.
* [OutputCollector](javadocs/org/apache/storm/task/OutputCollector.html): bolts emit tuples to their output streams using an instance of this class
* [Guaranteeing message processing](Guaranteeing-message-processing.html)

### Stream groupings

Part of defining a topology is specifying for each bolt which streams it should receive as input. A stream grouping defines how that stream should be partitioned among the bolt's tasks.

There are eight built-in stream groupings in Storm, and you can implement a custom stream grouping by implementing the [CustomStreamGrouping](javadocs/org/apache/storm/grouping/CustomStreamGrouping.html) interface:

1. **Shuffle grouping**: Tuples are randomly distributed across the bolt's tasks in a way such that each bolt is guaranteed to get an equal number of tuples.
2. **Fields grouping**: The stream is partitioned by the fields specified in the grouping. For example, if the stream is grouped by the "user-id" field, tuples with the same "user-id" will always go to the same task, but tuples with different "user-id"'s may go to different tasks.
3. **Partial Key grouping**: The stream is partitioned by the fields specified in the grouping, like the Fields grouping, but are load balanced between two downstream bolts, which provides better utilization of resources when the incoming data is skewed. [This paper](https://melmeric.files.wordpress.com/2014/11/the-power-of-both-choices-practical-load-balancing-for-distributed-stream-processing-engines.pdf) provides a good explanation of how it works and the advantages it provides.
4. **All grouping**: The stream is replicated across all the bolt's tasks. Use this grouping with care.
5. **Global grouping**: The entire stream goes to a single one of the bolt's tasks. Specifically, it goes to the task with the lowest id.
6. **None grouping**: This grouping specifies that you don't care how the stream is grouped. Currently, none groupings are equivalent to shuffle groupings. Eventually though, Storm will push down bolts with none groupings to execute in the same thread as the bolt or spout they subscribe from (when possible).
7. **Direct grouping**: This is a special kind of grouping. A stream grouped this way means that the __producer__ of the tuple decides which task of the consumer will receive this tuple. Direct groupings can only be declared on streams that have been declared as direct streams. Tuples emitted to a direct stream must be emitted using one of the [emitDirect](javadocs/org/apache/storm/task/OutputCollector.html#emitDirect(int, int, java.util.List) methods. A bolt can get the task ids of its consumers by either using the provided [TopologyContext](javadocs/org/apache/storm/task/TopologyContext.html) or by keeping track of the output of the `emit` method in [OutputCollector](javadocs/org/apache/storm/task/OutputCollector.html) (which returns the task ids that the tuple was sent to).
8. **Local or shuffle grouping**: If the target bolt has one or more tasks in the same worker process, tuples will be shuffled to just those in-process tasks. Otherwise, this acts like a normal shuffle grouping.

**Resources:**

* [TopologyBuilder](javadocs/org/apache/storm/topology/TopologyBuilder.html): use this class to define topologies
* [InputDeclarer](javadocs/org/apache/storm/topology/InputDeclarer.html): this object is returned whenever `setBolt` is called on `TopologyBuilder` and is used for declaring a bolt's input streams and how those streams should be grouped

### Reliability

Storm guarantees that every spout tuple will be fully processed by the topology. It does this by tracking the tree of tuples triggered by every spout tuple and determining when that tree of tuples has been successfully completed. Every topology has a "message timeout" associated with it. If Storm fails to detect that a spout tuple has been completed within that timeout, then it fails the tuple and replays it later. 

To take advantage of Storm's reliability capabilities, you must tell Storm when new edges in a tuple tree are being created and tell Storm whenever you've finished processing an individual tuple. These are done using the [OutputCollector](javadocs/org/apache/storm/task/OutputCollector.html) object that bolts use to emit tuples. Anchoring is done in the `emit` method, and you declare that you're finished with a tuple using the `ack` method.

This is all explained in much more detail in [Guaranteeing message processing](Guaranteeing-message-processing.html). 

### Tasks

Each spout or bolt executes as many tasks across the cluster. Each task corresponds to one thread of execution, and stream groupings define how to send tuples from one set of tasks to another set of tasks. You set the parallelism for each spout or bolt in the `setSpout` and `setBolt` methods of [TopologyBuilder](javadocs/org/apache/storm/topology/TopologyBuilder.html).

### Workers

Topologies execute across one or more worker processes. Each worker process is a physical JVM and executes a subset of all the tasks for the topology. For example, if the combined parallelism of the topology is 300 and 50 workers are allocated, then each worker will execute 6 tasks (as threads within the worker). Storm tries to spread the tasks evenly across all the workers.

**Resources:**

* [Config.TOPOLOGY_WORKERS](javadocs/org/apache/storm/Config.html#TOPOLOGY_WORKERS): this config sets the number of workers to allocate for executing the topology
