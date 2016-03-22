---
title: FAQ
layout: documentation
documentation: true
---

## Best Practices

### What rules of thumb can you give me for configuring Storm+Trident?

* number of workers a multiple of number of machines; parallelism a multiple of number of workers; number of kafka partitions a multiple of number of spout parallelism
* Use one worker per topology per machine
* Start with fewer, larger aggregators, one per machine with workers on it
* Use the isolation scheduler
* Use one acker per worker -- 0.9 makes that the default, but earlier versions do not.
* enable GC logging; you should see very few major GCs if things are in reasonable shape.
* set the trident batch millis to about 50% of your typical end-to-end latency.
* Start with a max spout pending that is for sure too small -- one for trident, or the number of executors for storm -- and increase it until you stop seeing changes in the flow. You'll probably end up with something near `2*(throughput in recs/sec)*(end-to-end latency)` (2x the Little's law capacity).

### What are some of the best ways to get a worker to mysteriously and bafflingly die?

* Do you have write access to the log directory
* Are you blowing out your heap?
* Are all the right libraries installed on all of the workers?
* Is the zookeeper hostname still set to localhost?
* Did you supply a correct, unique hostname -- one that resolves back to the machine -- to each worker, and put it in the storm conf file?
* Have you opened firewall/securitygroup permissions _bidirectionally_ among a) all the workers, b) the storm master, c) zookeeper? Also, from the workers to any kafka/kestrel/database/etc that your topology accesses? Use netcat to poke the appropriate ports and be sure. 

### Halp! I cannot see:

* **my logs** Logs by default go to $STORM_HOME/logs. Check that you have write permissions to that directory. They are configured in log4j2/{cluster, worker}.xml.
* **final JVM settings** Add the `-XX+PrintFlagsFinal` commandline option in the childopts (see the conf file)
* **final Java system properties** Add `Properties props = System.getProperties(); props.list(System.out);` near where you build your topology.

### How many Workers should I use?

The total number of workers is set by the supervisors -- there's some number of JVM slots each supervisor will superintend. The thing you set on the topology is how many worker slots it will try to claim.

There's no great reason to use more than one worker per topology per machine.

With one topology running on three 8-core nodes, and parallelism hint 24, each bolt gets 8 executors per machine, i.e. one for each core. There are three big benefits to running three workers (with 8 assigned executors each) compare to running say 24 workers (one assigned executor each).

First, data that is repartitioned (shuffles or group-bys) to executors in the same worker will not have to hit the transfer buffer. Instead, tuples are deposited directly from send to receive buffer. That's a big win. By contrast, if the destination executor were on the same machine in a different worker, it would have to go send -> worker transfer -> local socket -> worker recv -> exec recv buffer. It doesn't hit the network card, but it's not as big a win as when executors are in the same worker.

Second, you're typically better off with three aggregators having very large backing cache than having twenty-four aggregators having small backing caches. This reduces the effect of skew, and improves LRU efficiency.

Lastly, fewer workers reduces control flow chatter.

## Topology

### Can a Trident topology have Multiple Streams?

> Can a Trident Topology work like a workflow with conditional paths (if-else)? e.g. A Spout (S1) connects to a bolt (B0) which based on certain values in the incoming tuple routes them to either bolt (B1) or bolt (B2) but not both.

A Trident "each" operator returns a Stream object, which you can store in a variable. You can then run multiple eaches on the same Stream to split it, e.g.: 

        Stream s = topology.each(...).groupBy(...).aggregate(...) 
        Stream branch1 = s.each(..., FilterA) 
        Stream branch2 = s.each(..., FilterB) 

You can join streams with join, merge or multiReduce.

At time of writing, you can't emit to multiple output streams from Trident -- see [STORM-68](https://issues.apache.org/jira/browse/STORM-68)

### Why am I getting a NotSerializableException/IllegalStateException when my topology is being started up?

Within the Storm lifecycle, the topology is instantiated and then serialized to byte format to be stored in ZooKeeper, prior to the topology being executed. Within this step, if a spout or bolt within the topology has an initialized unserializable property, serialization will fail. If there is a need for a field that is unserializable, initialize it within the bolt or spout's prepare method, which is run after the topology is delivered to the worker.

## Spouts

### What is a coordinator, and why are there several?

A trident-spout is actually run within a storm _bolt_. The storm-spout of a trident topology is the MasterBatchCoordinator -- it coordinates trident batches and is the same no matter what spouts you use. A batch is born when the MBC dispenses a seed tuple to each of the spout-coordinators. The spout-coordinator bolts know how your particular spouts should cooperate -- so in the kafka case, it's what helps figure out what partition and offset range each spout should pull from.

### What can I store into the spout's metadata record?

You should only store static data, and as little of it as possible, into the metadata record (note: maybe you _can_ store more interesting things; you shouldn't, though)

### How often is the 'emitPartitionBatchNew' function called?

Since the MBC is the actual spout, all the tuples in a batch are just members of its tupletree. That means storm's "max spout pending" config effectively defines the number of concurrent batches trident runs. The MBC emits a new batch if it has fewer than max-spending tuples pending and if at least one [trident batch interval]({{page.git-blob-base}}/conf/defaults.yaml#L115)'s worth of seconds has passed since the last batch.

### If nothing was emitted does Trident slow down the calls?

Yes, there's a pluggable "spout wait strategy"; the default is to sleep for a [configurable amount of time]({{page.git-blob-base}}/conf/defaults.yaml#L110)

### OK, then what is the trident batch interval for?

You know how computers of the 486 era had a [turbo button](http://en.wikipedia.org/wiki/Turbo_button) on them? It's like that. 

Actually, it has two practical uses. One is to throttle spouts that poll a remote source without throttling processing. For example, we have a spout that looks in a given S3 bucket for a new batch-uploaded file to read, linebreak and emit. We don't want it hitting S3 more than every few seconds: files don't show up more than once every few minutes, and a batch takes a few seconds to process.

The other is to limit overpressure on the internal queues during startup or under a heavy burst load -- if the spouts spring to life and suddenly jam ten batches' worth of records into the system, you could have a mass of less-urgent tuples from batch 7 clog up the transfer buffer and prevent the $commit tuple from batch 3 to get through (or even just the regular old tuples from batch 3). What we do is set the trident batch interval to about half the typical end-to-end processing latency -- if it takes 600ms to process a batch, it's OK to only kick off a batch every 300ms.

Note that this is a cap, not an additional delay -- with a period of 300ms, if your batch takes 258ms Trident will only delay an additional 42ms.

### How do you set the batch size?

Trident doesn't place its own limits on the batch count. In the case of the Kafka spout, the max fetch bytes size divided by the average record size defines an effective records per subbatch partition.

### How do I resize a batch?

The trident batch is a somewhat overloaded facility. Together with the number of partitions, the batch size is constrained by or serves to define

1. the unit of transactional safety (tuples at risk vs time)
2. per partition, an effective windowing mechanism for windowed stream analytics
3. per partition, the number of simultaneous queries that will be made by a partitionQuery, partitionPersist, etc;
4. per partition, the number of records convenient for the spout to dispatch at the same time;

You can't change the overall batch size once generated, but you can change the number of partitions -- do a shuffle and then change the parallelism hint

## Time Series

### How do I aggregate events by time?

If you have records with an immutable timestamp, and you would like to count, average or otherwise aggregate them into discrete time buckets, Trident is an excellent and scalable solution.

Write an `Each` function that turns the timestamp into a time bucket: if the bucket size was "by hour", then the timestamp `2013-08-08 12:34:56` would be mapped to the `2013-08-08 12:00:00` time bucket, and so would everything else in the twelve o'clock hour. Then group on that timebucket and use a grouped persistentAggregate. The persistentAggregate uses a local cacheMap backed by a data store. Groups with many records require very few reads from the data store, and use efficient bulk reads and writes; as long as your data feed is relatively prompt Trident will make very efficient use of memory and network. Even if a server drops off line for a day, then delivers that full day's worth of data in a rush, the old results will be calmly retrieved and updated -- and without interfering with calculating the current results.

### How can I know that all records for a time bucket have been received?

You cannot know that all events are collected -- this is an epistemological challenge, not a distributed systems challenge. You can:

* Set a time limit using domain knowledge
* Introduce a _punctuation_: a record known to come after all records in the given time bucket. Trident uses this scheme to know when a batch is complete. If you for instance receive records from a set of sensors, each in order for that sensor, then once all sensors have sent you a 3:02:xx or later timestamp lets you know you can commit. 
* When possible, make your process incremental: each value that comes in makes the answer more an more true. A Trident ReducerAggregator is an operator that takes a prior result and a set of new records and returns a new result. This lets the result be cached and serialized to a datastore; if a server drops off line for a day and then comes back with a full day's worth of data in a rush, the old results will be calmly retrieved and updated.
* Lambda architecture: Record all events into an archival store (S3, HBase, HDFS) on receipt. in the fast layer, once the time window is clear, process the bucket to get an actionable answer, and ignore everything older than the time window. Periodically run a global aggregation to calculate a "correct" answer.
