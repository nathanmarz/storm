---
title: Trident Spouts
layout: documentation
documentation: true
---
# Trident spouts

Like in the vanilla Storm API, spouts are the source of streams in a Trident topology. On top of the vanilla Storm spouts, Trident exposes additional APIs for more sophisticated spouts.

There is an inextricable link between how you source your data streams and how you update state (e.g. databases) based on those data streams. See [Trident state doc](Trident-state.html) for an explanation of this – understanding this link is imperative for understanding the spout options available.

Regular Storm spouts will be non-transactional spouts in a Trident topology. To use a regular Storm IRichSpout, create the stream like this in a TridentTopology:

```java
TridentTopology topology = new TridentTopology();
topology.newStream("myspoutid", new MyRichSpout());
```

All spouts in a Trident topology are required to be given a unique identifier for the stream – this identifier must be unique across all topologies run on the cluster. Trident will use this identifier to store metadata about what the spout has consumed in Zookeeper, including the txid and any metadata associated with the spout.

You can configure the Zookeeper storage of spout metadata via the following configuration options:

1. `transactional.zookeeper.servers`: A list of Zookeeper hostnames 
2. `transactional.zookeeper.port`: The port of the Zookeeper cluster
3. `transactional.zookeeper.root`: The root dir in Zookeeper where metadata is stored. Metadata will be stored at the path <root path>/<spout id>

## Pipelining

By default, Trident processes a single batch at a time, waiting for the batch to succeed or fail before trying another batch. You can get significantly higher throughput – and lower latency of processing of each batch – by pipelining the batches. You configure the maximum amount of batches to be processed simultaneously with the "topology.max.spout.pending" property. 

Even while processing multiple batches simultaneously, Trident will order any state updates taking place in the topology among batches. For example, suppose you're doing a global count aggregation into a database. The idea is that while you're updating the count in the database for batch 1, you can still be computing the partial counts for batches 2 through 10. Trident won't move on to the state updates for batch 2 until the state updates for batch 1 have succeeded. This is essential for achieving exactly-once processing semantics, as outline in [Trident state doc](Trident-state.html).

## Trident spout types

Here are the following spout APIs available:

1. [ITridentSpout]({{page.git-blob-base}}/storm-core/src/jvm/storm/trident/spout/ITridentSpout.java): The most general API that can support transactional or opaque transactional semantics. Generally you'll use one of the partitioned flavors of this API rather than this one directly.
2. [IBatchSpout]({{page.git-blob-base}}/storm-core/src/jvm/storm/trident/spout/IBatchSpout.java): A non-transactional spout that emits batches of tuples at a time
3. [IPartitionedTridentSpout]({{page.git-blob-base}}/storm-core/src/jvm/storm/trident/spout/IPartitionedTridentSpout.java): A transactional spout that reads from a partitioned data source (like a cluster of Kafka servers)
4. [IOpaquePartitionedTridentSpout]({{page.git-blob-base}}/storm-core/src/jvm/storm/trident/spout/IOpaquePartitionedTridentSpout.java): An opaque transactional spout that reads from a partitioned data source

And, like mentioned in the beginning of this tutorial, you can use regular IRichSpout's as well.
 

