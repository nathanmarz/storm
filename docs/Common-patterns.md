---
layout: documentation
---

This page lists a variety of common patterns in Storm topologies.

1. Streaming joins
2. Batching
3. BasicBolt
4. In-memory caching + fields grouping combo
5. Streaming top N
6. TimeCacheMap for efficiently keeping a cache of things that have been recently updated
7. CoordinatedBolt and KeyedFairBolt for Distributed RPC

### Joins

A streaming join combines two or more data streams together based on some common field. Whereas a normal database join has finite input and clear semantics for a join, a streaming join has infinite input and unclear semantics for what a join should be.

The join type you need will vary per application. Some applications join all tuples for two streams over a finite window of time, whereas other applications expect exactly one tuple for each side of the join for each join field. Other applications may do the join completely differently. The common pattern among all these join types is partitioning multiple input streams in the same way. This is easily accomplished in Storm by using a fields grouping on the same fields for many input streams to the joiner bolt. For example:

```java
builder.setBolt("join", new MyJoiner(), parallelism)
  .fieldsGrouping("1", new Fields("joinfield1", "joinfield2"))
  .fieldsGrouping("2", new Fields("joinfield1", "joinfield2"))
  .fieldsGrouping("3", new Fields("joinfield1", "joinfield2"));
```

The different streams don't have to have the same field names, of course.


### Batching

Oftentimes for efficiency reasons or otherwise, you want to process a group of tuples in batch rather than individually. For example, you may want to batch updates to a database or do a streaming aggregation of some sort.

If you want reliability in your data processing, the right way to do this is to hold on to tuples in an instance variable while the bolt waits to do the batching. Once you do the batch operation, you then ack all the tuples you were holding onto.

If the bolt emits tuples, then you may want to use multi-anchoring to ensure reliability. It all depends on the specific application. See [Guaranteeing message processing](Guaranteeing-message-processing.html) for more details on how reliability works.

### BasicBolt
Many bolts follow a similar pattern of reading an input tuple, emitting zero or more tuples based on that input tuple, and then acking that input tuple immediately at the end of the execute method. Bolts that match this pattern are things like functions and filters. This is such a common pattern that Storm exposes an interface called [IBasicBolt](javadocs/backtype/storm/topology/IBasicBolt.html) that automates this pattern for you. See [Guaranteeing message processing](Guaranteeing-message-processing.html) for more information.

### In-memory caching + fields grouping combo

It's common to keep caches in-memory in Storm bolts. Caching becomes particularly powerful when you combine it with a fields grouping. For example, suppose you have a bolt that expands short URLs (like bit.ly, t.co, etc.) into long URLs. You can increase performance by keeping an LRU cache of short URL to long URL expansions to avoid doing the same HTTP requests over and over. Suppose component "urls" emits short URLS, and component "expand" expands short URLs into long URLs and keeps a cache internally. Consider the difference between the two following snippets of code:

```java
builder.setBolt("expand", new ExpandUrl(), parallelism)
  .shuffleGrouping(1);
```

```java
builder.setBolt("expand", new ExpandUrl(), parallelism)
  .fieldsGrouping("urls", new Fields("url"));
```

The second approach will have vastly more effective caches, since the same URL will always go to the same task. This avoids having duplication across any of the caches in the tasks and makes it much more likely that a short URL will hit the cache.

### Streaming top N

A common continuous computation done on Storm is a "streaming top N" of some sort. Suppose you have a bolt that emits tuples of the form ["value", "count"] and you want a bolt that emits the top N tuples based on count. The simplest way to do this is to have a bolt that does a global grouping on the stream and maintains a list in memory of the top N items.

This approach obviously doesn't scale to large streams since the entire stream has to go through one task. A better way to do the computation is to do many top N's in parallel across partitions of the stream, and then merge those top N's together to get the global top N. The pattern looks like this:

```java
builder.setBolt("rank", new RankObjects(), parallellism)
  .fieldsGrouping("objects", new Fields("value"));
builder.setBolt("merge", new MergeObjects())
  .globalGrouping("rank");
```

This pattern works because of the fields grouping done by the first bolt which gives the partitioning you need for this to be semantically correct. You can see an example of this pattern in storm-starter [here](https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/RollingTopWords.java).


### TimeCacheMap for efficiently keeping a cache of things that have been recently updated

You sometimes want to keep a cache in memory of items that have been recently "active" and have items that have been inactive for some time be automatically expires. [TimeCacheMap](javadocs/backtype/storm/utils/TimeCacheMap.html) is an efficient data structure for doing this and provides hooks so you can insert callbacks whenever an item is expired.

### CoordinatedBolt and KeyedFairBolt for Distributed RPC

When building distributed RPC applications on top of Storm, there are two common patterns that are usually needed. These are encapsulated by [CoordinatedBolt](javadocs/backtype/storm/task/CoordinatedBolt.html) and [KeyedFairBolt](javadocs/backtype/storm/task/KeyedFairBolt.html) which are part of the "standard library" that ships with the Storm codebase.

`CoordinatedBolt` wraps the bolt containing your logic and figures out when your bolt has received all the tuples for any given request. It makes heavy use of direct streams to do this.

`KeyedFairBolt` also wraps the bolt containing your logic and makes sure your topology processes multiple DRPC invocations at the same time, instead of doing them serially one at a time.

See [Distributed RPC](Distributed-RPC.html) for more details.
