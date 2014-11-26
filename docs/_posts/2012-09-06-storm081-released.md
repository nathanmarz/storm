---
layout: post
title: Storm 0.8.1 released
author: Nathan Marz
---

Storm 0.8.1 is now available on the downloads page and in Maven. This release contains many bug fixes as well as a few important new features. These include: 

Storm's unit testing facilities have been exposed via Java
-----------------------
This is an extremely powerful API that lets you do things like: 
   a) Easily bring up and tear down local clusters 
   b) Run a fixed set of tuples through a topology and see all the tuples emitted by all components 
   c) Feed some tuples through the topology, wait until they've been processed, and then run your assertions 
   d) Use time simulation and step through time via an API. This is useful for testing topologies that do things based on time advancing. You can see examples of the unit testing API [here](https://github.com/xumingming/storm-lib/blob/master/src/jvm/storm/TestingApiDemo.java).
   
Spout wait strategies
---------------------

There's two situations in which a spout needs to wait. The first is when the max spout pending limit is reached. The second is when nothing is emitted from nextTuple. Previously, Storm would just have that spout sit in a busy loop in those cases. What Storm does in those situations is now pluggable, and the default is now for the spout to sleep for 1 ms. This will cause the spout to use dramatically less CPU when it hits those cases, and it also obviates the need for spouts to do any sleeping in their implementation to be "polite". The wait strategy can be configured with TOPOLOGY_SPOUT_WAIT_STRATEGY and can be configured on a spout by spout basis. The interface to implement for a wait strategy is backtype.storm.spout.ISpoutWaitStrategy 

The full changelog is below: 

 * Exposed Storm's unit testing facilities via the backtype.storm.Testing class. Notable functions are Testing/withLocalCluster and Testing/completeTopology 
 * Implemented pluggable spout wait strategy that is invoked when a spout emits nothing from nextTuple or when a spout hits the MAX_SPOUT_PENDING limit 
 * Spouts now have a default wait strategy of a 1 millisecond sleep 
 * Changed log level of "Failed message" logging to DEBUG 
 * Deprecated LinearDRPCTopologyBuilder, TimeCacheMap, and transactional topologies 
 * During "storm jar", whether topology is already running or not is checked before submitting jar to save time (thanks jasonjckn) 
 * Added BaseMultiReducer class to Trident that provides empty implementations of prepare and cleanup 
 * Added Negate builtin operation to reverse a Filter 
 * Added topology.kryo.decorators config that allows functions to be plugged in to customize Kryo (thanks jasonjckn) 
 * Enable message timeouts when using LocalCluster 
 * Multilang subprocesses can set "need_task_ids" to false when emitting tuples to tell Storm not to send task ids back (performance optimization) (thanks barrywhart) 
 * Add contains method on Tuple (thanks okapies) 
 * Added ISchemableSpout interface 
 * Bug fix: When an item is consumed off an internal buffer, the entry on the buffer is nulled to allow GC to happen on that data 
 * Bug fix: Helper class for Trident MapStates now clear their read cache when a new commit happens, preventing updates from spilling over from a failed batch attempt to the next attempt 
 * Bug fix: Fix NonTransactionalMap to take in an IBackingMap for regular values rather than TransactionalValue (thanks sjoerdmulder) 
 * Bug fix: Fix NPE when no input fields given for regular Aggregator 
 * Bug fix: Fix IndexOutOfBoundsExceptions when a bolt for global aggregation had a parallelism greater than 1 (possible with splitting, stateQuerying, and multiReduce) 
 * Bug fix: Fix "fields size" error that would sometimes occur when splitting a stream with multiple eaches 
 * Bug fix: Fix bug where a committer spout (including opaque spouts) could cause Trident batches to fail 
 * Bug fix: Fix Trident bug where multiple groupings on same stream would cause tuples to be duplicated to all consumers 
 * Bug fix: Fixed error when repartitioning stream twice in a row without any operations in between 
 * Bug fix: Fix rare bug in supervisor where it would continuously fail to clean up workers because the worker was already partially cleaned up 
 * Bug fix: Fix emitDirect in storm.py 
