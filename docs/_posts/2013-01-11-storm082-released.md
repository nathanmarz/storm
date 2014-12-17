---
layout: post
title: Storm 0.8.2 released
author: Nathan Marz
---

Storm 0.8.2 has been released and is available from [the downloads page](/downloads.html). This release contains a ton of improvements and fixes and is a highly recommended upgrade for everyone.

Isolation Scheduler
-------------------

The highlight of this release is the new "Isolation scheduler" that makes it easy and safe to share a cluster among many topologies. The isolation scheduler lets you specify which topologies should be "isolated", meaning that they run on a dedicated set of machines within the cluster where no other topologies will be running. These isolated topologies are given priority on the cluster, so resources will be allocated to isolated topologies if there's competition with non-isolated topologies, and resources will be taken away from non-isolated topologies if necessary to get resources for an isolated topology. Once all isolated topologies are allocated, the remaining machines on the cluster are shared among all non-isolated topologies.

You configure the isolation scheduler in the Nimbus configuration. Set "storm.scheduler" to "backtype.storm.scheduler.IsolationScheduler". Then, use the "isolation.scheduler.machines" config to specify how many machines each topology should get. This config is a map from topology name to number of machines. For example:

<script src="https://gist.github.com/4514691.js"></script>

Any topologies submitted to the cluster not listed there will not be isolated. Note that there is no way for a user of Storm to affect their isolation settings – this is only allowed by the administrator of the cluster (this is very much intentional).

The isolation scheduler solves the multi-tenancy problem – avoiding resource contention between topologies – by providing full isolation between topologies. The intention is that "productionized" topologies should be listed in the isolation config, and test or in-development topologies should not. The remaining machines on the cluster serve the dual role of failover for isolated topologies and for running the non-isolated topologies.

Storm UI improvements
-------------------

The Storm UI has also been made significantly more useful. There are new stats "#executed", "execute latency", and "capacity" tracked for all bolts. The "capacity" metric is very useful and tells you what % of the time in the last 10 minutes the bolt spent executing tuples. If this value is close to 1, then the bolt is "at capacity" and is a bottleneck in your topology. The solution to at-capacity bolts is to increase the parallelism of that bolt.

Another useful improvement is the ability to kill, activate, deactivate, and rebalance topologies from the Storm UI.


Important bug fixes
-------------------

There are also a few important bug fixes in this release. We fixed two bugs that could cause a topology to freeze up and stop processing. One of these bugs was extremely unlikely to hit, but the other one was a regression in 0.8.1 and there was a small chance of hitting it anytime a worker was restarted.


Changelog
---------

 * Added backtype.storm.scheduler.IsolationScheduler. This lets you run topologies that are completely isolated at the machine level. Configure Nimbus to isolate certain topologies, and how many machines to give to each of those topologies, with the isolation.scheduler.machines config in Nimbus's storm.yaml. Topologies run on the cluster that are not listed there will share whatever remaining machines there are on the cluster after machines are allocated to the listed topologies.
 * Storm UI now uses nimbus.host to find Nimbus rather than always using localhost (thanks Frostman)
 * Added report-error! to Clojure DSL
 * Automatically throttle errors sent to Zookeeper/Storm UI when too many are reported in a time interval (all errors are still logged) Configured with TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL and TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS
 * Kryo instance used for serialization can now be controlled via IKryoFactory interface and TOPOLOGY_KRYO_FACTORY config
 * Add ability to plug in custom code into Nimbus to allow/disallow topologies to be submitted via NIMBUS_TOPOLOGY_VALIDATOR config
 * Added TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS config to control how often a batch can be emitted in a Trident topology. Defaults to 500 milliseconds. This is used to prevent too much load from being placed on Zookeeper in the case that batches are being processed super quickly.
 * Log any topology submissions errors in nimbus.log
 * Add static helpers in Config when using regular maps
 * Make Trident much more memory efficient during failures by immediately removing state for failed attempts when a more recent attempt is seen
 * Add ability to name portions of a Trident computation and have those names appear in the Storm UI
 * Show Nimbus and topology configurations through Storm UI (thanks rnfein)
 * Added ITupleCollection interface for TridentState's and TupleCollectionGet QueryFunction for getting the full contents of a state. MemoryMapState and LRUMemoryMapState implement this
 * Can now submit a topology in inactive state. Storm will wait to call open/prepare on the spouts/bolts until it is first activated.
 * Can now activate, deactive, rebalance, and kill topologies from the Storm UI (thanks Frostman)
 * Can now use --config option to override which yaml file from ~/.storm to use for the config (thanks tjun)
 * Redesigned the pluggable resource scheduler (INimbus, ISupervisor) interfaces to allow for much simpler integrations
 * Added prepare method to IScheduler
 * Added "throws Exception" to TestJob interface
 * Added reportError to multilang protocol and updated Python and Ruby adapters to use it (thanks Lazyshot)
 * Number tuples executed now tracked and shown in Storm UI
 * Added ReportedFailedException which causes a batch to fail without killing worker and reports the error to the UI
 * Execute latency now tracked and shown in Storm UI
 * Adding testTuple methods for easily creating Tuple instances to Testing API (thanks xumingming)
 * Trident now throws an error during construction of a topology when try to select fields that don't exist in a stream (thanks xumingming)
 * Compute the capacity of a bolt based on execute latency and #executed over last 10 minutes and display in UI
 * Storm UI displays exception instead of blank page when there's an error rendering the page (thanks Frostman)
 * Added MultiScheme interface (thanks sritchie)
 * Added MockTridentTuple for testing (thanks emblem)
 * Add whitelist methods to Cluster to allow only a subset of hosts to be revealed as available slots
 * Updated Trident Debug filter to take in an identifier to use when logging (thanks emblem)
 * Number of DRPC server worker threads now customizable (thanks xiaokang)
 * DRPC server now uses a bounded queue for requests to prevent being overloaded with requests (thanks xiaokang)
 * Add __hash__ method to all generated Python Thrift objects so that Python code can read Nimbus stats which use Thrift objects as dict keys
 * Bug fix: Fix for bug that could cause topology to hang when ZMQ blocks sending to a worker that got reassigned
 * Bug fix: Fix deadlock bug due to variant of dining philosophers problem. Spouts now use an overflow buffer to prevent blocking and guarantee that it can consume the incoming queue of acks/fails.
 * Bug fix: Fix race condition in supervisor that would lead to supervisor continuously crashing due to not finding "stormconf.ser" file for an already killed topology
 * Bug fix: bin/storm script now displays a helpful error message when an invalid command is specified
 * Bug fix: fixed NPE when emitting during emit method of Aggregator
 * Bug fix: URLs with periods in them in Storm UI now route correctly
 * Bug fix: Fix occasional cascading worker crashes due when a worker dies due to not removing connections from connection cache appropriately
 


