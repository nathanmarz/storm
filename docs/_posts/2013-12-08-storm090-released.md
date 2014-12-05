---
layout: post
title: Storm 0.9.0 Released
author: P. Taylor Goetz
---

We are pleased to announce that Storm 0.9.0 has been released and is available from [the downloads page](/downloads.html). This release represents an important milestone in the evolution of Storm.

While a number of new features have been added, a key focus area for this release has been stability-related fixes. Though many users are successfully running work-in-progress builds for Storm 0.9.x in production, this release represents the most stable version to-date, and is highly recommended for everyone, especially users of 0.8.x versions.


Netty Transport
---------------
The first hightlight of this release is the new [Netty](http://netty.io/index.html) Transport contributed by [Yahoo! Engineering](http://yahooeng.tumblr.com/). Storm's core network transport mechanism is now plugable, and Storm now comes with two implementations: The original 0MQ transport, and a new Netty-based implementation.

In earlier versions, Storm relied solely on 0MQ for transport. Since 0MQ is a native library, it was highly platform-dependent and, at times, challenging to install properly. In addition, stability between versions varied widely between versions and only a relatively old 0MQ version (2.1.7) was certified to work with Storm.

The Netty transport offers a pure Java alternative that eliminates Storm's dependency on native libraries. The Netty transport's performance is up to twice as fast as 0MQ, and it will open the door for authorization and authentication between worker processes. For an in-depth performance comparison of the 0MQ and Netty transports, see [this blog post](http://yahooeng.tumblr.com/post/64758709722/making-storm-fly-with-netty) by Storm contributor [Bobby Evans](https://github.com/revans2).

To configure Storm to use the Netty transport simply add the following to your `storm.yaml` configuration and adjust the values to best suit your use-case:

```
storm.messaging.transport: "backtype.storm.messaging.netty.Context"
storm.messaging.netty.server_worker_threads: 1
storm.messaging.netty.client_worker_threads: 1
storm.messaging.netty.buffer_size: 5242880
storm.messaging.netty.max_retries: 100
storm.messaging.netty.max_wait_ms: 1000
storm.messaging.netty.min_wait_ms: 100
```
You can also write your own transport implementation by implementing the [`backtype.storm.messaging.IContext`](https://github.com/apache/incubator-storm/blob/master/storm-core/src/jvm/backtype/storm/messaging/IContext.java) interface.


Log Viewer UI
-------------
Storm now includes a helpful new feature for debugging and monitoring topologies: The `logviewer` daemon.

In earlier versions of Storm, viewing worker logs involved determining a worker's location (host/port), typically through Storm UI, then `ssh`ing to that host and `tail`ing the corresponding worker log file. With the new log viewer. You can now easily access a specific worker's log in a web browser by clicking on a worker's port number right from Storm UI.

The `logviewer` daemon runs as a separate process on Storm supervisor nodes. To enable the `logviewer` run the following command (under supervision) on your cluster's supervisor nodes:

```
$ storm logviewer
```


Improved Windows Support
------------------------
In previous versions, running Storm on Microsoft Windows required installing third-party binaries (0MQ), modifying Storm's source, and adding Windows-specific scripts. With the addition of the platform-independent Netty transport, as well as numerous enhancements to make Storm more platform-independent, running Storm on Windows is easier than ever.


Security Improvements
---------------------
Security, Authentication, and Authorization have been and will continue to be important focus areas for upcoming features. Storm 0.9.0 introduces an API for pluggable tuple serialization and a blowfish encryption based implementation for encrypting tuple data for sensitive use cases.


API Compatibility and Upgrading
-------------------------------
For most Storm topology developers, upgrading to 0.9.0 is simply a matter of updating the [dependency](https://clojars.org/storm). Storm's core API has changed very little since the 0.8.2 release.

On the devops side, when upgrading to a new Storm release, it is safest to clear any existing state (Zookeeper, `storm.local.dir`), prior to upgrading.

Logging Changes
---------------
Another important change in 0.9.0 has to do with logging. Storm has largely switched over to the [slf4j API](http://www.slf4j.org) (backed by a [logback](http://logback.qos.ch) logger implementation). Some Storm dependencies rely on the log4j API, so Storm currently depends on [log4j-over-slf4j](http://www.slf4j.org/legacy.html#log4j-over-slf4j).

These changes have implications for existing topologies and topology components that use the log4j API.

In general, and when possible, Storm topologies and topology components should use the [slf4j API](http://www.slf4j.org) for logging.


Thanks
------
Special thanks are due to all those who have contributed to Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.


Changelog
---------

* Update build configuration to force compatibility with Java 1.6
* Fixed a netty client issue where sleep times for reconnection could be negative (thanks brndnmtthws)
* Fixed an issue that would cause storm-netty unit tests to fail
* Added configuration to limit ShellBolt internal _pendingWrites queue length (thanks xiaokang)
* Fixed a a netty client issue where sleep times for reconnection could be negative (thanks brndnmtthws)
* Fixed a display issue with system stats in Storm UI (thanks d2r)
* Nimbus now does worker heartbeat timeout checks as soon as heartbeats are updated (thanks d2r)
* The logviewer now determines log file location by examining the logback configuration (thanks strongh)
* Allow tick tuples to work with the system bolt (thanks xumingming)
* Add default configuration values for the netty transport and the ability to configure the number of worker threads (thanks revans2)
* Added timeout to unit tests to prevent a situation where tests would hang indefinitely (thanks d2r)
* Fixed an issue in the system bolt where local mode would not be detected accurately (thanks miofthena)
* Fixed `storm jar` command to work properly when STORM_JAR_JVM_OPTS is not specified (thanks roadkill001)
* All logging now done with slf4j
* Replaced log4j logging system with logback
* Logs are now limited to 1GB per worker (configurable via logging configuration file)
* Build upgraded to leiningen 2.0
* Revamped Trident spout interfaces to support more dynamic spouts, such as a spout who reads from a changing set of brokers
* How tuples are serialized is now pluggable (thanks anfeng)
* Added blowfish encryption based tuple serialization (thanks anfeng)
* Have storm fall back to installed storm.yaml (thanks revans2)
* Improve error message when Storm detects bundled storm.yaml to show the URL's for offending resources (thanks revans2)
* Nimbus throws NotAliveException instead of FileNotFoundException from various query methods when topology is no longer alive (thanks revans2)
* Escape HTML and Javascript appropriately in Storm UI (thanks d2r)
* Storm's Zookeeper client now uses bounded exponential backoff strategy on failures
* Automatically drain and log error stream of multilang subprocesses
* Append component name to thread name of running executors so that logs are easier to read
* Messaging system used for passing messages between workers is now pluggable (thanks anfeng)
* Netty implementation of messaging (thanks anfeng)
* Include topology id, worker port, and worker id in properties for worker processes, useful for logging (thanks d2r)
* Tick tuples can now be scheduled using floating point seconds (thanks tscurtu)
* Added log viewer daemon and links from UI to logviewers (thanks xiaokang)
* DRPC server childopts now configurable (thanks strongh)
* Default number of ackers to number of workers, instead of just one (thanks lyogavin)
* Validate that Storm configs are of proper types/format/structure (thanks d2r)
* FixedBatchSpout will now replay batches appropriately on batch failure (thanks ptgoetz)
* Can set JAR_JVM_OPTS env variable to add jvm options when calling 'storm jar' (thanks srmelody)
* Throw error if batch id for transaction is behind the batch id in the opaque value (thanks mrflip)
* Sort topologies by name in UI (thanks jaked)
* Added LoggingMetricsConsumer to log all metrics to a file, by default not enabled (thanks mrflip)
* Add prepare(Map conf) method to TopologyValidator (thanks ankitoshniwal)
* Bug fix: Supervisor provides full path to workers to logging config rather than relative path (thanks revans2) 
* Bug fix: Call ReducerAggregator#init properly when used within persistentAggregate (thanks lorcan)
* Bug fix: Set component-specific configs correctly for Trident spouts

 


