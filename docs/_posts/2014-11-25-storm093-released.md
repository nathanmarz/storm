---
layout: post
title: Storm 0.9.3 released
author: P. Taylor Goetz
---

We are pleased to announce that Apache Storm 0.9.3 has been released and is available from [the downloads page](/downloads.html). This release includes 100 new fixes and improvements from 62 individual contributors.

Improved Kafka Integration
----------------------------

Apache Storm has supported [Apache Kafka](http://kafka.apache.org/) as a streaming data source since version 0.9.2-incubating. Version 0.9.3 brings a number of improvements to the Kafka integration including the ability to write data to one or more Kafka clusters and topics.

The ability to both read from and write to Kafka further unlocks potential of the already powerful combination of Storm and Kafka. Storm users can now use Kafka as a source of and destination for streaming data. This allows for inter-topology communication, topology chaining, combining spout/bolt-based topologies with Trident-based data flows, and integration with any external system that supports data ingest from Kafka.

More information about Storm's Kafka integration can be found in the [storm-kafka project documentation](https://github.com/apache/storm/blob/v0.9.3/external/storm-kafka/README.md).


HDFS Integration
----------------

Many stream processing use cases involve storing data in HDFS for further analysis and offline (batch) processing. Apache Storm’s HDFS integration consists of several bolt and Trident state implementations that allow topology developers to easily write data to HDFS from any Storm topology. 

More information about Storm's HDFS integration can be found in the [storm-hdfs project documentation](https://github.com/apache/storm/tree/v0.9.3/external/storm-hdfs).


HBase Integration
-----------------

Apache Storm’s HBase integration includes a number of components that allow Storm topologies to both write to and query HBase in real-time. Many organizations use Apache HBase as part of their big data strategy for batch, interactive, and real-time workflows. Storm’s HBase integration allows users to leverage data assets in HBase as streaming queries, as well as using HBase as a destination for streaming computation results.

More information about Storm's HBase integration can be found in the [storm-hbase project documentation](https://github.com/apache/storm/tree/v0.9.3/external/storm-hbase).

Reduced Dependency Conflicts
----------------------------
In previous Storm releases, it was not uncommon for users' topology dependencies to conflict with the libraries used by Storm. In Storm 0.9.3 several dependency packages that were common sources of conflicts have been package-relocated (shaded) to avoid this situation. Developers are free to use the Storm-packaged versions, or supply their own version. 

The following table lists the dependency package relocations:

| Dependency  | Original Package | Storm Package |
|:---|:---|:---|
| Apache Thrift | `org.apache.thrift` | `org.apache.thrift7` |
| Netty | `org.jboss.netty` | `org.apache.storm.netty` |
| Google Guava | `com.google.common` | `org.apache.storm.guava` |
|              | `com.google.thirdparty` | `org.apache.storm.guava.thirdparty` |
| Apache HTTPClient | `org.apache.http` | `org.apache.storm.http` |
| Apache ZooKeeper | `org.apache.zookeeper` | `org.apache.storm.zookeeper` |
| Apache Curator | `org.apache.curator` | `org.apache.storm.curator` |

Multi-Lang Improvements
-----------------------
Apache Storm 0.9.3 includes a new [Node.js](http://nodejs.org) multi-lang implementation that allows developers to write spouts and bolts in JavaScript.

In addition to the Node.js implementation, the multi-lang protocol has been substantially improved in terms of robustness and error handling capabilities. As a result, **the multi-lang API has changed in a non-backward-compatible way**. Users with existing multi-lang topologies should consult the Python, Ruby, and JavaScript multi-lang examples to determine the impact prior to upgrading.


Thanks
------
Special thanks are due to all those who have contributed to Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.


Full Changelog
---------

 * STORM-558: change "swap!" to "reset!" to fix assignment-versions in supervisor
 * STORM-555: Storm json response should set charset to UTF-8
 * STORM-513: check heartbeat from multilang subprocess
 * STORM-549: "topology.enable.message.timeouts" does nothing
 * STORM-546: Local hostname configuration ignored by executor
 * STORM-492: Test timeout should be configurable
 * STORM-540: Change default time format in logs to ISO8601 in order to include timezone
 * STORM-511: Storm-Kafka spout keeps sending fetch requests with invalid offset
 * STORM-538: Guava com.google.thirdparty.publicsuffix is not shaded
 * STORM-497: don't modify the netty server taskToQueueId mapping while the someone could be reading it.
 * STORM-537: A worker reconnects infinitely to another dead worker (Sergey Tryuber)
 * STORM-519: add tuple as an input param to HBaseValueMapper 
 * STORM-488: Exit with 254 error code if storm CLI is run with unknown command
 * STORM-506: Do not count bolt acks & fails in total stats
 * STORM-490: fix build under Windows
 * STORM-439: Replace purl.js qith jquery URL plugin
 * STORM-499: Document and clean up shaded dependncy resolution with maven
 * STORM-210: Add storm-hbase module
 * STORM-507: Topology visualization should not block ui
 * STORM-504: Class used by `repl` command is deprecated.
 * STORM-330: Implement storm exponential backoff stategy for netty client and curator
 * STORM-461: exit-process! does not always exit the process, but throws an exception
 * STORM-341: fix assignment sorting
 * STORM-476: external/storm-kafka: avoid NPE on null message payloads
 * STORM-424: fix error message related to kafka offsets
 * STORM-454: correct documentation in STORM-UI-REST-API.md
 * STORM-474: Reformat UI HTML code
 * STORM-447: shade/relocate packages of dependencies that are common causes of dependency conflicts
 * STORM-279: cluster.xml doesn't take in STORM_LOG_DIR values.
 * STORM-380: Kafka spout: throw RuntimeException if a leader cannot be found for a partition
 * STORM-435: Improve storm-kafka documentation
 * STORM-405: Add kafka trident state so messages can be sent to kafka topic
 * STORM-211: Add module for HDFS integration
 * STORM-337: Expose managed spout ids publicly
 * STORM-320: Support STORM_CONF_DIR environment variable to support
 * STORM-360: Add node details for Error Topology and Component pages
 * STORM-54: Per-Topology Classpath and Environment for Workers
 * STORM-355: excluding outdated netty transitively included via curator
 * STORM-183: Replacing RunTime.halt() with RunTime.exit()
 * STORM-213: Decouple In-Process ZooKeeper from LocalCluster.
 * STORM-365: Add support for Python 3 to storm command.
 * STORM-332: Enable Kryo serialization in storm-kafka
 * STORM-370: Add check for empty table before sorting dom in UI
 * STORM-359: add logviewer paging and download
 * STORM-372: Typo in storm_env.ini
 * STORM-266: Adding shell process pid and name in the log message
 * STORM-367: Storm UI REST api documentation.
 * STORM-200: Proposal for Multilang's Metrics feature
 * STORM-351: multilang python process fall into endless loop
 * STORM-375: Smarter downloading of assignments by supervisors and workers
 * STORM-328: More restrictive Config checks, strict range check within Utils.getInt()
 * STORM-381: Replace broken jquery.tablesorter.min.js to latest
 * STORM-312: add storm monitor tools to monitor throughtput interactively
 * STORM-354: Testing: allow users to pass TEST-TIMEOUT-MS as param for complete-topology
 * STORM-254: one Spout/Bolt can register metric twice with same name in different timeBucket
 * STORM-403: heartbeats-to-nimbus in supervisor-test failed due to uninten...
 * STORM-402: FileNotFoundException when using storm with apache tika
 * STORM-364: The exception time display as default timezone.
 * STORM-420: Missing quotes in storm-starter python code
 * STORM-399: Kafka Spout defaulting to latest offset when current offset is older then 100k
 * STORM-421: Memoize local hostname lookup in executor
 * STORM-414: support logging level to multilang protocol spout and bolt
 * STORM-321: Added a tool to see the current status of STORM JIRA and github pulls.
 * STORM-415: validate-launched-once in supervisor-test can not handle multiple topologies
 * STORM-155: Storm rebalancing code causes multiple topologies assigned to a single port
 * STORM-419: Updated test so sort ordering is very explicit.
 * STORM-406: Fix for reconnect logic in netty client.
 * STORM-366: Add color span to most recent error and fix ui templates.
 * STORM-369: topology summary page displays wrong order.
 * STORM-239: Allow supervisor to operate in paths with spaces in them
 * STORM-87: fail fast on ShellBolt exception
 * STORM-417: Storm UI lost table sort style when tablesorter was updated
 * STORM-396: Replace NullPointerException with IllegalArgumentExeption
 * STORM-451: Latest storm does not build due to a pom.xml error in storm-hdfs pom.xml
 * STORM-453: migrated to curator 2.5.0
 * STORM-458: sample spout uses incorrect name when connecting bolt
 * STORM-455: Report error-level messages from ShellBolt children
 * STORM-443: multilang log's loglevel protocol can cause hang
 * STORM-449: Updated ShellBolt to not exit when shutting down.
 * STORM-464: Simulated time advanced after test cluster exits causes intermittent test failures
 * STORM-463: added static version of metrics helpers for Config
 * STORM-376: Add compression to serialization
 * STORM-437: Enforce utf-8 when multilang reads from stdin
 * STORM-361: Add JSON-P support to Storm UI API
 * STORM-373: Provide Additional String substitutions for *.worker.childopts
 * STORM-274: Add support for command remoteconfvalue in storm.cmd
 * STORM-132: sort supervisor by free slot in desending order
 * STORM-472: Improve error message for non-completeable testing spouts
 * STORM-401: handle InterruptedIOException properly.
 * STORM-461: exit-process! does not always exit the process, but throws an exception instead
 * STORM-475: Storm UI pages do not use UTF-8
 * STORM-336: Logback version should be upgraded
 * STORM-386: nodejs multilang protocol implementation and examples
 * STORM-500: Add Spinner when UI is loading stats from nimbus
 * STORM-501: Missing StormSubmitter API
 * STORM-493: Workers inherit storm.conf.file/storm.options properties of their supervisor
 * STORM-498: make ZK connection timeout configurable in Kafka spout
 * STORM-428: extracted ITuple interface
 * STORM-508: Update DEVELOPER.md now that Storm has graduated from Incubator
 * STORM-514: Update storm-starter README now that Storm has graduated from Incubator
