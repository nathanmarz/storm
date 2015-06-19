---
layout: post
title: Storm 0.10.0-beta Released
author: P. Taylor Goetz
---

Fast on the heals of the 0.9.5 maintenance release, the Apache Storm community is pleased to announce that Apache Storm 0.10.0-beta has been released and is now available on [the downloads page](/downloads.html).

Aside from many stability and performance improvements, this release includes a number of important new features, some of which are highlighted below.

Secure, Multi-Tenant Deployment
--------------------------------

Much like the early days of Hadoop, Apache Storm originally evolved in an environment where security was not a high-priority concern. Rather, it was assumed that Storm would be deployed to environments suitably cordoned off from security threats. While a large number of users were comfortable setting up their own security measures for Storm (usually at the Firewall/OS level), this proved a hindrance to broader adoption among larger enterprises where security policies prohibited deployment without specific safeguards.

Yahoo! hosts one of the largest Storm deployments in the world, and their engineering team recognized the need for security early on, so it implemented many of the features necessary to secure its own Apache Storm deployment. Yahoo!, Hortonworks, Symantec, and the broader Apache Storm community have worked together to bring those security innovations into the main Apache Storm code base.

We are pleased to announce that work is now complete. Some of the highlights of Storm's new security features include:

 * Kerberos Authentication with Automatic Credential Push and Renewal
 * Pluggable Authorization and ACLs
 * Multi-Tenant Scheduling with Per-User isolation and configurable resource limits.
 * User Impersonation
 * SSL Support for Storm UI, Log Viewer, and DRPC (Distributed Remote Procedure Call)
 * Secure integration with other Hadoop Projects (such as ZooKeeper, HDFS, HBase, etc.)
 * User isolation (Storm topologies run as the user who submitted them)

For more details and instructions for securing Storm, please see [the security documentation](https://github.com/apache/storm/blob/v0.10.0-beta/SECURITY.md).

A Foundation for Rolling Upgrades and Continuity of Operations
--------------------------------------------------------------

In the past, upgrading a Storm cluster could be an arduous process that involved un-deploying existing topologies, removing state from local disk and ZooKeeper, installing the upgrade, and finally redeploying topologies. From an operations perspective, this process was disruptive to say the very least.

The underlying cause of this headache was rooted in the data format Storm processes used to store both local and distributed state. Between versions, these data structures would change in incompatible ways.

Beginning with version 0.10.0, this limitation has been eliminated. In the future, upgrading from Storm 0.10.0 to a newer version can be accomplished seamlessly, with zero down time. In fact, for users who use [Apache Ambari](https://ambari.apache.org) for cluster provisioning and management, the process can be completely automated.


Easier Deployment and Declarative Topology Wiring with Flux
----------------------------------------------------------
Apache Storm 0.10.0 now includes Flux, which is a framework and set of utilities that make defining and deploying Storm topologies less painful and developer-intensive. A common pain point mentioned by Storm users is the fact that the wiring for a Topology graph is often tied up in Java code, and that any changes require recompilation and repackaging of the topology jar file. Flux aims to alleviate that pain by allowing you to package all your Storm components in a single jar, and use an external text file to define the layout and configuration of your topologies.

Some of Flux' features include:

 * Easily configure and deploy Storm topologies (Both Storm core and Micro-batch API) without embedding configuration in your topology code
 * Support for existing topology code
 * Define Storm Core API (Spouts/Bolts) using a flexible YAML DSL
 * YAML DSL support for most Storm components (storm-kafka, storm-hdfs, storm-hbase, etc.)
 * Convenient support for multi-lang components
 * External property substitution/filtering for easily switching between configurations/environments (similar to Maven-style `${variable.name}` substitution)

You can read more about Flux on the [Flux documentation page](https://github.com/apache/storm/blob/v0.10.0-beta/external/flux/README.md).

Partial Key Groupings
---------------------
In addition to the standard Stream Groupings Storm has always supported, version 0.10.0 introduces a new grouping named "Partial Key Grouping". With the Partial Stream Grouping, the tuple  stream is partitioned by the fields specified in the grouping, like the Fields Grouping, but are load balanced between two downstream bolts, which provides better utilization of resources when the incoming data is skewed. 

Documentation for the Partial Key Grouping and other stream groupings supported by Storm can be found [here](https://storm.apache.org/documentation/Concepts.html). [This research paper](https://melmeric.files.wordpress.com/2014/11/the-power-of-both-choices-practical-load-balancing-for-distributed-stream-processing-engines.pdf) provides additional details regarding how it works and its advantages.


Improved Logging Framework
--------------------------
Debugging distributed applications can be difficult, and usually focuses on one main source of information: application log files. But in a very low latency system like Storm where every millisecond counts, logging can be a double-edged sword: If you log too little information you may miss the information you need to solve a problem; log too much and you risk degrading the overall performance of your application as resources are consumed by the logging framework.

In version 0.10.0 Storm's logging framework now uses [Apache Log4j 2](http://logging.apache.org/log4j/2.x/) which, like Storm's internal messaging subsystem, uses the extremely performant [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) messaging library. Log4j 2 boast an 18x higher throughput and orders of magnitude lower latency than Storm's previous logging framework. More efficient resource utilization at the logging level means more resources are available where they matter most: executing your business logic.

A few of the important features these changes bring include:

 * Rolling log files with size, duration, and date-based triggers that are composable
 * Dynamic log configuration updates without dropping log messages
 * Remote log monitoring and (re)configuration via JMX
 * A Syslog/[RFC-5424](https://tools.ietf.org/html/rfc5424)-compliant appender.
 * Integration with log aggregators such as syslog-ng


Streaming ingest with Apache Hive
---------------------------------
Introduced in version 0.13, [Apache Hive](https://hive.apache.org) includes a [Streaming Data Ingest API](https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest) that allows data to be written continuously into Hive. The incoming data can be continuously committed in small batches of records into existing Hive partition or table. Once the data is committed its immediately visible to all hive queries.

Apache Storm 0.10.0 introduces both a Storm Core API bolt implementation 
that allows users to stream data from Storm directly into hive. Storm's Hive integration also includes a [State](https://storm.apache.org/documentation/Trident-state) implementation for Storm's Micro-batching/Transactional API (Trident) that allows you to write to Hive from a micro-batch/transactional topology and supports exactly-once semantics for data persistence.

For more information on Storm's Hive integration, see the [storm-hive documentation](https://github.com/apache/storm/blob/v0.10.0-beta/external/storm-hive/README.md).


Microsoft Azure Event Hubs Integration
--------------------------------------
With Microsoft Azure's [support for running Storm on HDInsight](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-storm-overview/), Storm is now a first class citizen of the Azure cloud computing platform. To better support Storm integration with Azure services, Microsoft engineers have contributed several components that allow Storm to integrate directly with [Microsoft Azure Event Hubs](http://azure.microsoft.com/en-us/services/event-hubs/).

Storm's Event Hubs integration includes both spout and bolt implementations for reading from, and writing to Event Hubs. The Event Hub integration also includes a Micro-batching/Transactional (Trident) spout implementation that supports fully fault-tolerant and reliable processing, as well as support for exactly-once message processing semantics.


Redis Support
-------------
Apache Storm 0.10.0 also introduces support for the [Redis](http://redis.io) data structure server. Storm's Redis support includes bolt implementations for both writing to and querying Redis from a Storm topology, and is easily extended for custom use cases. For Storm's micro-batching/transactional API, the Redis support includes both [Trident State and MapState](https://storm.apache.org/documentation/Trident-state.html) implementations for fault-tolerant state management with Redis.

Further information can be found in the [storm-redis documentation](https://github.com/apache/storm/blob/v0.10.0-beta/external/storm-redis/README.md).


JDBC/RDBMS Integration
----------------------
Many stream processing data flows require accessing data from or writing data to a relational data store. Storm 0.10.0 introduces highly flexible and customizable support for integrating with virtually any JDBC-compliant database.

The Storm-JDBC package includes core Storm bolt and Trident state implementations that allow a storm topology to either insert Storm tuple data into a database table or execute select queries against a database to enrich streaming data in a storm topology.

Further details and instructions can be found in the [Storm-JDBC documentation](https://github.com/apache/storm/blob/v0.10.0-beta/external/storm-jdbc/README.md).


Reduced Dependency Conflicts
----------------------------
In previous Storm releases, it was not uncommon for users' topology dependencies to conflict with the libraries used by Storm. In Storm 0.9.3 several dependency packages that were common sources of conflicts have been package-relocated (shaded) to avoid this situation. In 0.10.0 this list has been expanded.

Developers are free to use the Storm-packaged versions, or supply their own version. 

The full list of Storm's package relocations can be found [here](https://github.com/apache/storm/blob/v0.10.0-beta/storm-core/pom.xml#L439).


Future Work
-----------
While the 0.10.0 release is an important milestone in the evolution of Apache Storm, the Storm community is actively working on new improvements, both near and long term, and continuously exploring the realm of the possible.

Twitter recently announced the Heron project, which claims to provide substantial performance improvements while maintaining 100% API compatibility with Storm. The corresponding research paper provides additional details regarding the architectural improvements. The fact that Twitter chose to maintain API compatibility with Storm is a testament to the power and flexibility of that API. Twitter has also expressed a desire to share their experiences and work with the Apache Storm community.

A number of concepts expressed in the Heron paper were already in the implementation stage by the Storm community even before it was published, and we look forward to working with Twitter to bring those and other improvements to Storm.

Thanks
------
Special thanks are due to all those who have contributed to Apache Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are very much valued and appreciated.


Full Change Log
---------------

 * STORM-856: use serialized value of delay secs for topo actions
 * STORM-852: Replaced Apache Log4j Logger with SLF4J API
 * STORM-813: Change storm-starter's README so that it explains mvn exec:java cannot run multilang topology
 * STORM-853: Fix upload API to handle multi-args properly
 * STORM-850: Convert storm-core's logback-test.xml to log4j2-test.xml
 * STORM-848: Shade external dependencies
 * STORM-849: Add storm-redis to storm binary distribution
 * STORM-760: Use JSON for serialized conf
 * STORM-833: Logging framework logback -> log4j 2.x
 * STORM-842: Drop Support for Java 1.6
 * STORM-835: Netty Client hold batch object until io operation complete
 * STORM-827: Allow AutoTGT to work with storm-hdfs too.
 * STORM-821: Adding connection provider interface to decouple jdbc connector from a single connection pooling implementation.
 * STORM-818: storm-eventhubs configuration improvement and refactoring
 * STORM-816: maven-gpg-plugin does not work with gpg 2.1
 * STORM-811: remove old metastor_db before running tests again.
 * STORM-808: allow null to be parsed as null
 * STORM-807: quote args to storm.py correctly
 * STORM-801: Add Travis CI badge to README
 * STORM-797: DisruptorQueueTest has some race conditions in it.
 * STORM-796: Add support for "error" command in ShellSpout
 * STORM-795: Update the user document for the extlib issue
 * STORM-792: Missing documentation in backtype.storm.generated.Nimbus
 * STORM-791: Storm UI displays maps in the config incorrectly
 * STORM-790: Log "task is null" instead of let worker died when task is null in transfer-fn
 * STORM-789: Send more topology context to Multi-Lang components via initial handshake
 * STORM-788: UI Fix key for process latencies
 * STORM-787: test-ns should announce test failures with 'BUILD FAILURE'
 * STORM-786: KafkaBolt should ack tick tuples
 * STORM-773: backtype.storm.transactional-test fails periodically with timeout
 * STORM-772: Tasts fail periodically with InterruptedException or InterruptedIOException
 * STORM-766: Include version info in the service page
 * STORM-765: Thrift serialization for local state
 * STORM-764: Have option to compress thrift heartbeat
 * STORM-762: uptime for worker heartbeats is lost when converted to thrift
 * STORM-761: An option for new/updated Redis keys to expire in RedisMapState
 * STORM-757: Simulated time can leak out on errors
 * STORM-753: Improve RedisStateQuerier to convert List<Values> from Redis value
 * STORM-752: [storm-redis] Clarify Redis*StateUpdater's expire is optional
 * STORM-750: Set Config serialVersionUID
 * STORM-749: Remove CSRF check from the REST API.
 * STORM-747: assignment-version-callback/info-with-version-callback are not fired when assignments change
 * STORM-746: Skip ack init when there are no output tasks
 * STORM-745: fix storm.cmd to evaluate 'shift' correctly with 'storm jar'
 * STORM-741: Allow users to pass a config value to perform impersonation.
 * STORM-740: Simple Transport Client cannot configure thrift buffer size
 * STORM-737: Check task->node+port with read lock to prevent sending to closed connection
 * STORM-735: [storm-redis] Upgrade Jedis to 2.7.0
 * STORM-730: remove extra curly brace
 * STORM-729: Include Executors (Window Hint) if the component is of Bolt type
 * STORM-728: Put emitted and transferred stats under correct columns
 * STORM-727: Storm tests should succeed even if a storm process is running locally.
 * STORM-724: Document RedisStoreBolt and RedisLookupBolt which is missed.
 * STORM-723: Remove RedisStateSetUpdater / RedisStateSetCountQuerier which didn't tested and have a bug
 * STORM-721: Storm UI server should support SSL.
 * STORM-715: Add a link to AssignableMetric.java in Metrics.md
 * STORM-714: Make CSS more consistent with self, prev release
 * STORM-713: Include topic information with Kafka metrics.
 * STORM-712: Storm daemons shutdown if OutOfMemoryError occurs in any thread
 * STORM-711: All connectors should use collector.reportError and tuple anchoring.
 * STORM-708: CORS support for STORM UI.
 * STORM-707: Client (Netty): improve logging to help troubleshooting connection woes
 * STORM-704: Apply Travis CI to Apache Storm Project
 * STORM-703: With hash key option for RedisMapState, only get values for keys in batch
 * STORM-699: storm-jdbc should support custom insert queries. 
 * STORM-696: Single Namespace Test Launching
 * STORM-694: java.lang.ClassNotFoundException: backtype.storm.daemon.common.SupervisorInfo
 * STORM-693: KafkaBolt exception handling improvement.
 * STORM-691: Add basic lookup / persist bolts
 * STORM-690: Return Jedis into JedisPool with marking 'broken' if connection is broken
 * STORM-689: SimpleACLAuthorizer should provide a way to restrict who can submit topologies.
 * STORM-688: update Util to compile under JDK8
 * STORM-687: Storm UI does not display up to date information despite refreshes in IE
 * STORM-685: wrong output in log when committed offset is too far behind latest offset
 * STORM-684: In RichSpoutBatchExecutor: underlying spout is not closed when emitter is closed
 * STORM-683: Make false in a conf really evaluate to false in clojure.
 * STORM-682: supervisor should handle worker state corruption gracefully.
 * STORM-681: Auto insert license header with genthrift.sh
 * STORM-675: Allow users to have storm-env.sh under config dir to set custom JAVA_HOME and other env variables.
 * STORM-673: Typo 'deamon' in security documentation
 * STORM-672: Typo in Trident documentation example
 * STORM-670: restore java 1.6 compatibility (storm-kafka)
 * STORM-669: Replace links with ones to latest api document
 * STORM-667: Incorrect capitalization "SHell" in Multilang-protocol.md
 * STORM-663: Create javadocs for BoltDeclarer
 * STORM-659: return grep matches each on its own line.
 * STORM-657: make the shutdown-worker sleep time before kill -9 configurable
 * STORM-656: Document "external" modules and "Committer Sponsors"
 * STORM-651: improvements to storm.cmd
 * STORM-641: Add total number of topologies to api/v1/cluster/summary.
 * STORM-640: Storm UI vulnerable to poodle attack.
 * STORM-637: Integrate PartialKeyGrouping into storm API
 * STORM-636: Faster, optional retrieval of last component error
 * STORM-635: logviewer returns 404 if storm_home/logs is a symlinked dir.
 * STORM-634: Storm serialization changed to thrift to support rolling upgrade.
 * STORM-632: New grouping for better load balancing
 * STORM-630: Support for Clojure 1.6.0
 * STORM-629: Place Link to Source Code Repository on Webpage
 * STORM-627: Storm-hbase configuration error.
 * STORM-626: Add script to print out the merge command for a given pull request.
 * STORM-625: Don't leak netty clients when worker moves or reuse netty client.	
 * STORM-623: Generate latest javadocs
 * STORM-620: Duplicate maven plugin declaration
 * STORM-616: Storm JDBC Connector.
 * STORM-615: Add REST API to upload topology.
 * STORM-613: Fix wrong getOffset return value
 * STORM-612: Update the contact address in configure.ac
 * STORM-611: Remove extra "break"s
 * STORM-610: Check the return value of fts_close()
 * STORM-609: Add storm-redis to storm external
 * STORM-608: Storm UI CSRF escape characters not work correctly.
 * STORM-607: storm-hbase HBaseMapState should support user to customize the hbase-key & hbase-qualifier
 * STORM-603: Log errors when required kafka params are missing
 * STORM-601: Make jira-github-join ignore case.
 * STORM-600: upgrade jacoco plugin to support jdk8
 * STORM-599: Use nimbus's cached heartbeats rather than fetching again from ZK
 * STORM-596: remove config topology.receiver.buffer.size
 * STORM-595: storm-hdfs can only work with sequence files that use Writables.
 * STORM-586: Trident kafka spout fails instead of updating offset when kafka offset is out of range.
 * STORM-585: Performance issue in none grouping
 * STORM-583: Add Microsoft Azure Event Hub spout implementations
 * STORM-578: Calls to submit-mocked-assignment in supervisor-test use invalid executor-id format
 * STORM-577: long time launch worker will block supervisor heartbeat
 * STORM-575: Ability to specify Jetty host to bind to
 * STORM-572: Storm UI 'favicon.ico'
 * STORM-572: Allow Users to pass TEST-TIMEOUT-MS for java
 * STORM-571: upgrade clj-time.
 * STORM-570: Switch from tablesorter to datatables jquery plugin.
 * STORM-569: Add Conf for bolt's outgoing overflow-buffer.
 * STORM-567: Move Storm Documentation/Website from SVN to git
 * STORM-565: Fix NPE when topology.groups is null.
 * STORM-563: Kafka Spout doesn't pick up from the beginning of the queue unless forceFromStart specified.
 * STORM-561: Add flux as an external module
 * STORM-557: High Quality Images for presentations
 * STORM-554: the type of first param "topology" should be ^StormTopology not ^TopologyContext
 * STORM-552: Add netty socket backlog config
 * STORM-548: Receive Thread Shutdown hook should connect to local hostname but not Localhost
 * STORM-541: Build produces maven warnings
 * STORM-539: Storm Hive Connector.
 * STORM-533: Add in client and server IConnection metrics.
 * STORM-527: update worker.clj -- delete "missing-tasks" checking
 * STORM-525: Add time sorting function to the 2nd col of bolt exec table
 * STORM-512: KafkaBolt doesn't handle ticks properly
 * STORM-505: Fix debug string construction
 * STORM-495: KafkaSpout retries with exponential backoff
 * STORM-487: Remove storm.cmd, no need to duplicate work python runs on windows too.
 * STORM-483: provide dedicated directories for classpath extension
 * STORM-456: Storm UI: cannot navigate to topology page when name contains spaces.
 * STORM-446: Allow superusers to impersonate other users in secure mode.
 * STORM-444: Add AutoHDFS like credential fetching for HBase
 * STORM-442: multilang ShellBolt/ShellSpout die() can be hang when Exception happened
 * STORM-441: Remove bootstrap macro from Clojure codebase
 * STORM-410: Add groups support to log-viewer
 * STORM-400: Thrift upgrade to thrift-0.9.2
 * STORM-329: fix cascading Storm failure by improving reconnection strategy and buffering messages (thanks tedxia)
 * STORM-322: Windows script do not handle spaces in JAVA_HOME path
 * STORM-248: cluster.xml location is hardcoded for workers
 * STORM-243: Record version and revision information in builds
 * STORM-188: Allow user to specifiy full configuration path when running storm command
 * STORM-130: Supervisor getting killed due to java.io.FileNotFoundException: File '../stormconf.ser' does not exist.
