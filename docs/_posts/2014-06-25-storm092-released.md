---
layout: post
title: Storm 0.9.2 released
author: P. Taylor Goetz
---

We are pleased to announce that Storm 0.9.2-incubating has been released and is available from [the downloads page](/downloads.html). This release includes many important fixes and improvements.

Netty Transport Improvements
----------------------------
Storm's Netty-based transport has been overhauled to significantly improve performance through better utilization of thread, CPU, and network resources, particularly in cases where message sizes are small. Storm contributor Sean Zhong ([@clockfly](https://github.com/clockfly)) deserves a great deal of credit not only for discovering, analyzing, documenting and fixing the root cause, but also for persevering through an extended review process and promptly addressing all concerns.

Those interested in the technical details and evolution of this patch can find out more in the [JIRA ticket for STORM-297](https://issues.apache.org/jira/browse/STORM-297).

Sean also discovered and fixed an [elusive bug](https://issues.apache.org/jira/browse/STORM-342) in Storm's usage of the Disruptor queue that could lead to out-of-order or lost messages. 

Many thanks to Sean for contributing these important fixes.

Storm UI Improvements
---------------------
This release also includes a number of improvements to the Storm UI service. Contributor Sriharsha Chintalapani([@harshach](https://github.com/harshach)) added a REST API to the Storm UI service to expose metrics and operations in JSON format, and updated the UI to use that API.

The new REST API will make it considerably easier for other services to consume availabe cluster and topology metrics for monitoring and visualization applications. Kyle Nusbaum ([@knusbaum](https://github.com/knusbaum)) has already leveraged the REST API to create a topology visualization tool now included in Storm UI and illustrated in the screenshot below.

&nbsp;

![Storm UI Topology Visualization](/images/ui_topology_viz.png)

&nbsp;

In the visualization, spout components are represented as blue, while bolts are colored between green and red depending on their associated capacity metric. The width of the lines between the components represent the flow of tuples relative to the other visible streams. 

Kafka Spout
-----------
This is the first Storm release to include official support for consuming data from Kafka 0.8.x. In the past, development of Kafka spouts for Storm had become somewhat fragmented and finding an implementation that worked with certain versions of Storm and Kafka proved burdensome for some developers. This is no longer the case, as the `storm-kafka` module is now part of the Storm project and associated artifacts are released to official channels (Maven Central) along with Storm's other components.

Thanks are due to GitHub user [@wurstmeister]() for picking up Nathan Marz' original Kafka 0.7.x implementation, updating it to work with Kafka 0.8.x, and contributing that work back to the Storm community.

The `storm-kafka` module can be found in the `/external/` directory of the source tree and binary distributions. The `external` area has been set up to contain projects that while not required by Storm, are often used in conjunction with Storm to integrate with some other technology. Such projects also come with a maintenance committment from at least one Storm committer to ensure compatibility with Storm's main codebase as it evolves.

The `storm-kafka` dependency is available now from Maven Central at the following coordinates:


	groupId: org.apache.storm
	artifactId: storm-kafka
	version: 0.9.2-incubating

Users, and Scala developers in particular, should note that the Kafka dependency is listed as `provided`. This allows users to choose a specific Scala version as described in the [project README](https://github.com/apache/incubator-storm/tree/v0.9.2-incubating/external/storm-kafka).


Storm Starter and Examples
--------------------------

Similar to the `external` section of the codebase, we have also added an `examples` directory and pulled in the `storm-starter` project to ensure it will be maintained in lock-step with Storm's main codebase.

Thank you to Storm committer Michael G. Noll for his continued work in maintaining and improving the `storm-starter` project.


Plugable Serialization for Multilang
------------------------------------
In previous versions of Storm, serialization of data to and from multilang components was limited to JSON, imposing somewhat of performance penalty. Thanks to a contribution from John Gilmore ([@jsgilmore](https://github.com/jsgilmore)) the serialization mechanism is now plugable and enables the use of more performant serialization frameworks like protocol buffers in addition to JSON.


Thanks
------
Special thanks are due to all those who have contributed to Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.


Changelog
---------

 * STORM-352: [storm-kafka] PartitionManager does not save offsets to ZooKeeper
 * STORM-66: send taskid on initial handshake
 * STORM-342: Contention in Disruptor Queue which may cause out of order or lost messages
 * STORM-338: Move towards idiomatic Clojure style 
 * STORM-335: add drpc test for removing timed out requests from queue
 * STORM-69: Storm UI Visualizations for Topologies
 * STORM-297: Performance scaling with CPU
 * STORM-244: DRPC timeout can return null instead of throwing an exception
 * STORM-63: remove timeout drpc request from its function's request queue
 * STORM-313: Remove log-level-page from logviewer
 * STORM-205: Add REST API To Storm UI
 * STORM-326: tasks send duplicate metrics
 * STORM-331: Update the Kafka dependency of storm-kafka to 0.8.1.1
 * STORM-308: Add support for config_value to {supervisor,nimbus,ui,drpc,logviewer} childopts
 * STORM-309: storm-starter Readme: windows documentation update
 * STORM-318: update storm-kafka to use apache curator-2.4.0
 * STORM-303: storm-kafka reliability improvements
 * STORM-233: Removed inline heartbeat to nimbus to avoid workers being killed when under heavy ZK load
 * STORM-267: fix package name of LoggingMetricsConsumer in storm.yaml.example
 * STORM-265: upgrade to clojure 1.5.1
 * STORM-232: ship JNI dependencies with the topology jar
 * STORM-295: Add storm configuration to define JAVA_HOME
 * STORM-138: Pluggable serialization for multilang
 * STORM-264: Removes references to the deprecated topology.optimize
 * STORM-245: implement Stream.localOrShuffle() for trident
 * STORM-317: Add SECURITY.md to release binaries
 * STORM-310: Change Twitter authentication
 * STORM-305: Create developer documentation
 * STORM-280: storm unit tests are failing on windows
 * STORM-298: Logback file does not include full path for metrics appender fileNamePattern
 * STORM-316: added validation to registermetrics to have timebucketSizeInSecs >= 1
 * STORM-315: Added progress bar when submitting topology
 * STORM-214: Windows: storm.cmd does not properly handle multiple -c arguments
 * STORM-306: Add security documentation
 * STORM-302: Fix Indentation for pom.xml in storm-dist
 * STORM-235: Registering a null metric should blow up early
 * STORM-113: making thrift usage thread safe for local cluster
 * STORM-223: use safe parsing for reading YAML
 * STORM-238: LICENSE and NOTICE files are duplicated in storm-core jar
 * STORM-276: Add support for logviewer in storm.cmd.
 * STORM-286: Use URLEncoder#encode with the encoding specified.
 * STORM-296: Storm kafka unit tests are failing on windows
 * STORM-291: upgrade http-client to 4.3.3
 * STORM-252: Upgrade curator to latest version
 * STORM-294: Commas not escaped in command line
 * STORM-287: Fix the positioning of documentation strings in clojure code
 * STORM-290: Fix a log binding conflict caused by curator dependencies
 * STORM-289: Fix Trident DRPC memory leak
 * STORM-173: Treat command line "-c" option number config values as such
 * STORM-194: Support list of strings in *.worker.childopts, handle spaces
 * STORM-288: Fixes version spelling in pom.xml
 * STORM-208: Add storm-kafka as an external module
 * STORM-285: Fix storm-core shade plugin config
 * STORM-12: reduce thread usage of netty transport
 * STORM-281: fix and issue with config parsing that could lead to leaking file descriptors
 * STORM-196: When JVM_OPTS are set, storm jar fails to detect storm.jar from environment
 * STORM-260: Fix a potential race condition with simulated time in Storm's unit tests
 * STORM-258: Update commons-io version to 2.4
 * STORM-270: don't package .clj files in release jars.
 * STORM-273: Error while running storm topologies on Windows using "storm jar"
 * STROM-247: Replace links to github resources in storm script
 * STORM-263: Update Kryo version to 2.21+
 * STORM-187: Fix Netty error "java.lang.IllegalArgumentException: timeout value is negative"
 * STORM-186: fix float secs to millis long convertion
 * STORM-70: Upgrade to ZK-3.4.5 and curator-1.3.3
 * STORM-146: Unit test regression when storm is compiled with 3.4.5 zookeeper
