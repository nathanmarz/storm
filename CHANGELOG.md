## 0.7.0-SNAPSHOT

 * Transactional topologies: a new higher level abstraction that enables exactly-once messaging semantics for most most computations. Documented on the wiki.
 * Component-specific configurations: Can now set configurations on a per-spout or per-bolt basis. 
 * Optimized tuple trees for batch processing in DRPC and transactional topologies. Only the coordination tuples are anchored. OutputCollector#fail still works because CoordinatedBolt will propagate the fail to all other tuples in the batch. 
 * Introduction of base classes for various bolt and spout types. These base classes are in the backtype.storm.topology.base package and provide empty implementations for commonly unused methods
 * 
 * Can customize the JVM options for Storm UI with new ui.childopts config
 * Bug fix: "storm supervisor" now uses supervisor.childopts instead of nimbus.childopts
 * Bug fix: supervisor.childopts and nimbus.childopts can now contain whitespace. Previously only the first token was taken from the string

## 0.6.2

 * Automatically delete old files in Nimbus's inbox. Configurable with "nimbus.cleanup.inbox.freq.secs" and "nimbus.inbox.jar.expiration.secs"
 * Redirect System.out and System.err to log4j
 * Added "topology.worker.child.opts" config, for topology-configurable worker options.
 * Use Netflix's Curator library for Zookeeper communication. Workers now reconnect to Zookeeper rather than crash when there's a disconnection.
 * Bug fix: DRPC server no longer hangs with too many concurrent requests. DPRC server now requires two ports: "drpc.port" and "drpc.invocations.port"
 * Bug fix: Multilang resources are now extracted from the relevant jar on the classpath when appropriate. Previously an error would be thrown if the resources/ dir was in a jar in local mode.
 * Bug fix: Fix race condition in unit testing where time simulation fails to detect that Storm cluster is waiting due to threads that are not alive
 * Bug fix: Fix deadlock in Nimbus that could be triggered by a kill command.

## 0.6.1

 * storm client "activate" and "deactivate" commands
 * storm client "rebalance" command
 * Nimbus will automatically detect and cleanup corrupt topologies (this would previously give an error of the form "file storm...ser cannot be found").
 * "storm" client will not run unless it's being used from a release. 
 * Topology jar path now passed in using a java property rather than an environment variable.
 * LD\_LIBRARY\_PATH environment variable is now set on worker processes appropriately.
 * Replaced jvyaml with snakeyaml. UTF-8 YAML files should now work properly. 
 * Upgraded httpclient, httpcore, and commons-codec dependencies.

## 0.6.0

 * New serialization system based on Kryo
 * Component and stream ids are now strings
 * Pluggable stream groupings
 * Storm now chooses an unused port for Zookeeper in local mode instead of crashing when 2181 was in use.
 * Better support for defining topologies in non-JVM languages. The Thrift structure for topologies now allows you to specify components using a Java class name and a list of arguments to that class's constructor.
 * Bug fix: errors during the preparation phase of spouts or bolts will be reported to the Storm UI 
 * Bug fix: Fixed bugs related to LinearDRPC topologies where the last bolt implements FinishedCallback 
 * Bug fix: String greater than 64K will now serialize properly 
 * Generalized type of anchors in OutputCollector methods to Collection from List. 
 * Improved logging throughout.
 * In the "worker.childopts" config, %ID% will be replaced by the worker port. 
 * Significant internal refactorings to clean up the codebase. 

## 0.5.4

 * LinearDRPCTopologyBuilder, a polished DRPC implementation, 
 * Improved custom serialization support. no longer need to provide "token" ids. 
 * Fallback on Java serialization by default. Can be turned off by setting "topology.fall.back.on.java.serialization" to false. 
 * Improved "storm kill" command. Can override the wait time with "-w" flag.
 * Display topology status in Storm UI
 * Changed Thrift namespace to avoid conflicts
 * Better error messages throughout
 * Storm UI port is configurable through "ui.port" 
 * Minor improvements to Clojure DSL 

## 0.5.3

 * Nimbus and supervisor daemons can now share a local dir. 
 * Greatly improved Clojure DSL for creating topologies.
 * Increased the default timeouts for startup of workers and tasks.
 * Added the commands "localconfvalue", "remoteconfvalue", and "repl" to the storm script.
 * Better error message when "storm jar" can't find the nimbus host in the configuration. 

## 0.5.2

 * No longer need any native dependencies to run Storm in local mode. Storm now uses a pure Java messaging system in local mode
 * Fixed logging configurations so that logging is no longer suppressed when including the Storm release jars on the classpath in local mode. 

## 0.5.1

 * Changed ISerialization's "accept" interface to not annotate the Class with the generic type
 * Made Config class implement Map and added helper methods for setting common configs
 
## 0.5.0
 
 * Initial release!