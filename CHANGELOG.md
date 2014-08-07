## 0.9.3-incubating
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
 * STORM-350: LMAX Disruptor 3.2.1
 * STORM-239: Allow supervisor to operate in paths with spaces in them

## 0.9.2-incubating
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

## 0.9.1-incubating
* Fix to prevent Nimbus from hanging if random data is sent to nimbus thrift port
* Improved support for running on Windows platforms
* Removed dependency on the `unzip` binary
* Switch build system from Leiningen to Maven
* STORM-1: Replaced 0MQ as the default transport with Netty.
* STORM-181: Nimbus now validates topology configuration when topologies are submitted (thanks d2r)
* STORM-182: Storm UI now includes tooltips to document fields (thanks d2r)
* STORM-195: `dependency-reduced-pom.xml` should be in `.gitignore`
* STORM-13: Change license on README.md
* STORM-2: Move all dependencies off of storm-specific builds
* STORM-159: Upload separate source and javadoc jars for maven use
* STORM-149: `storm jar` doesn't work on Windows

## 0.9.0.1
* Update build configuration to force compatibility with Java 1.6

## 0.9.0
* Fixed a netty client issue where sleep times for reconnection could be negative (thanks brndnmtthws)
* Fixed an issue that would cause storm-netty unit tests to fail

## 0.9.0-rc3
* Added configuration to limit ShellBolt internal _pendingWrites queue length (thanks xiaokang)
* Fixed a a netty client issue where sleep times for reconnection could be negative (thanks brndnmtthws)
* Fixed a display issue with system stats in Storm UI (thanks d2r)
* Nimbus now does worker heartbeat timeout checks as soon as heartbeats are updated (thanks d2r)
* The logviewer now determines log file location by examining the logback configuration (thanks strongh)
* Allow tick tuples to work with the system bolt (thanks xumingming)
* Add default configuration values for the netty transport and the ability to configure the number of worker threads (thanks revans2)
* Added timeout to unit tests to prevent a situation where tests would hang indefinitely (thanks d2r)
* Fixed and issue in the system bolt where local mode would not be detected accurately (thanks miofthena)

## 0.9.0-rc2 

* Fixed `storm jar` command to work properly when STORM_JAR_JVM_OPTS is not specified (thanks roadkill001)

## 0.9.0-rc1

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

## 0.8.3 (unreleased)

 * Revert zmq layer to not rely on multipart messages to fix issue reported by some users
 * Bug fix: Fix TransactionalMap and OpaqueMap to correctly do multiple updates to the same key in the same batch
 * Bug fix: Fix race condition between supervisor and Nimbus that could lead to stormconf.ser errors and infinite crashing of supervisor
 * Bug fix: Fix default scheduler to always reassign workers in a constrained topology when there are dead executors
 * Bug fix: Fix memory leak in Trident LRUMemoryMapState due to concurrency issue with LRUMap (thanks jasonjckn)
 * Bug fix: Properly ignore NoNodeExists exceptions when deleting old transaction states

## 0.8.2

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
  
## 0.8.1

 * Exposed Storm's unit testing facilities via the backtype.storm.Testing class. Notable functions are Testing/withLocalCluster and Testing/completeTopology (thanks xumingming)
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

## 0.8.0

 * Added Trident, the new high-level abstraction for intermixing high throughput, stateful stream processing with low-latency distributed querying
 * Added executor abstraction between workers and tasks. Workers = processes, executors = threads that run many tasks from the same spout or bolt.
 * Pluggable scheduler (thanks xumingming)
 * Eliminate explicit storage of task->component in Zookeeper
 * Number of workers can be dynamically changed at runtime through rebalance command and -n switch
 * Number of executors for a component can be dynamically changed at runtime through rebalance command and -e switch (multiple -e switches allowed)
 * Use worker heartbeats instead of task heartbeats (thanks xumingming)
 * UI performance for topologies with many executors/tasks much faster due to optimized usage of Zookeeper (10x improvement)
 * Added button to show/hide system stats (e.g., acker component and stream stats) from the Storm UI (thanks xumingming)
 * Stats are tracked on a per-executor basis instead of per-task basis
 * Major optimization for unreliable spouts and unanchored tuples (will use far less CPU)
 * Revamped internals of Storm to use LMAX disruptor for internal queuing. Dramatic reductions in contention and CPU usage.
 * Numerous micro-optimizations all throughout the codebase to reduce CPU usage.
 * Optimized internals of Storm to use much fewer threads - two fewer threads per spout and one fewer thread per acker.
 * Removed error method from task hooks (to be re-added at a later time)
 * Validate that subscriptions come from valid components and streams, and if it's a field grouping that the schema is correct (thanks xumingming)
 * MemoryTransactionalSpout now works in cluster mode
 * Only track errors on a component by component basis to reduce the amount stored in zookeeper (to speed up UI). A side effect of this change is the removal of the task page in the UI.
 * Add TOPOLOGY-TICK-TUPLE-FREQ-SECS config to have Storm automatically send "tick" tuples to a bolt's execute method coming from the __system component and __tick stream at the configured frequency. Meant to be used as a component-specific configuration.
 * Upgrade Kryo to v2.17
 * Tuple is now an interface and is much cleaner. The Clojure DSL helpers have been moved to TupleImpl
 * Added shared worker resources. Storm provides a shared ExecutorService thread pool by default. The number of threads in the pool can be configured with topology.worker.shared.thread.pool.size
 * Improve CustomStreamGrouping interface to make it more flexible by providing more information
 * Enhanced INimbus interface to allow for forced schedulers and better integration with global scheduler
 * Added assigned method to ISupervisor so it knows exactly what's running and not running
 * Custom serializers can now have one of four constructors: (), (Kryo), (Class), or (Kryo, Class)
 * Disallow ":", ".", and "\" from topology names
 * Errors in multilang subprocesses that go to stderr will be captured and logged to the worker logs (thanks vinodc)
 * Workers detect and warn for missing outbound connections from assignment, drop messages for which there's no outbound connection
 * Zookeeper connection timeout is now configurable (via storm.zookeeper.connection.timeout config)
 * Storm is now less aggressive about halting process when there are Zookeeper errors, preferring to wait until client calls return exceptions.
 * Can configure Zookeeper authentication for Storm's Zookeeper clients via "storm.zookeeper.auth.scheme" and "storm.zookeeper.auth.payload" configs
 * Supervisors only download code for topologies assigned to them
 * Include task id information in task hooks (thanks velvia)
 * Use execvp to spawn daemons (replaces the python launcher process) (thanks ept)
 * Expanded INimbus/ISupervisor interfaces to provide more information (used in Storm/Mesos integration)
 * Bug fix: Realize task ids when worker heartbeats to supervisor. Some users were hitting deserialization problems here in very rare cases (thanks herberteuler)
 * Bug fix: Fix bug where a topology's status would get corrupted to true if nimbus is restarted while status is rebalancing

## 0.7.4

 * Bug fix: Disallow slashes in topology names since it causes Nimbus to break by affecting local filesystem and zookeeper paths
 * Bug fix: Prevent slow loading tasks from causing worker timeouts by launching the heartbeat thread before tasks are loaded

## 0.7.3

 * Changed debug level of "Failed message" logging to DEBUG
 * Bug fix: Fixed critical regression in 0.7.2 that could cause workers to timeout to the supervisors or to Nimbus. 0.7.2 moved all system tasks to the same thread, so if one took a long time it would block the other critical tasks. Now different system tasks run on different threads.

## 0.7.2

NOTE: The change from 0.7.0 in which OutputCollector no longer assumes immutable inputs has been reverted to support optimized sending of tuples to colocated tasks

 * Messages sent to colocated tasks are sent in-memory, skipping serialization (useful in conjunction with localOrShuffle grouping) (thanks xumingming)
 * Upgrade to Clojure 1.4 (thanks sorenmacbeth)
 * Exposed INimbus and ISupervisor interfaces for running Storm on different resource frameworks (like Mesos).
 * Can override the hostname that supervisors report using "storm.local.hostname" config.
 * Make request timeout within DRPC server configurable via "drpc.request.timeout.secs"
 * Added "storm list" command to show running topologies at the command line (thanks xumingming)
 * Storm UI displays the release version (thanks xumingming)
 * Added reportError to BasicOutputCollector
 * Added reportError to BatchOutputCollector
 * Added close method to OpaqueTransactionalSpout coordinator
 * Added "storm dev-zookeeper" command for launching a local zookeeper server. Useful for testing a one node Storm cluster locally. Zookeeper dir configured with "dev.zookeeper.path"
 * Use new style classes for Python multilang adapter (thanks hellp)
 * Added "storm version" command
 * Heavily refactored and simplified the supervisor and worker code
 * Improved error message when duplicate config files found on classpath
 * Print the host and port of Nimbus when using the storm command line client
 * Include as much of currently read output as possible when pipe to subprocess is broken in multilang components
 * Lower supervisor worker start timeout to 120 seconds
 * More debug logging in supervisor
 * "nohup" no longer used by supervisor to launch workers (unnecessary)
 * Throw helpful error message if StormSubmitter used without using storm client script
 * Add Values class as a default serialization
 * Bug fix: give absolute piddir to subprocesses (so that relative paths can be used for storm local dir)
 * Bug fix: Fixed critical bug in transactional topologies where a batch would be considered successful even if the batch didn't finish
 * Bug fix: Fixed critical bug in opaque transactional topologies that would lead to duplicate messages when using pipelining
 * Bug fix: Workers will now die properly if a ShellBolt subprocess dies (thanks tomo)
 * Bug fix: Hide the BasicOutputCollector#getOutputter method, since it shouldn't be a publicly available method
 * Bug fix: Zookeeper in local mode now always gets an unused port. This will eliminate conflicts with other local mode processes or other Zookeeper instances on a local machine. (thanks xumingming)
 * Bug fix: Fixed NPE in CoordinatedBolt it tuples emitted, acked, or failed for a request id that has already timed out. (thanks xumingming)
 * Bug fix: UI no longer errors for topologies with no assigned tasks (thanks xumingming)
 * Bug fix: emitDirect on SpoutOutputCollector now works
 * Bug fix: Fixed NPE when giving null parallelism hint for spout in TransactionalTopologyBuilder (thanks xumingming)

## 0.7.1

 * Implemented shell spout (thanks tomo)
 * Shell bolts can now asynchronously emit/ack messages (thanks tomo)
 * Added hooks for when a tuple is emitted, acked, or failed in bolts or spouts.
 * Added activate and deactivate lifecycle methods on spouts. Spouts start off deactivated.
 * Added isReady method to ITransactionalSpout$Coordinator to give the ability to delay the creation of new batches
 * Generalized CustomStreamGrouping to return the target tasks rather than the indices. Also parameterized custom groupings with TopologyContext. (not backwards compatible)
 * Added localOrShuffle grouping that will send to tasks in the same worker process if possible, or do a shuffle grouping otherwise.
 * Removed parameter from TopologyContext#maxTopologyMessageTimeout (simplification).
 * Storm now automatically sets TOPOLOGY_NAME in the config passed to the bolts and spouts to the name of the topology.
 * Added TOPOLOGY_AUTO_TASK_HOOKS config to automatically add hooks into every spout/bolt for the topology.
 * Added ability to override configs at the command line. These config definitions have the highest priority.
 * Error thrown if invalid (not json-serializable) topology conf used.
 * bin/storm script can now be symlinked (thanks gabrielgrant)
 * Socket timeout for DRPCClient is now configurable
 * Added getThisWorkerPort() method to TopologyContext
 * Added better error checking in Fields (thanks git2samus)
 * Improved Clojure DSL to allow destructuring in bolt/spout methods
 * Added Nimbus stats methods to LocalCluster (thanks KasperMadsen)
 * Added rebalance, activate, deactivate, and killTopologyWithOpts methods to LocalCluster
 * Added custom stream groupings to LinearDRPC API
 * Simplify multilang protocol to use json for all messages (thanks tomoj)
 * Bug fix: Fixed string encoding in ShellBolt protocol to be UTF-8 (thanks nicoo)
 * Bug fix: Fixed race condition in FeederSpout that could lead to dropped messages
 * Bug fix: Quoted arguments with spaces now work properly with storm client script
 * Bug fix: Workers start properly when topology name has spaces
 * Bug fix: UI works properly when there are spaces in topology or spout/bolt names (thanks xiaokang)
 * Bug fix: Tuple$Seq now returns correct count (thanks travisfw)

## 0.7.0

 * Transactional topologies: a new higher level abstraction that enables exactly-once messaging semantics for most computations. Documented on the wiki.
 * Component-specific configurations: Can now set configurations on a per-spout or per-bolt basis. 
 * New batch bolt abstraction that simplifies the processing of batches in DRPC or transactional topologies. A new batch bolt is created per batch and they are automatically cleaned up.
 * Introduction of base classes for various bolt and spout types. These base classes are in the backtype.storm.topology.base package and provide empty implementations for commonly unused methods
 * CoordinatedBolt generalized to handle non-linear topologies. This will make it easy to implement a non-linear DRPC topology abstraction.
 * Can customize the JVM options for Storm UI with new ui.childopts config
 * BigIntegers are now serializable by default
 * All bolts/spouts now emit a system stream (id "__system"). Currently it only emits startup events, but may emit other events in the future.
 * Optimized tuple trees for batch processing in DRPC and transactional topologies. Only the coordination tuples are anchored. OutputCollector#fail still works because CoordinatedBolt will propagate the fail to all other tuples in the batch. 
 * CoordinatedBolt moved to backtype.storm.coordination package
 * Clojure test framework significantly more composable
 * Massive internal refactorings and simplifications, including changes to the Thrift definition for storm topologies.
 * Optimized acking system. Bolts with zero or more than one consumer used to send an additional ack message. Now those are no longer sent.
 * Changed interface of CustomStreamGrouping to receive a List<Object> rather than a Tuple.
 * Added "storm.zookeeper.retry.times" and "storm.zookeeper.retry.interval" configs (thanks killme2008)
 * Added "storm help" and "storm help {cmd}" to storm script (thanks kachayev)
 * Logging now always goes to logs/ in the Storm directory, regardless of where you launched the daemon (thanks haitaoyao)
 * Improved Clojure DSL: can emit maps and Tuples implement the appropriate interfaces to integrate with Clojure's seq functions (thanks schleyfox)
 * Added "ui.childopts" config (thanks ddillinger)
 * Bug fix: OutputCollector no longer assumes immutable inputs [NOTE: this was reverted in 0.7.2 because it conflicts with sending tuples to colocated tasks without serialization]
 * Bug fix: DRPC topologies now throw a proper error when no DRPC servers are configured instead of NPE (thanks danharvey)
 * Bug fix: Fix local mode so multiple topologies can be run on one LocalCluster
 * Bug fix: "storm supervisor" now uses supervisor.childopts instead of nimbus.childopts (thanks ddillinger)
 * Bug fix: supervisor.childopts and nimbus.childopts can now contain whitespace. Previously only the first token was taken from the string
 * Bug fix: Make TopologyContext "getThisTaskIndex" and "getComponentTasks" consistent
 * Bug fix: Fix NoNodeException that would pop up with task heartbeating under heavy load
 * Bug fix: Catch InterruptedExceptions appropriately in local mode so shutdown always works properly

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
