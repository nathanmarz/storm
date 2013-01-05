package backtype.storm;

import backtype.storm.serialization.IKryoDecorator;
import backtype.storm.serialization.IKryoFactory;
import com.esotericsoftware.kryo.Serializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Topology configs are specified as a plain old map. This class provides a 
 * convenient way to create a topology config map by providing setter methods for 
 * all the configs that can be set. It also makes it easier to do things like add 
 * serializations.
 * 
 * <p>This class also provides constants for all the configurations possible on a Storm
 * cluster and Storm topology. Default values for these configs can be found in
 * defaults.yaml.</p>
 *
 * <p>Note that you may put other configurations in any of the configs. Storm
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of 
 * Spouts. .</p>
 */
public class Config extends HashMap<String, Object> {
    
    /**
     * A list of hosts of ZooKeeper servers used to manage the cluster.
     */
    public static String STORM_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";

    /**
     * The port Storm will use to connect to each of the ZooKeeper servers.
     */
    public static String STORM_ZOOKEEPER_PORT = "storm.zookeeper.port";

    /**
     * A directory on the local filesystem used by Storm for any local
     * filesystem usage it needs. The directory must exist and the Storm daemons must
     * have permission to read/write from this location.
     */
    public static String STORM_LOCAL_DIR = "storm.local.dir";

    /**
     * A global task scheduler used to assign topologies's tasks to supervisors' wokers.
     * 
     * If this is not set, a default system scheduler will be used.
     */
    public static String STORM_SCHEDULER = "storm.scheduler";

    /**
     * The mode this Storm cluster is running in. Either "distributed" or "local".
     */
    public static String STORM_CLUSTER_MODE = "storm.cluster.mode";

    /**
     * The hostname the supervisors/workers should report to nimbus. If unset, Storm will 
     * get the hostname to report by calling <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     * 
     * You should set this config when you dont have a DNS which supervisors/workers
     * can utilize to find each other based on hostname got from calls to
     * <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     */
    public static String STORM_LOCAL_HOSTNAME = "storm.local.hostname";

    /**
     * Whether or not to use ZeroMQ for messaging in local mode. If this is set 
     * to false, then Storm will use a pure-Java messaging system. The purpose 
     * of this flag is to make it easy to run Storm in local mode by eliminating 
     * the need for native dependencies, which can be difficult to install.
     *
     * Defaults to false.
     */
    public static String STORM_LOCAL_MODE_ZMQ = "storm.local.mode.zmq";

    /**
     * The root location at which Storm stores data in ZooKeeper.
     */
    public static String STORM_ZOOKEEPER_ROOT = "storm.zookeeper.root";

    /**
     * The session timeout for clients to ZooKeeper.
     */
    public static String STORM_ZOOKEEPER_SESSION_TIMEOUT = "storm.zookeeper.session.timeout";

    /**
     * The connection timeout for clients to ZooKeeper.
     */
    public static String STORM_ZOOKEEPER_CONNECTION_TIMEOUT = "storm.zookeeper.connection.timeout";
    
    
    /**
     * The number of times to retry a Zookeeper operation.
     */
    public static String STORM_ZOOKEEPER_RETRY_TIMES="storm.zookeeper.retry.times";
    
    /**
     * The interval between retries of a Zookeeper operation.
     */
    public static String STORM_ZOOKEEPER_RETRY_INTERVAL="storm.zookeeper.retry.interval";

    /**
     * The Zookeeper authentication scheme to use, e.g. "digest". Defaults to no authentication.
     */
    public static String STORM_ZOOKEEPER_AUTH_SCHEME="storm.zookeeper.auth.scheme";
    
    /**
     * A string representing the payload for Zookeeper authentication. It gets serialized using UTF-8 encoding during authentication.
     */
    public static String STORM_ZOOKEEPER_AUTH_PAYLOAD="storm.zookeeper.auth.payload";
    
    /**
     * The id assigned to a running topology. The id is the storm name with a unique nonce appended.
     */
    public static String STORM_ID = "storm.id";
    
    /**
     * The host that the master server is running on.
     */
    public static String NIMBUS_HOST = "nimbus.host";

    /**
     * Which port the Thrift interface of Nimbus should run on. Clients should
     * connect to this port to upload jars and submit topologies.
     */
    public static String NIMBUS_THRIFT_PORT = "nimbus.thrift.port";


    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the nimbus daemon.
     */
    public static String NIMBUS_CHILDOPTS = "nimbus.childopts";


    /**
     * How long without heartbeating a task can go before nimbus will consider the
     * task dead and reassign it to another location.
     */
    public static String NIMBUS_TASK_TIMEOUT_SECS = "nimbus.task.timeout.secs";


    /**
     * How often nimbus should wake up to check heartbeats and do reassignments. Note
     * that if a machine ever goes down Nimbus will immediately wake up and take action.
     * This parameter is for checking for failures when there's no explicit event like that
     * occuring.
     */
    public static String NIMBUS_MONITOR_FREQ_SECS = "nimbus.monitor.freq.secs";

    /**
     * How often nimbus should wake the cleanup thread to clean the inbox.
     * @see NIMBUS_INBOX_JAR_EXPIRATION_SECS
     */
    public static String NIMBUS_CLEANUP_INBOX_FREQ_SECS = "nimbus.cleanup.inbox.freq.secs";

    /**
     * The length of time a jar file lives in the inbox before being deleted by the cleanup thread.
     *
     * Probably keep this value greater than or equal to NIMBUS_CLEANUP_INBOX_JAR_EXPIRATION_SECS.
     * Note that the time it takes to delete an inbox jar file is going to be somewhat more than
     * NIMBUS_CLEANUP_INBOX_JAR_EXPIRATION_SECS (depending on how often NIMBUS_CLEANUP_FREQ_SECS
     * is set to).
     * @see NIMBUS_CLEANUP_FREQ_SECS
     */
    public static String NIMBUS_INBOX_JAR_EXPIRATION_SECS = "nimbus.inbox.jar.expiration.secs";

    /**
     * How long before a supervisor can go without heartbeating before nimbus considers it dead
     * and stops assigning new work to it.
     */
    public static String NIMBUS_SUPERVISOR_TIMEOUT_SECS = "nimbus.supervisor.timeout.secs";

    /**
     * A special timeout used when a task is initially launched. During launch, this is the timeout
     * used until the first heartbeat, overriding nimbus.task.timeout.secs.
     *
     * <p>A separate timeout exists for launch because there can be quite a bit of overhead
     * to launching new JVM's and configuring them.</p>
     */
    public static String NIMBUS_TASK_LAUNCH_SECS = "nimbus.task.launch.secs";

    /**
     * Whether or not nimbus should reassign tasks if it detects that a task goes down. 
     * Defaults to true, and it's not recommended to change this value.
     */
    public static String NIMBUS_REASSIGN = "nimbus.reassign";

    /**
     * During upload/download with the master, how long an upload or download connection is idle
     * before nimbus considers it dead and drops the connection.
     */
    public static String NIMBUS_FILE_COPY_EXPIRATION_SECS = "nimbus.file.copy.expiration.secs";

    /**
     * A custom class that implements ITopologyValidator that is run whenever a
     * topology is submitted. Can be used to provide business-specific logic for
     * whether topologies are allowed to run or not.
     */
    public static String NIMBUS_TOPOLOGY_VALIDATOR = "nimbus.topology.validator";
    
    
    /**
     * Storm UI binds to this port.
     */
    public static String UI_PORT = "ui.port";

    /**
     * Childopts for Storm UI Java process.
     */
    public static String UI_CHILDOPTS = "ui.childopts";
    
    
    /**
     * List of DRPC servers so that the DRPCSpout knows who to talk to.
     */
    public static String DRPC_SERVERS = "drpc.servers";

    /**
     * This port is used by Storm DRPC for receiving DPRC requests from clients.
     */
    public static String DRPC_PORT = "drpc.port";
    
    /**
     * DRPC thrift server worker threads 
     */
    public static String DRPC_WORKER_THREADS = "drpc.worker.threads";

    /**
     * DRPC thrift server queue size 
     */
    public static String DRPC_QUEUE_SIZE = "drpc.queue.size";
    
    /**
     * This port on Storm DRPC is used by DRPC topologies to receive function invocations and send results back. 
     */
    public static String DRPC_INVOCATIONS_PORT = "drpc.invocations.port";  
    
    /**
     * The timeout on DRPC requests within the DRPC server. Defaults to 10 minutes. Note that requests can also
     * timeout based on the socket timeout on the DRPC client, and separately based on the topology message
     * timeout for the topology implementing the DRPC function.
     */
    public static String DRPC_REQUEST_TIMEOUT_SECS  = "drpc.request.timeout.secs";  
    
    /**
     * the metadata configed on the supervisor
     */    
    public static String SUPERVISOR_SCHEDULER_META = "supervisor.scheduler.meta";
    /**
     * A list of ports that can run workers on this supervisor. Each worker uses one port, and
     * the supervisor will only run one worker per port. Use this configuration to tune
     * how many workers run on each machine.
     */
    public static String SUPERVISOR_SLOTS_PORTS = "supervisor.slots.ports";



    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the supervisor daemon.
     */
    public static String SUPERVISOR_CHILDOPTS = "supervisor.childopts";


    /**
     * How long a worker can go without heartbeating before the supervisor tries to
     * restart the worker process.
     */
    public static String SUPERVISOR_WORKER_TIMEOUT_SECS = "supervisor.worker.timeout.secs";


    /**
     * How long a worker can go without heartbeating during the initial launch before
     * the supervisor tries to restart the worker process. This value override
     * supervisor.worker.timeout.secs during launch because there is additional
     * overhead to starting and configuring the JVM on launch.
     */
    public static String SUPERVISOR_WORKER_START_TIMEOUT_SECS = "supervisor.worker.start.timeout.secs";


    /**
     * Whether or not the supervisor should launch workers assigned to it. Defaults
     * to true -- and you should probably never change this value. This configuration
     * is used in the Storm unit tests.
     */
    public static String SUPERVISOR_ENABLE = "supervisor.enable";


    /**
     * how often the supervisor sends a heartbeat to the master.
     */
    public static String SUPERVISOR_HEARTBEAT_FREQUENCY_SECS = "supervisor.heartbeat.frequency.secs";


    /**
     * How often the supervisor checks the worker heartbeats to see if any of them
     * need to be restarted.
     */
    public static String SUPERVISOR_MONITOR_FREQUENCY_SECS = "supervisor.monitor.frequency.secs";
    
    /**
     * The jvm opts provided to workers launched by this supervisor. All "%ID%" substrings are replaced
     * with an identifier for this worker.
     */
    public static String WORKER_CHILDOPTS = "worker.childopts";


    /**
     * How often this worker should heartbeat to the supervisor.
     */
    public static String WORKER_HEARTBEAT_FREQUENCY_SECS = "worker.heartbeat.frequency.secs";

    /**
     * How often a task should heartbeat its status to the master.
     */
    public static String TASK_HEARTBEAT_FREQUENCY_SECS = "task.heartbeat.frequency.secs";


    /**
     * How often a task should sync its connections with other tasks (if a task is
     * reassigned, the other tasks sending messages to it need to refresh their connections).
     * In general though, when a reassignment happens other tasks will be notified
     * almost immediately. This configuration is here just in case that notification doesn't
     * come through.
     */
    public static String TASK_REFRESH_POLL_SECS = "task.refresh.poll.secs";

    
    
    /**
     * True if Storm should timeout messages or not. Defaults to true. This is meant to be used
     * in unit tests to prevent tuples from being accidentally timed out during the test.
     */
    public static String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
    
    /**
     * When set to true, Storm will log every message that's emitted.
     */
    public static String TOPOLOGY_DEBUG = "topology.debug";


    /**
     * Whether or not the master should optimize topologies by running multiple
     * tasks in a single thread where appropriate.
     */
    public static String TOPOLOGY_OPTIMIZE = "topology.optimize";

    /**
     * How many processes should be spawned around the cluster to execute this
     * topology. Each process will execute some number of tasks as threads within
     * them. This parameter should be used in conjunction with the parallelism hints
     * on each component in the topology to tune the performance of a topology.
     */
    public static String TOPOLOGY_WORKERS = "topology.workers";

    /**
     * How many instances to create for a spout/bolt. A task runs on a thread with zero or more
     * other tasks for the same spout/bolt. The number of tasks for a spout/bolt is always
     * the same throughout the lifetime of a topology, but the number of executors (threads) for 
     * a spout/bolt can change over time. This allows a topology to scale to more or less resources 
     * without redeploying the topology or violating the constraints of Storm (such as a fields grouping
     * guaranteeing that the same value goes to the same task).
     */
    public static String TOPOLOGY_TASKS = "topology.tasks";

    /**
     * How many executors to spawn for ackers.
     *
     * <p>If this is set to 0, then Storm will immediately ack tuples as soon
     * as they come off the spout, effectively disabling reliability.</p>
     */
    public static String TOPOLOGY_ACKER_EXECUTORS = "topology.acker.executors";


    /**
     * The maximum amount of time given to the topology to fully process a message
     * emitted by a spout. If the message is not acked within this time frame, Storm
     * will fail the message on the spout. Some spouts implementations will then replay
     * the message at a later time.
     */
    public static String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";

    /**
     * A list of serialization registrations for Kryo ( http://code.google.com/p/kryo/ ),
     * the underlying serialization framework for Storm. A serialization can either
     * be the name of a class (in which case Kryo will automatically create a serializer for the class
     * that saves all the object's fields), or an implementation of com.esotericsoftware.kryo.Serializer.
     *
     * See Kryo's documentation for more information about writing custom serializers.
     */
    public static String TOPOLOGY_KRYO_REGISTER = "topology.kryo.register";

    /**
     * A list of classes that customize storm's kryo instance during start-up.
     * Each listed class name must implement IKryoDecorator. During start-up the 
     * listed class is instantiated with 0 arguments, then its 'decorate' method 
     * is called with storm's kryo instance as the only argument.
     */
    public static String TOPOLOGY_KRYO_DECORATORS = "topology.kryo.decorators";

    /**
     * Class that specifies how to create a Kryo instance for serialization. Storm will then apply
     * topology.kryo.register and topology.kryo.decorators on top of this. The default implementation
     * implements topology.fall.back.on.java.serialization and turns references off.
     */
    public static String TOPOLOGY_KRYO_FACTORY = "topology.kryo.factory";

    
    /**
     * Whether or not Storm should skip the loading of kryo registrations for which it
     * does not know the class or have the serializer implementation. Otherwise, the task will
     * fail to load and will throw an error at runtime. The use case of this is if you want to
     * declare your serializations on the storm.yaml files on the cluster rather than every single
     * time you submit a topology. Different applications may use different serializations and so
     * a single application may not have the code for the other serializers used by other apps.
     * By setting this config to true, Storm will ignore that it doesn't have those other serializations
     * rather than throw an error.
     */
    public static String TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS= "topology.skip.missing.kryo.registrations";

    /*
     * A list of classes implementing IMetricsConsumer (See storm.yaml.example for exact config format).
     * Each listed class will be routed all the metrics data generated by the storm metrics API. 
     * Each listed class maps 1:1 to a system bolt named __metrics_ClassName#N, and it's parallelism is configurable.
     */
    public static String TOPOLOGY_METRICS_CONSUMER_REGISTER = "topology.metrics.consumer.register";


    /**
     * The maximum parallelism allowed for a component in this topology. This configuration is
     * typically used in testing to limit the number of threads spawned in local mode.
     */
    public static String TOPOLOGY_MAX_TASK_PARALLELISM="topology.max.task.parallelism";


    /**
     * The maximum number of tuples that can be pending on a spout task at any given time. 
     * This config applies to individual tasks, not to spouts or topologies as a whole. 
     * 
     * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
     * Note that this config parameter has no effect for unreliable spouts that don't tag 
     * their tuples with a message id.
     */
    public static String TOPOLOGY_MAX_SPOUT_PENDING="topology.max.spout.pending"; 
    
    /**
     * A class that implements a strategy for what to do when a spout needs to wait. Waiting is
     * triggered in one of two conditions:
     * 
     * 1. nextTuple emits no tuples
     * 2. The spout has hit maxSpoutPending and can't emit any more tuples
     */
    public static String TOPOLOGY_SPOUT_WAIT_STRATEGY="topology.spout.wait.strategy"; 

    /**
     * The amount of milliseconds the SleepEmptyEmitStrategy should sleep for.
     */
    public static String TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS="topology.sleep.spout.wait.strategy.time.ms";     
    
    /**
     * The maximum amount of time a component gives a source of state to synchronize before it requests
     * synchronization again.
     */
    public static String TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS="topology.state.synchronization.timeout.secs";

    /**
     * The percentage of tuples to sample to produce stats for a task.
     */
    public static String TOPOLOGY_STATS_SAMPLE_RATE="topology.stats.sample.rate";

    /**
     * The time period that builtin metrics data in bucketed into. 
     */
    public static String TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS="topology.builtin.metrics.bucket.size.secs";

    /**
     * Whether or not to use Java serialization in a topology.
     */
    public static String TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION="topology.fall.back.on.java.serialization";

    /**
     * Topology-specific options for the worker child process. This is used in addition to WORKER_CHILDOPTS.
     */
    public static String TOPOLOGY_WORKER_CHILDOPTS="topology.worker.childopts";

    /**
     * This config is available for TransactionalSpouts, and contains the id ( a String) for
     * the transactional topology. This id is used to store the state of the transactional
     * topology in Zookeeper.
     */
    public static String TOPOLOGY_TRANSACTIONAL_ID="topology.transactional.id";
    
    /**
     * A list of task hooks that are automatically added to every spout and bolt in the topology. An example
     * of when you'd do this is to add a hook that integrates with your internal 
     * monitoring system. These hooks are instantiated using the zero-arg constructor.
     */
    public static String TOPOLOGY_AUTO_TASK_HOOKS="topology.auto.task.hooks";


    /**
     * The size of the Disruptor receive queue for each executor. Must be a power of 2.
     */
    public static String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE="topology.executor.receive.buffer.size";

    /**
     * The maximum number of messages to batch from the thread receiving off the network to the 
     * executor queues. Must be a power of 2.
     */
    public static String TOPOLOGY_RECEIVER_BUFFER_SIZE="topology.receiver.buffer.size";

    /**
     * The size of the Disruptor send queue for each executor. Must be a power of 2.
     */
    public static String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE="topology.executor.send.buffer.size";

    /**
     * The size of the Disruptor transfer queue for each worker.
     */
    public static String TOPOLOGY_TRANSFER_BUFFER_SIZE="topology.transfer.buffer.size";

    /**
     * How often a tick tuple from the "__system" component and "__tick" stream should be sent
     * to tasks. Meant to be used as a component-specific configuration.
     */
     public static String TOPOLOGY_TICK_TUPLE_FREQ_SECS="topology.tick.tuple.freq.secs";


    /**
     * Configure the wait strategy used for internal queuing. Can be used to tradeoff latency
     * vs. throughput
     */
     public static String TOPOLOGY_DISRUPTOR_WAIT_STRATEGY="topology.disruptor.wait.strategy";
    
    /**
     * The size of the shared thread pool for worker tasks to make use of. The thread pool can be accessed 
     * via the TopologyContext.
     */
     public static String TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE="topology.worker.shared.thread.pool.size";

     /**
      * The interval in seconds to use for determining whether to throttle error reported to Zookeeper. For example, 
      * an interval of 10 seconds with topology.max.error.report.per.interval set to 5 will only allow 5 errors to be
      * reported to Zookeeper per task for every 10 second interval of time.
      */
     public static String TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS="topology.error.throttle.interval.secs";

     /**
      * See doc for TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS
      */
     public static String TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL="topology.max.error.report.per.interval";


     /**
      * How often a batch can be emitted in a Trident topology.
      */
     public static String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS="topology.trident.batch.emit.interval.millis";

    /**
     * Name of the topology. This config is automatically set by Storm when the topology is submitted.
     */
    public static String TOPOLOGY_NAME="topology.name";  
    
    /**
     * The root directory in ZooKeeper for metadata about TransactionalSpouts.
     */
    public static String TRANSACTIONAL_ZOOKEEPER_ROOT="transactional.zookeeper.root";
    
    /**
     * The list of zookeeper servers in which to keep the transactional state. If null (which is default),
     * will use storm.zookeeper.servers
     */
    public static String TRANSACTIONAL_ZOOKEEPER_SERVERS="transactional.zookeeper.servers";

    /**
     * The port to use to connect to the transactional zookeeper servers. If null (which is default),
     * will use storm.zookeeper.port
     */
    public static String TRANSACTIONAL_ZOOKEEPER_PORT="transactional.zookeeper.port";
    
    /**
     * The number of threads that should be used by the zeromq context in each worker process.
     */
    public static String ZMQ_THREADS = "zmq.threads";

    /**
     * How long a connection should retry sending messages to a target host when
     * the connection is closed. This is an advanced configuration and can almost
     * certainly be ignored.
     */
    public static String ZMQ_LINGER_MILLIS = "zmq.linger.millis";

    /**
     * The high water for the ZeroMQ push sockets used for networking. Use this config to prevent buffer explosion
     * on the networking layer.
     */
    public static String ZMQ_HWM = "zmq.hwm";
        
    /**
     * This value is passed to spawned JVMs (e.g., Nimbus, Supervisor, and Workers)
     * for the java.library.path value. java.library.path tells the JVM where 
     * to look for native libraries. It is necessary to set this config correctly since
     * Storm uses the ZeroMQ and JZMQ native libs. 
     */
    public static String JAVA_LIBRARY_PATH = "java.library.path";
    
    /**
     * The path to use as the zookeeper dir when running a zookeeper server via
     * "storm dev-zookeeper". This zookeeper instance is only intended for development;
     * it is not a production grade zookeeper setup.
     */
    public static String DEV_ZOOKEEPER_PATH = "dev.zookeeper.path";
    
    /**
     * A map from topology name to the number of machines that should be dedicated for that topology. Set storm.scheduler
     * to backtype.storm.scheduler.IsolationScheduler to make use of the isolation scheduler.
     */
    public static String ISOLATION_SCHEDULER_MACHINES = "isolation.scheduler.machines";
        
    public static void setDebug(Map conf, boolean isOn) {
        conf.put(Config.TOPOLOGY_DEBUG, isOn);
    } 

    public void setDebug(boolean isOn) {
        setDebug(this, isOn);
    }
    
    @Deprecated
    public void setOptimize(boolean isOn) {
        put(Config.TOPOLOGY_OPTIMIZE, isOn);
    } 
    
    public static void setNumWorkers(Map conf, int workers) {
        conf.put(Config.TOPOLOGY_WORKERS, workers);
    }

    public void setNumWorkers(int workers) {
        setNumWorkers(this, workers);
    }

    public static void setNumAckers(Map conf, int numExecutors) {
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, numExecutors);
    }

    public void setNumAckers(int numExecutors) {
        setNumAckers(this, numExecutors);
    }
    
    public static void setMessageTimeoutSecs(Map conf, int secs) {
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, secs);
    }

    public void setMessageTimeoutSecs(int secs) {
        setMessageTimeoutSecs(this, secs);
    }
    
    public static void registerSerialization(Map conf, Class klass) {
        getRegisteredSerializations(conf).add(klass.getName());
    }

    public void registerSerialization(Class klass) {
        registerSerialization(this, klass);
    }
    
    public static void registerSerialization(Map conf, Class klass, Class<? extends Serializer> serializerClass) {
        Map<String, String> register = new HashMap<String, String>();
        register.put(klass.getName(), serializerClass.getName());
        getRegisteredSerializations(conf).add(register);        
    }

    public void registerSerialization(Class klass, Class<? extends Serializer> serializerClass) {
        registerSerialization(this, klass, serializerClass);
    }
    
    public void registerMetricsConsumer(Class klass, Object argument, long parallelismHint) {
        HashMap m = new HashMap();
        m.put("class", klass.getCanonicalName());
        m.put("parallelism.hint", parallelismHint);
        m.put("argument", argument);

        List l = (List)this.get(TOPOLOGY_METRICS_CONSUMER_REGISTER);
        if(l == null) { l = new ArrayList(); }
        l.add(m);
        this.put(TOPOLOGY_METRICS_CONSUMER_REGISTER, l);
    }

    public void registerMetricsConsumer(Class klass, long parallelismHint) {
        registerMetricsConsumer(klass, null, parallelismHint);
    }

    public void registerMetricsConsumer(Class klass) {
        registerMetricsConsumer(klass, null, 1L);
    }

    public static void registerDecorator(Map conf, Class<? extends IKryoDecorator> klass) {
        getRegisteredDecorators(conf).add(klass.getName());
    }

    public void registerDecorator(Class<? extends IKryoDecorator> klass) {
        registerDecorator(this, klass);
    }
    
    public static void setKryoFactory(Map conf, Class<? extends IKryoFactory> klass) {
        conf.put(Config.TOPOLOGY_KRYO_FACTORY, klass.getName());
    }

    public void setKryoFactory(Class<? extends IKryoFactory> klass) {
        setKryoFactory(this, klass);
    }

    public static void setSkipMissingKryoRegistrations(Map conf, boolean skip) {
        conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, skip);
    }

    public void setSkipMissingKryoRegistrations(boolean skip) {
       setSkipMissingKryoRegistrations(this, skip);
    }
    
    public static void setMaxTaskParallelism(Map conf, int max) {
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, max);
    }

    public void setMaxTaskParallelism(int max) {
        setMaxTaskParallelism(this, max);
    }
    
    public static void setMaxSpoutPending(Map conf, int max) {
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, max);
    }

    public void setMaxSpoutPending(int max) {
        setMaxSpoutPending(this, max);
    }
    
    public static void setStatsSampleRate(Map conf, double rate) {
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, rate);
    }    

    public void setStatsSampleRate(double rate) {
        setStatsSampleRate(this, rate);
    }    

    public static void setFallBackOnJavaSerialization(Map conf, boolean fallback) {
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, fallback);
    }    

    public void setFallBackOnJavaSerialization(boolean fallback) {
        setFallBackOnJavaSerialization(this, fallback);
    }    
    
    private static List getRegisteredSerializations(Map conf) {
        List ret;
        if(!conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
            ret = new ArrayList();
        } else {
            ret = new ArrayList((List) conf.get(Config.TOPOLOGY_KRYO_REGISTER));
        }
        conf.put(Config.TOPOLOGY_KRYO_REGISTER, ret);
        return ret;
    }
    
    private static List getRegisteredDecorators(Map conf) {
        List ret;
        if(!conf.containsKey(Config.TOPOLOGY_KRYO_DECORATORS)) {
            ret = new ArrayList();
        } else {
            ret = new ArrayList((List) conf.get(Config.TOPOLOGY_KRYO_DECORATORS));            
        }
        conf.put(Config.TOPOLOGY_KRYO_DECORATORS, ret);
        return ret;
    }
}
