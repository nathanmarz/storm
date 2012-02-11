package backtype.storm;

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
     * The mode this Storm cluster is running in. Either "distributed" or "local".
     */
    public static String STORM_CLUSTER_MODE = "storm.cluster.mode";

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
     * The timeout for clients to ZooKeeper.
     */
    public static String STORM_ZOOKEEPER_SESSION_TIMEOUT = "storm.zookeeper.session.timeout";
    
    /**
     * The number of times to retry a Zookeeper operation.
     */
    public static String STORM_ZOOKEEPER_RETRY_TIMES="storm.zookeeper.retry.times";
    
    /**
     * The interval between retries of a Zookeeper operation.
     */
    public static String STORM_ZOOKEEPER_RETRY_INTERVAL="storm.zookeeper.retry.interval";

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
     * This port on Storm DRPC is used by DRPC topologies to receive function invocations and send results back. 
     */
    public static String DRPC_INVOCATIONS_PORT = "drpc.invocations.port";    
    
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
     * How many acker tasks should be spawned for the topology. An acker task keeps
     * track of a subset of the tuples emitted by spouts and detects when a spout
     * tuple is fully processed. When an acker task detects that a spout tuple
     * is finished, it sends a message to the spout to acknowledge the tuple. The
     * number of ackers should be scaled with the amount of throughput going
     * through the cluster for the topology. Typically, you don't need that many
     * ackers though.
     *
     * <p>If this is set to 0, then Storm will immediately ack tuples as soon
     * as they come off the spout, effectively disabling reliability.</p>
     */
    public static String TOPOLOGY_ACKERS = "topology.ackers";


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
     * The maximum amount of time a component gives a source of state to synchronize before it requests
     * synchronization again.
     */
    public static String TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS="topology.state.synchronization.timeout.secs";

    /**
     * The percentage of tuples to sample to produce stats for a task.
     */
    public static String TOPOLOGY_STATS_SAMPLE_RATE="topology.stats.sample.rate";

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
     * This value is passed to spawned JVMs (e.g., Nimbus, Supervisor, and Workers)
     * for the java.library.path value. java.library.path tells the JVM where 
     * to look for native libraries. It is necessary to set this config correctly since
     * Storm uses the ZeroMQ and JZMQ native libs. 
     */
    public static String JAVA_LIBRARY_PATH = "java.library.path";
    
    public void setDebug(boolean isOn) {
        put(Config.TOPOLOGY_DEBUG, isOn);
    } 

    public void setOptimize(boolean isOn) {
        put(Config.TOPOLOGY_OPTIMIZE, isOn);
    } 
    
    public void setNumWorkers(int workers) {
        put(Config.TOPOLOGY_WORKERS, workers);
    }
    
    public void setNumAckers(int numTasks) {
        put(Config.TOPOLOGY_ACKERS, numTasks);
    }
    
    public void setMessageTimeoutSecs(int secs) {
        put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, secs);
    }
    
    public void registerSerialization(Class klass) {
        getRegisteredSerializations().add(klass.getName());
    }
    
    public void registerSerialization(Class klass, Class<? extends Serializer> serializerClass) {
        Map<String, String> register = new HashMap<String, String>();
        register.put(klass.getName(), serializerClass.getName());
        getRegisteredSerializations().add(register);        
    }
    
    public void setSkipMissingKryoRegistrations(boolean skip) {
        put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, skip);
    }
    
    public void setMaxTaskParallelism(int max) {
        put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, max);
    }
    
    public void setMaxSpoutPending(int max) {
        put(Config.TOPOLOGY_MAX_SPOUT_PENDING, max);
    }
    
    public void setStatsSampleRate(double rate) {
        put(Config.TOPOLOGY_STATS_SAMPLE_RATE, rate);
    }    

    public void setFallBackOnJavaSerialization(boolean fallback) {
        put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, fallback);
    }    
    
    private List getRegisteredSerializations() {
        if(!containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
            put(Config.TOPOLOGY_KRYO_REGISTER, new ArrayList());
        }
        return (List) get(Config.TOPOLOGY_KRYO_REGISTER);
    }
}
