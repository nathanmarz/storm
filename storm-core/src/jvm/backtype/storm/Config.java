/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm;

import backtype.storm.serialization.IKryoDecorator;
import backtype.storm.serialization.IKryoFactory;
import backtype.storm.validation.ConfigValidationAnnotations.*;
import backtype.storm.validation.ConfigValidation.*;
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
 * This class also provides constants for all the configurations possible on
 * a Storm cluster and Storm topology. Each constant is paired with an annotation
 * that defines the validity criterion of the corresponding field. Default
 * values for these configs can be found in defaults.yaml.
 *
 * Note that you may put other configurations in any of the configs. Storm
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of
 * Spouts.
 */
public class Config extends HashMap<String, Object> {

    //DO NOT CHANGE UNLESS WE ADD IN STATE NOT STORED IN THE PARENT CLASS
    private static final long serialVersionUID = -1550278723792864455L;

    /**
     * This is part of a temporary workaround to a ZK bug, it is the 'scheme:acl' for
     * the user Nimbus and Supervisors use to authenticate with ZK.
     */
    @isString
    public static final String STORM_ZOOKEEPER_SUPERACL = "storm.zookeeper.superACL";

    /**
     * The transporter for communication among Storm tasks
     */
    @isString
    public static final String STORM_MESSAGING_TRANSPORT = "storm.messaging.transport";

    /**
     * Netty based messaging: The buffer size for send/recv buffer
     */
    @isInteger
    @isPositiveNumber
    public static final String STORM_MESSAGING_NETTY_BUFFER_SIZE = "storm.messaging.netty.buffer_size";

    /**
     * Netty based messaging: Sets the backlog value to specify when the channel binds to a local address
     */
    @isInteger
    @isPositiveNumber
    public static final String STORM_MESSAGING_NETTY_SOCKET_BACKLOG = "storm.messaging.netty.socket.backlog";

    /**
     * Netty based messaging: The max # of retries that a peer will perform when a remote is not accessible
     *@deprecated "Since netty clients should never stop reconnecting - this does not make sense anymore.
     */
    @Deprecated
    @isInteger
    public static final String STORM_MESSAGING_NETTY_MAX_RETRIES = "storm.messaging.netty.max_retries";

    /**
     * Netty based messaging: The min # of milliseconds that a peer will wait.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_MIN_SLEEP_MS = "storm.messaging.netty.min_wait_ms";

    /**
     * Netty based messaging: The max # of milliseconds that a peer will wait.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_MAX_SLEEP_MS = "storm.messaging.netty.max_wait_ms";

    /**
     * Netty based messaging: The # of worker threads for the server.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS = "storm.messaging.netty.server_worker_threads";

    /**
     * Netty based messaging: The # of worker threads for the client.
     */
    @isInteger
    public static final String STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS = "storm.messaging.netty.client_worker_threads";

    /**
     * If the Netty messaging layer is busy, the Netty client will try to batch message as more as possible up to the size of STORM_NETTY_MESSAGE_BATCH_SIZE bytes
     */
    @isInteger
    public static final String STORM_NETTY_MESSAGE_BATCH_SIZE = "storm.messaging.netty.transfer.batch.size";

    /**
     * We check with this interval that whether the Netty channel is writable and try to write pending messages
     */
    @isInteger
    public static final String STORM_NETTY_FLUSH_CHECK_INTERVAL_MS = "storm.messaging.netty.flush.check.interval.ms";

    /**
     * Netty based messaging: Is authentication required for Netty messaging from client worker process to server worker process.
     */
    @isBoolean
    public static final String STORM_MESSAGING_NETTY_AUTHENTICATION = "storm.messaging.netty.authentication";

    /**
     * The delegate for serializing metadata, should be used for serialized objects stored in zookeeper and on disk.
     * This is NOT used for compressing serialized tuples sent between topologies.
     */
    @isString
    public static final String STORM_META_SERIALIZATION_DELEGATE = "storm.meta.serialization.delegate";

    /**
     * A list of hosts of ZooKeeper servers used to manage the cluster.
     */
    @isStringList
    public static final String STORM_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";

    /**
     * The port Storm will use to connect to each of the ZooKeeper servers.
     */
    @isInteger
    @isPositiveNumber
    public static final String STORM_ZOOKEEPER_PORT = "storm.zookeeper.port";

    /**
     * A directory on the local filesystem used by Storm for any local
     * filesystem usage it needs. The directory must exist and the Storm daemons must
     * have permission to read/write from this location.
     */
    @isString
    public static final String STORM_LOCAL_DIR = "storm.local.dir";

    /**
     * A directory that holds configuration files for log4j2.
     * It can be either a relative or an absolute directory.
     * If relative, it is relative to the storm's home directory.
     */
    @isString
    public static final String STORM_LOG4J2_CONF_DIR = "storm.log4j2.conf.dir";

    /**
     * A global task scheduler used to assign topologies's tasks to supervisors' workers.
     *
     * If this is not set, a default system scheduler will be used.
     */
    @isString
    public static final String STORM_SCHEDULER = "storm.scheduler";

    /**
     * Whether we want to display all the resource capacity and scheduled usage on the UI page.
     * We suggest to have this variable set if you are using any kind of resource-related scheduler.
     *
     * If this is not set, we will not display resource capacity and usage on the UI.
     */
    @isBoolean
    public static final String SCHEDULER_DISPLAY_RESOURCE = "scheduler.display.resource";

    /**
     * The mode this Storm cluster is running in. Either "distributed" or "local".
     */
    @isString
    public static final String STORM_CLUSTER_MODE = "storm.cluster.mode";

    /**
     * What Network Topography detection classes should we use.
     * Given a list of supervisor hostnames (or IP addresses), this class would return a list of
     * rack names that correspond to the supervisors. This information is stored in Cluster.java, and
     * is used in the resource aware scheduler.
     */
    @isString
    public static final String STORM_NETWORK_TOPOGRAPHY_PLUGIN = "storm.network.topography.plugin";

    /**
     * The hostname the supervisors/workers should report to nimbus. If unset, Storm will
     * get the hostname to report by calling <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     *
     * You should set this config when you don't have a DNS which supervisors/workers
     * can utilize to find each other based on hostname got from calls to
     * <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     */
    @isString
    public static final String STORM_LOCAL_HOSTNAME = "storm.local.hostname";

    /**
     * The plugin that will convert a principal to a local user.
     */
    @isString
    public static final String STORM_PRINCIPAL_TO_LOCAL_PLUGIN = "storm.principal.tolocal";

    /**
     * The plugin that will provide user groups service
     */
    @isString
    public static final String STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN = "storm.group.mapping.service";

    /**
     * Max no.of seconds group mapping service will cache user groups
     */
    @isInteger
    public static final String STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS = "storm.group.mapping.service.cache.duration.secs";

    /**
     * Initialization parameters for the group mapping service plugin.
     * Provides a way for a @link{STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN}
     * implementation to access optional settings.
     */
    @isType(type=Map.class)
    public static final String STORM_GROUP_MAPPING_SERVICE_PARAMS = "storm.group.mapping.service.params";

    /**
     * The default transport plug-in for Thrift client/server communication
     */
    @isString
    public static final String STORM_THRIFT_TRANSPORT_PLUGIN = "storm.thrift.transport";

    /**
     * The serializer class for ListDelegate (tuple payload).
     * The default serializer will be ListDelegateSerializer
     */
    @isString
    public static final String TOPOLOGY_TUPLE_SERIALIZER = "topology.tuple.serializer";

    /**
     * Disable load aware grouping support.
     */
    @isBoolean
    @NotNull
    public static final String TOPOLOGY_DISABLE_LOADAWARE_MESSAGING = "topology.disable.loadaware.messaging";

    /**
     * Try to serialize all tuples, even for local transfers.  This should only be used
     * for testing, as a sanity check that all of your tuples are setup properly.
     */
    @isBoolean
    public static final String TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE = "topology.testing.always.try.serialize";

    /**
     * Whether or not to use ZeroMQ for messaging in local mode. If this is set
     * to false, then Storm will use a pure-Java messaging system. The purpose
     * of this flag is to make it easy to run Storm in local mode by eliminating
     * the need for native dependencies, which can be difficult to install.
     *
     * Defaults to false.
     */
    @isBoolean
    public static final String STORM_LOCAL_MODE_ZMQ = "storm.local.mode.zmq";

    /**
     * The root location at which Storm stores data in ZooKeeper.
     */
    @isString
    public static final String STORM_ZOOKEEPER_ROOT = "storm.zookeeper.root";

    /**
     * The session timeout for clients to ZooKeeper.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_SESSION_TIMEOUT = "storm.zookeeper.session.timeout";

    /**
     * The connection timeout for clients to ZooKeeper.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_CONNECTION_TIMEOUT = "storm.zookeeper.connection.timeout";

    /**
     * The number of times to retry a Zookeeper operation.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_RETRY_TIMES="storm.zookeeper.retry.times";

    /**
     * The interval between retries of a Zookeeper operation.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_RETRY_INTERVAL="storm.zookeeper.retry.interval";

    /**
     * The ceiling of the interval between retries of a Zookeeper operation.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING="storm.zookeeper.retry.intervalceiling.millis";

    /**
     * The cluster Zookeeper authentication scheme to use, e.g. "digest". Defaults to no authentication.
     */
    @isString
    public static final String STORM_ZOOKEEPER_AUTH_SCHEME="storm.zookeeper.auth.scheme";

    /**
     * A string representing the payload for cluster Zookeeper authentication.
     * It gets serialized using UTF-8 encoding during authentication.
     * Note that if this is set to something with a secret (as when using
     * digest authentication) then it should only be set in the
     * storm-cluster-auth.yaml file.
     * This file storm-cluster-auth.yaml should then be protected with
     * appropriate permissions that deny access from workers.
     */
    @isString
    public static final String STORM_ZOOKEEPER_AUTH_PAYLOAD="storm.zookeeper.auth.payload";

    /**
     * The topology Zookeeper authentication scheme to use, e.g. "digest". Defaults to no authentication.
     */
    @isString
    public static final String STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME="storm.zookeeper.topology.auth.scheme";

    /**
     * A string representing the payload for topology Zookeeper authentication. It gets serialized using UTF-8 encoding during authentication.
     */
    @isString
    public static final String STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD="storm.zookeeper.topology.auth.payload";

    /**
     * The id assigned to a running topology. The id is the storm name with a unique nonce appended.
     */
    @isString
    public static final String STORM_ID = "storm.id";

    /**
     * The directory where storm's health scripts go.
     */
    @isString
    public static final String STORM_HEALTH_CHECK_DIR = "storm.health.check.dir";

    /**
     * The time to allow any given healthcheck script to run before it
     * is marked failed due to timeout
     */
    @isNumber
    public static final String STORM_HEALTH_CHECK_TIMEOUT_MS = "storm.health.check.timeout.ms";

    /**
     * The number of times to retry a Nimbus operation.
     */
    @isNumber
    public static final String STORM_NIMBUS_RETRY_TIMES="storm.nimbus.retry.times";

    /**
     * The starting interval between exponential backoff retries of a Nimbus operation.
     */
    @isNumber
    public static final String STORM_NIMBUS_RETRY_INTERVAL="storm.nimbus.retry.interval.millis";

    /**
     * The ceiling of the interval between retries of a client connect to Nimbus operation.
     */
    @isNumber
    public static final String STORM_NIMBUS_RETRY_INTERVAL_CEILING="storm.nimbus.retry.intervalceiling.millis";

    /**
     * The ClusterState factory that worker will use to create a ClusterState
     * to store state in. Defaults to ZooKeeper.
     */
    @isString
    public static final String STORM_CLUSTER_STATE_STORE = "storm.cluster.state.store";

    /**
     * The Nimbus transport plug-in for Thrift client/server communication
     */
    @isString
    public static final String NIMBUS_THRIFT_TRANSPORT_PLUGIN = "nimbus.thrift.transport";

    /**
     * The host that the master server is running on, added only for backward compatibility,
     * the usage deprecated in favor of nimbus.seeds config.
     */
    @Deprecated
    @isString
    public static final String NIMBUS_HOST = "nimbus.host";

    /**
     * List of seed nimbus hosts to use for leader nimbus discovery.
     */
    @isStringList
    public static final String NIMBUS_SEEDS = "nimbus.seeds";

    /**
     * Which port the Thrift interface of Nimbus should run on. Clients should
     * connect to this port to upload jars and submit topologies.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_THRIFT_PORT = "nimbus.thrift.port";

    /**
     * The number of threads that should be used by the nimbus thrift server.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_THRIFT_THREADS = "nimbus.thrift.threads";

    /**
     * A list of users that are cluster admins and can run any command.  To use this set
     * nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String NIMBUS_ADMINS = "nimbus.admins";

    /**
     * A list of users that are the only ones allowed to run user operation on storm cluster.
     * To use this set nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String NIMBUS_USERS = "nimbus.users";

    /**
     * A list of groups , users belong to these groups are the only ones allowed to run user operation on storm cluster.
     * To use this set nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String NIMBUS_GROUPS = "nimbus.groups";

    /**
     * A list of users that run the supervisors and should be authorized to interact with
     * nimbus as a supervisor would.  To use this set
     * nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String NIMBUS_SUPERVISOR_USERS = "nimbus.supervisor.users";

    /**
     * This is the user that the Nimbus daemon process is running as. May be used when security
     * is enabled to authorize actions in the cluster.
     */
    @isString
    public static final String NIMBUS_DAEMON_USER = "nimbus.daemon.user";

    /**
     * The maximum buffer size thrift should use when reading messages.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_THRIFT_MAX_BUFFER_SIZE = "nimbus.thrift.max_buffer_size";

    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the nimbus daemon.
     */
    @isString
    public static final String NIMBUS_CHILDOPTS = "nimbus.childopts";


    /**
     * How long without heartbeating a task can go before nimbus will consider the
     * task dead and reassign it to another location.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_TASK_TIMEOUT_SECS = "nimbus.task.timeout.secs";


    /**
     * How often nimbus should wake up to check heartbeats and do reassignments. Note
     * that if a machine ever goes down Nimbus will immediately wake up and take action.
     * This parameter is for checking for failures when there's no explicit event like that
     * occurring.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_MONITOR_FREQ_SECS = "nimbus.monitor.freq.secs";

    /**
     * How often nimbus should wake the cleanup thread to clean the inbox.
     * @see #NIMBUS_INBOX_JAR_EXPIRATION_SECS
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_CLEANUP_INBOX_FREQ_SECS = "nimbus.cleanup.inbox.freq.secs";

    /**
     * The length of time a jar file lives in the inbox before being deleted by the cleanup thread.
     *
     * Probably keep this value greater than or equal to NIMBUS_CLEANUP_INBOX_JAR_EXPIRATION_SECS.
     * Note that the time it takes to delete an inbox jar file is going to be somewhat more than
     * NIMBUS_CLEANUP_INBOX_JAR_EXPIRATION_SECS (depending on how often NIMBUS_CLEANUP_FREQ_SECS
     * is set to).
     * @see #NIMBUS_CLEANUP_INBOX_FREQ_SECS
     */
    @isInteger
    public static final String NIMBUS_INBOX_JAR_EXPIRATION_SECS = "nimbus.inbox.jar.expiration.secs";

    /**
     * How long before a supervisor can go without heartbeating before nimbus considers it dead
     * and stops assigning new work to it.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_SUPERVISOR_TIMEOUT_SECS = "nimbus.supervisor.timeout.secs";

    /**
     * A special timeout used when a task is initially launched. During launch, this is the timeout
     * used until the first heartbeat, overriding nimbus.task.timeout.secs.
     *
     * <p>A separate timeout exists for launch because there can be quite a bit of overhead
     * to launching new JVM's and configuring them.</p>
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_TASK_LAUNCH_SECS = "nimbus.task.launch.secs";

    /**
     * During upload/download with the master, how long an upload or download connection is idle
     * before nimbus considers it dead and drops the connection.
     */
    @isInteger
    public static final String NIMBUS_FILE_COPY_EXPIRATION_SECS = "nimbus.file.copy.expiration.secs";

    /**
     * A custom class that implements ITopologyValidator that is run whenever a
     * topology is submitted. Can be used to provide business-specific logic for
     * whether topologies are allowed to run or not.
     */
    @isString
    public static final String NIMBUS_TOPOLOGY_VALIDATOR = "nimbus.topology.validator";

    /**
     * Class name for authorization plugin for Nimbus
     */
    @isString
    public static final String NIMBUS_AUTHORIZER = "nimbus.authorizer";

    /**
     * Impersonation user ACL config entries.
     */
    @isString
    public static final String NIMBUS_IMPERSONATION_AUTHORIZER = "nimbus.impersonation.authorizer";

    /**
     * Impersonation user ACL config entries.
     */
    @isMapEntryCustom(keyValidatorClasses = {StringValidator.class}, valueValidatorClasses = {ImpersonationAclUserEntryValidator.class})
    public static final String NIMBUS_IMPERSONATION_ACL = "nimbus.impersonation.acl";

    /**
     * How often nimbus should wake up to renew credentials if needed.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_CREDENTIAL_RENEW_FREQ_SECS = "nimbus.credential.renewers.freq.secs";

    /**
     * A list of credential renewers that nimbus should load.
     */
    @isStringList
    public static final String NIMBUS_CREDENTIAL_RENEWERS = "nimbus.credential.renewers.classes";

    /**
     * A list of plugins that nimbus should load during submit topology to populate
     * credentials on user's behalf.
     */
    @isStringList
    public static final String NIMBUS_AUTO_CRED_PLUGINS = "nimbus.autocredential.plugins.classes";

    /**
     * FQCN of a class that implements {@code ITopologyActionNotifierPlugin} @see backtype.storm.nimbus.ITopologyActionNotifierPlugin for details.
     */
    @isString
    public static final String NIMBUS_TOPOLOGY_ACTION_NOTIFIER_PLUGIN = "nimbus.topology.action.notifier.plugin.class";

    /**
     * Storm UI binds to this host/interface.
     */
    @isString
    public static final String UI_HOST = "ui.host";

    /**
     * Storm UI binds to this port.
     */
    @isInteger
    @isPositiveNumber
    public static final String UI_PORT = "ui.port";

    /**
     * Storm UI Project BUGTRACKER Link for reporting issue.
     */
    @isString
    public static final String UI_PROJECT_BUGTRACKER_URL = "ui.project.bugtracker.url";

    /**
     * Storm UI Central Logging URL.
     */
    @isString
    public static final String UI_CENTRAL_LOGGING_URL = "ui.central.logging.url";

    /**
     * HTTP UI port for log viewer
     */
    @isInteger
    @isPositiveNumber
    public static final String LOGVIEWER_PORT = "logviewer.port";

    /**
     * Childopts for log viewer java process.
     */
    @isString
    public static final String LOGVIEWER_CHILDOPTS = "logviewer.childopts";

    /**
     * How often to clean up old log files
     */
    @isInteger
    @isPositiveNumber
    public static final String LOGVIEWER_CLEANUP_INTERVAL_SECS = "logviewer.cleanup.interval.secs";

    /**
     * How many minutes since a log was last modified for the log to be considered for clean-up
     */
    @isInteger
    @isPositiveNumber
    public static final String LOGVIEWER_CLEANUP_AGE_MINS = "logviewer.cleanup.age.mins";

    /**
     * The maximum number of bytes all worker log files can take up in MB
     */
    @isPositiveNumber
    public static final String LOGVIEWER_MAX_SUM_WORKER_LOGS_SIZE_MB = "logviewer.max.sum.worker.logs.size.mb";

    /**
     * The maximum number of bytes per worker's files can take up in MB
     */
    @isPositiveNumber
    public static final String LOGVIEWER_MAX_PER_WORKER_LOGS_SIZE_MB = "logviewer.max.per.worker.logs.size.mb";

    /**
     * Storm Logviewer HTTPS port
     */
    @isInteger
    @isPositiveNumber
    public static final String LOGVIEWER_HTTPS_PORT = "logviewer.https.port";

    /**
     * Path to the keystore containing the certs used by Storm Logviewer for HTTPS communications
     */
    @isString
    public static final String LOGVIEWER_HTTPS_KEYSTORE_PATH = "logviewer.https.keystore.path";

    /**
     * Password for the keystore for HTTPS for Storm Logviewer
     */
    @isString
    public static final String LOGVIEWER_HTTPS_KEYSTORE_PASSWORD = "logviewer.https.keystore.password";

    /**
     * Type of the keystore for HTTPS for Storm Logviewer.
     * see http://docs.oracle.com/javase/8/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String LOGVIEWER_HTTPS_KEYSTORE_TYPE = "logviewer.https.keystore.type";

    /**
     * Password to the private key in the keystore for setting up HTTPS (SSL).
     */
    @isString
    public static final String LOGVIEWER_HTTPS_KEY_PASSWORD = "logviewer.https.key.password";

    /**
     * Path to the truststore containing the certs used by Storm Logviewer for HTTPS communications
     */
    @isString
    public static final String LOGVIEWER_HTTPS_TRUSTSTORE_PATH = "logviewer.https.truststore.path";

    /**
     * Password for the truststore for HTTPS for Storm Logviewer
     */
    @isString
    public static final String LOGVIEWER_HTTPS_TRUSTSTORE_PASSWORD = "logviewer.https.truststore.password";

    /**
     * Type of the truststore for HTTPS for Storm Logviewer.
     * see http://docs.oracle.com/javase/8/docs/api/java/security/Truststore.html for more details.
     */
    @isString
    public static final String LOGVIEWER_HTTPS_TRUSTSTORE_TYPE = "logviewer.https.truststore.type";

    /**
     * Password to the truststore used by Storm Logviewer setting up HTTPS (SSL).
     */
    @isBoolean
    public static final String LOGVIEWER_HTTPS_WANT_CLIENT_AUTH = "logviewer.https.want.client.auth";

    @isBoolean
    public static final String LOGVIEWER_HTTPS_NEED_CLIENT_AUTH = "logviewer.https.need.client.auth";

    /**
     * A list of users allowed to view logs via the Log Viewer
     */
    @isStringList
    public static final String LOGS_USERS = "logs.users";

    /**
     * A list of groups allowed to view logs via the Log Viewer
     */
    @isStringList
    public static final String LOGS_GROUPS = "logs.groups";

    /**
     * Appender name used by log viewer to determine log directory.
     */
    @isString
    public static final String LOGVIEWER_APPENDER_NAME = "logviewer.appender.name";

    /**
     * Childopts for Storm UI Java process.
     */
    @isString
    public static final String UI_CHILDOPTS = "ui.childopts";

    /**
     * A class implementing javax.servlet.Filter for authenticating/filtering UI requests
     */
    @isString
    public static final String UI_FILTER = "ui.filter";

    /**
     * Initialization parameters for the javax.servlet.Filter
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String UI_FILTER_PARAMS = "ui.filter.params";

    /**
     * The size of the header buffer for the UI in bytes
     */
    @isInteger
    @isPositiveNumber
    public static final String UI_HEADER_BUFFER_BYTES = "ui.header.buffer.bytes";

    /**
     * This port is used by Storm DRPC for receiving HTTPS (SSL) DPRC requests from clients.
     */
    @isInteger
    @isPositiveNumber
    public static final String UI_HTTPS_PORT = "ui.https.port";

    /**
     * Path to the keystore used by Storm UI for setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_KEYSTORE_PATH = "ui.https.keystore.path";

    /**
     * Password to the keystore used by Storm UI for setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_KEYSTORE_PASSWORD = "ui.https.keystore.password";

    /**
     * Type of keystore used by Storm UI for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String UI_HTTPS_KEYSTORE_TYPE = "ui.https.keystore.type";

    /**
     * Password to the private key in the keystore for setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_KEY_PASSWORD = "ui.https.key.password";

    /**
     * Path to the truststore used by Storm UI setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_TRUSTSTORE_PATH = "ui.https.truststore.path";

    /**
     * Password to the truststore used by Storm UI setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_TRUSTSTORE_PASSWORD = "ui.https.truststore.password";

    /**
     * Type of truststore used by Storm UI for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String UI_HTTPS_TRUSTSTORE_TYPE = "ui.https.truststore.type";

    /**
     * Password to the truststore used by Storm DRPC setting up HTTPS (SSL).
     */
    @isBoolean
    public static final String UI_HTTPS_WANT_CLIENT_AUTH = "ui.https.want.client.auth";

    @isBoolean
    public static final String UI_HTTPS_NEED_CLIENT_AUTH = "ui.https.need.client.auth";

    /**
     * The host that Pacemaker is running on.
     */
    @isString
    public static final String PACEMAKER_HOST = "pacemaker.host";

    /**
     * The port Pacemaker should run on. Clients should
     * connect to this port to submit or read heartbeats.
     */
    @isNumber
    @isPositiveNumber
    public static final String PACEMAKER_PORT = "pacemaker.port";

    /**
     * The maximum number of threads that should be used by the Pacemaker.
     * When Pacemaker gets loaded it will spawn new threads, up to 
     * this many total, to handle the load.
     */
    @isNumber
    @isPositiveNumber
    public static final String PACEMAKER_MAX_THREADS = "pacemaker.max.threads";

    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the pacemaker daemon.
     */
    @isString
    public static final String PACEMAKER_CHILDOPTS = "pacemaker.childopts";

    /**
     * This should be one of "DIGEST", "KERBEROS", or "NONE"
     * Determines the mode of authentication the pacemaker server and client use.
     * The client must either match the server, or be NONE. In the case of NONE,
     * no authentication is performed for the client, and if the server is running with
     * DIGEST or KERBEROS, the client can only write to the server (no reads).
     * This is intended to provide a primitive form of access-control.
     */
    @CustomValidator(validatorClass=PacemakerAuthTypeValidator.class)
    public static final String PACEMAKER_AUTH_METHOD = "pacemaker.auth.method";
    
    /**
     * List of DRPC servers so that the DRPCSpout knows who to talk to.
     */
    @isStringList
    public static final String DRPC_SERVERS = "drpc.servers";

    /**
     * This port is used by Storm DRPC for receiving HTTP DPRC requests from clients.
     */
    @isInteger
    public static final String DRPC_HTTP_PORT = "drpc.http.port";

    /**
     * This port is used by Storm DRPC for receiving HTTPS (SSL) DPRC requests from clients.
     */
    @isInteger
    public static final String DRPC_HTTPS_PORT = "drpc.https.port";

    /**
     * Path to the keystore used by Storm DRPC for setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_KEYSTORE_PATH = "drpc.https.keystore.path";

    /**
     * Password to the keystore used by Storm DRPC for setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_KEYSTORE_PASSWORD = "drpc.https.keystore.password";

    /**
     * Type of keystore used by Storm DRPC for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String DRPC_HTTPS_KEYSTORE_TYPE = "drpc.https.keystore.type";

    /**
     * Password to the private key in the keystore for setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_KEY_PASSWORD = "drpc.https.key.password";

    /**
     * Path to the truststore used by Storm DRPC setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_TRUSTSTORE_PATH = "drpc.https.truststore.path";

    /**
     * Password to the truststore used by Storm DRPC setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_TRUSTSTORE_PASSWORD = "drpc.https.truststore.password";

    /**
     * Type of truststore used by Storm DRPC for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String DRPC_HTTPS_TRUSTSTORE_TYPE = "drpc.https.truststore.type";

    /**
     * Password to the truststore used by Storm DRPC setting up HTTPS (SSL).
     */
    @isBoolean
    public static final String DRPC_HTTPS_WANT_CLIENT_AUTH = "drpc.https.want.client.auth";

    @isBoolean
    public static final String DRPC_HTTPS_NEED_CLIENT_AUTH = "drpc.https.need.client.auth";

    /**
     * The DRPC transport plug-in for Thrift client/server communication
     */
    @isString
    public static final String DRPC_THRIFT_TRANSPORT_PLUGIN = "drpc.thrift.transport";

    /**
     * This port is used by Storm DRPC for receiving DPRC requests from clients.
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_PORT = "drpc.port";

    /**
     * Class name for authorization plugin for DRPC client
     */
    @isString
    public static final String DRPC_AUTHORIZER = "drpc.authorizer";

    /**
     * The Access Control List for the DRPC Authorizer.
     * @see backtype.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @isType(type=Map.class)
    public static final String DRPC_AUTHORIZER_ACL = "drpc.authorizer.acl";

    /**
     * File name of the DRPC Authorizer ACL.
     * @see backtype.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @isString
    public static final String DRPC_AUTHORIZER_ACL_FILENAME = "drpc.authorizer.acl.filename";

    /**
     * Whether the DRPCSimpleAclAuthorizer should deny requests for operations
     * involving functions that have no explicit ACL entry. When set to false
     * (the default) DRPC functions that have no entry in the ACL will be
     * permitted, which is appropriate for a development environment. When set
     * to true, explicit ACL entries are required for every DRPC function, and
     * any request for functions will be denied.
     * @see backtype.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @isBoolean
    public static final String DRPC_AUTHORIZER_ACL_STRICT = "drpc.authorizer.acl.strict";

    /**
     * DRPC thrift server worker threads
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_WORKER_THREADS = "drpc.worker.threads";

    /**
     * The maximum buffer size thrift should use when reading messages for DRPC.
     */
    @isNumber
    @isPositiveNumber
    public static final String DRPC_MAX_BUFFER_SIZE = "drpc.max_buffer_size";

    /**
     * DRPC thrift server queue size
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_QUEUE_SIZE = "drpc.queue.size";

    /**
     * The DRPC invocations transport plug-in for Thrift client/server communication
     */
    @isString
    public static final String DRPC_INVOCATIONS_THRIFT_TRANSPORT_PLUGIN = "drpc.invocations.thrift.transport";

    /**
     * This port on Storm DRPC is used by DRPC topologies to receive function invocations and send results back.
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_INVOCATIONS_PORT = "drpc.invocations.port";

    /**
     * DRPC invocations thrift server worker threads
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_INVOCATIONS_THREADS = "drpc.invocations.threads";

    /**
     * The timeout on DRPC requests within the DRPC server. Defaults to 10 minutes. Note that requests can also
     * timeout based on the socket timeout on the DRPC client, and separately based on the topology message
     * timeout for the topology implementing the DRPC function.
     */

    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String DRPC_REQUEST_TIMEOUT_SECS  = "drpc.request.timeout.secs";

    /**
     * Childopts for Storm DRPC Java process.
     */
    @isString
    public static final String DRPC_CHILDOPTS = "drpc.childopts";

    /**
     * Class name of the HTTP credentials plugin for the UI.
     */
    @isString
    public static final String UI_HTTP_CREDS_PLUGIN = "ui.http.creds.plugin";

    /**
     * Class name of the HTTP credentials plugin for DRPC.
     */
    @isString
    public static final String DRPC_HTTP_CREDS_PLUGIN = "drpc.http.creds.plugin";

    /**
     * the metadata configured on the supervisor
     */
    @isType(type=Map.class)
    public static final String SUPERVISOR_SCHEDULER_META = "supervisor.scheduler.meta";

    /**
     * A list of ports that can run workers on this supervisor. Each worker uses one port, and
     * the supervisor will only run one worker per port. Use this configuration to tune
     * how many workers run on each machine.
     */
    @isNoDuplicateInList
    @NotNull
    @isListEntryCustom(entryValidatorClasses={IntegerValidator.class,PositiveNumberValidator.class})
    public static final String SUPERVISOR_SLOTS_PORTS = "supervisor.slots.ports";

    /**
     * A number representing the maximum number of workers any single topology can acquire.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String NIMBUS_SLOTS_PER_TOPOLOGY = "nimbus.slots.perTopology";

    /**
     * A class implementing javax.servlet.Filter for DRPC HTTP requests
     */
    @isString
    public static final String DRPC_HTTP_FILTER = "drpc.http.filter";

    /**
     * Initialization parameters for the javax.servlet.Filter of the DRPC HTTP
     * service
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String DRPC_HTTP_FILTER_PARAMS = "drpc.http.filter.params";

    /**
     * A number representing the maximum number of executors any single topology can acquire.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String NIMBUS_EXECUTORS_PER_TOPOLOGY = "nimbus.executors.perTopology";

    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the supervisor daemon.
     */
    @isString
    public static final String SUPERVISOR_CHILDOPTS = "supervisor.childopts";

    /**
     * How long a worker can go without heartbeating before the supervisor tries to
     * restart the worker process.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String SUPERVISOR_WORKER_TIMEOUT_SECS = "supervisor.worker.timeout.secs";

    /**
     * How many seconds to sleep for before shutting down threads on worker
     */
    @isInteger
    @isPositiveNumber
    public static final String SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS = "supervisor.worker.shutdown.sleep.secs";

    /**
     * How long a worker can go without heartbeating during the initial launch before
     * the supervisor tries to restart the worker process. This value override
     * supervisor.worker.timeout.secs during launch because there is additional
     * overhead to starting and configuring the JVM on launch.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String SUPERVISOR_WORKER_START_TIMEOUT_SECS = "supervisor.worker.start.timeout.secs";

    /**
     * Whether or not the supervisor should launch workers assigned to it. Defaults
     * to true -- and you should probably never change this value. This configuration
     * is used in the Storm unit tests.
     */
    @isBoolean
    public static final String SUPERVISOR_ENABLE = "supervisor.enable";

    /**
     * how often the supervisor sends a heartbeat to the master.
     */
    @isInteger
    public static final String SUPERVISOR_HEARTBEAT_FREQUENCY_SECS = "supervisor.heartbeat.frequency.secs";


    /**
     * How often the supervisor checks the worker heartbeats to see if any of them
     * need to be restarted.
     */
    @isInteger
    @isPositiveNumber
    public static final String SUPERVISOR_MONITOR_FREQUENCY_SECS = "supervisor.monitor.frequency.secs";

    /**
     * Should the supervior try to run the worker as the lauching user or not.  Defaults to false.
     */
    @isBoolean
    public static final String SUPERVISOR_RUN_WORKER_AS_USER = "supervisor.run.worker.as.user";

    /**
     * Full path to the worker-laucher executable that will be used to lauch workers when
     * SUPERVISOR_RUN_WORKER_AS_USER is set to true.
     */
    @isString
    public static final String SUPERVISOR_WORKER_LAUNCHER = "supervisor.worker.launcher";

    /**
     * The total amount of memory (in MiB) a supervisor is allowed to give to its workers.
     *  A default value will be set for this config if user does not override
     */
    @isPositiveNumber
    public static final String SUPERVISOR_MEMORY_CAPACITY_MB = "supervisor.memory.capacity.mb";

    /**
     * The total amount of CPU resources a supervisor is allowed to give to its workers.
     * By convention 1 cpu core should be about 100, but this can be adjusted if needed
     * using 100 makes it simple to set the desired value to the capacity measurement
     * for single threaded bolts.  A default value will be set for this config if user does not override
     */
    @isPositiveNumber
    public static final String SUPERVISOR_CPU_CAPACITY = "supervisor.cpu.capacity";

    /**
     * The jvm opts provided to workers launched by this supervisor.
     * All "%ID%", "%WORKER-ID%", "%TOPOLOGY-ID%",
     * "%WORKER-PORT%" and "%HEAP-MEM%" substrings are replaced with:
     * %ID%          -> port (for backward compatibility),
     * %WORKER-ID%   -> worker-id,
     * %TOPOLOGY-ID%    -> topology-id,
     * %WORKER-PORT% -> port.
     * %HEAP-MEM% -> mem-onheap.
     */
    @isStringOrStringList
    public static final String WORKER_CHILDOPTS = "worker.childopts";

    /**
     * The default heap memory size in MB per worker, used in the jvm -Xmx opts for launching the worker
      */
    @isInteger
    @isPositiveNumber
    public static final String WORKER_HEAP_MEMORY_MB = "worker.heap.memory.mb";

    /**
     * The jvm profiler opts provided to workers launched by this supervisor.
     */
    @isStringOrStringList
    public static final String WORKER_PROFILER_CHILDOPTS = "worker.profiler.childopts";

    /**
     * This configuration would enable or disable component page profiing and debugging for workers.
     */
    @isBoolean
    public static final String WORKER_PROFILER_ENABLED = "worker.profiler.enabled";

    /**
     * The command launched supervisor with worker arguments
     * pid, action and [target_directory]
     * Where action is - start profile, stop profile, jstack, heapdump and kill against pid
     *
     */
    @isString
    public static final String WORKER_PROFILER_COMMAND = "worker.profiler.command";

    /**
     * The jvm opts provided to workers launched by this supervisor for GC. All "%ID%" substrings are replaced
     * with an identifier for this worker.  Because the JVM complains about multiple GC opts the topology
     * can override this default value by setting topology.worker.gc.childopts.
     */
    @isStringOrStringList
    public static final String WORKER_GC_CHILDOPTS = "worker.gc.childopts";

    /**
     * How often this worker should heartbeat to the supervisor.
     */
    @isInteger
    @isPositiveNumber
    public static final String WORKER_HEARTBEAT_FREQUENCY_SECS = "worker.heartbeat.frequency.secs";

    /**
     * How often a task should heartbeat its status to the master.
     */
    @isInteger
    @isPositiveNumber
    public static final String TASK_HEARTBEAT_FREQUENCY_SECS = "task.heartbeat.frequency.secs";

    /**
     * How often a task should sync its connections with other tasks (if a task is
     * reassigned, the other tasks sending messages to it need to refresh their connections).
     * In general though, when a reassignment happens other tasks will be notified
     * almost immediately. This configuration is here just in case that notification doesn't
     * come through.
     */
    @isInteger
    @isPositiveNumber
    public static final String TASK_REFRESH_POLL_SECS = "task.refresh.poll.secs";

    /**
     * How often a worker should check dynamic log level timeouts for expiration.
     * For expired logger settings, the clean up polling task will reset the log levels
     * to the original levels (detected at startup), and will clean up the timeout map
     */
    @isInteger
    @isPositiveNumber
    public static final String WORKER_LOG_LEVEL_RESET_POLL_SECS = "worker.log.level.reset.poll.secs";

    /**
     * How often a task should sync credentials, worst case.
     */
    @isInteger
    @isPositiveNumber
    public static final String TASK_CREDENTIALS_POLL_SECS = "task.credentials.poll.secs";

    /**
     * Whether to enable backpressure in for a certain topology
     */
    @isBoolean
    public static final String TOPOLOGY_BACKPRESSURE_ENABLE = "topology.backpressure.enable";

    /**
     * This signifies the tuple congestion in a disruptor queue.
     * When the used ratio of a disruptor queue is higher than the high watermark,
     * the backpressure scheme, if enabled, should slow down the tuple sending speed of
     * the spouts until reaching the low watermark.
     */
    @isPositiveNumber
    public static final String BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK="backpressure.disruptor.high.watermark";

    /**
     * This signifies a state that a disruptor queue has left the congestion.
     * If the used ratio of a disruptor queue is lower than the low watermark,
     * it will unset the backpressure flag.
     */
    @isPositiveNumber
    public static final String BACKPRESSURE_DISRUPTOR_LOW_WATERMARK="backpressure.disruptor.low.watermark";

    /**
     * A list of users that are allowed to interact with the topology.  To use this set
     * nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String TOPOLOGY_USERS = "topology.users";

    /**
     * A list of groups that are allowed to interact with the topology.  To use this set
     * nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String TOPOLOGY_GROUPS = "topology.groups";

    /**
     * True if Storm should timeout messages or not. Defaults to true. This is meant to be used
     * in unit tests to prevent tuples from being accidentally timed out during the test.
     */
    @isBoolean
    public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";

    /**
     * When set to true, Storm will log every message that's emitted.
     */
    @isBoolean
    public static final String TOPOLOGY_DEBUG = "topology.debug";

    /**
     * The serializer for communication between shell components and non-JVM
     * processes
     */
    @isString
    public static final String TOPOLOGY_MULTILANG_SERIALIZER = "topology.multilang.serializer";

    /**
     * How many processes should be spawned around the cluster to execute this
     * topology. Each process will execute some number of tasks as threads within
     * them. This parameter should be used in conjunction with the parallelism hints
     * on each component in the topology to tune the performance of a topology.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_WORKERS = "topology.workers";

    /**
     * How many instances to create for a spout/bolt. A task runs on a thread with zero or more
     * other tasks for the same spout/bolt. The number of tasks for a spout/bolt is always
     * the same throughout the lifetime of a topology, but the number of executors (threads) for
     * a spout/bolt can change over time. This allows a topology to scale to more or less resources
     * without redeploying the topology or violating the constraints of Storm (such as a fields grouping
     * guaranteeing that the same value goes to the same task).
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_TASKS = "topology.tasks";

    /**
     * The maximum amount of memory an instance of a spout/bolt will take on heap. This enables the scheduler
     * to allocate slots on machines with enough available memory. A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB = "topology.component.resources.onheap.memory.mb";

    /**
     * The maximum amount of memory an instance of a spout/bolt will take off heap. This enables the scheduler
     * to allocate slots on machines with enough available memory.  A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB = "topology.component.resources.offheap.memory.mb";

    /**
     * The config indicates the percentage of cpu for a core an instance(executor) of a component will use.
     * Assuming the a core value to be 100, a value of 10 indicates 10% of the core.
     * The P in PCORE represents the term "physical".  A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT = "topology.component.cpu.pcore.percent";

    /**
     * A per topology config that specifies the maximum amount of memory a worker can use for that specific topology
     */
    @isPositiveNumber
    public static final String TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB = "topology.worker.max.heap.size.mb";

    /**
     * How many executors to spawn for ackers.
     *
     * <p>By not setting this variable or setting it as null, Storm will set the number of acker executors
     * to be equal to the number of workers configured for this topology. If this variable is set to 0,
     * then Storm will immediately ack tuples as soon as they come off the spout, effectively disabling reliability.</p>
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_EXECUTORS = "topology.acker.executors";

    /**
     * How many executors to spawn for event logger.
     *
     * <p>By not setting this variable or setting it as null, Storm will set the number of eventlogger executors
     * to be equal to the number of workers configured for this topology. If this variable is set to 0,
     * event logging will be disabled.</p>
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_EVENTLOGGER_EXECUTORS = "topology.eventlogger.executors";

    /**
     * The maximum amount of time given to the topology to fully process a message
     * emitted by a spout. If the message is not acked within this time frame, Storm
     * will fail the message on the spout. Some spouts implementations will then replay
     * the message at a later time.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";

    /**
     * A list of serialization registrations for Kryo ( http://code.google.com/p/kryo/ ),
     * the underlying serialization framework for Storm. A serialization can either
     * be the name of a class (in which case Kryo will automatically create a serializer for the class
     * that saves all the object's fields), or an implementation of com.esotericsoftware.kryo.Serializer.
     *
     * See Kryo's documentation for more information about writing custom serializers.
     */
    @isKryoReg
    public static final String TOPOLOGY_KRYO_REGISTER = "topology.kryo.register";

    /**
     * A list of classes that customize storm's kryo instance during start-up.
     * Each listed class name must implement IKryoDecorator. During start-up the
     * listed class is instantiated with 0 arguments, then its 'decorate' method
     * is called with storm's kryo instance as the only argument.
     */
    @isStringList
    public static final String TOPOLOGY_KRYO_DECORATORS = "topology.kryo.decorators";

    /**
     * Class that specifies how to create a Kryo instance for serialization. Storm will then apply
     * topology.kryo.register and topology.kryo.decorators on top of this. The default implementation
     * implements topology.fall.back.on.java.serialization and turns references off.
     */
    @isString
    public static final String TOPOLOGY_KRYO_FACTORY = "topology.kryo.factory";

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
    @isBoolean
    public static final String TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS= "topology.skip.missing.kryo.registrations";

    /**
     * A list of classes implementing IMetricsConsumer (See storm.yaml.example for exact config format).
     * Each listed class will be routed all the metrics data generated by the storm metrics API.
     * Each listed class maps 1:1 to a system bolt named __metrics_ClassName#N, and it's parallelism is configurable.
     */

    @isListEntryCustom(entryValidatorClasses={MetricRegistryValidator.class})
    public static final String TOPOLOGY_METRICS_CONSUMER_REGISTER = "topology.metrics.consumer.register";

    /**
     * A map of metric name to class name implementing IMetric that will be created once per worker JVM
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String TOPOLOGY_WORKER_METRICS = "topology.worker.metrics";

    /**
     * A map of metric name to class name implementing IMetric that will be created once per worker JVM
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String WORKER_METRICS = "worker.metrics";

    /**
     * The maximum parallelism allowed for a component in this topology. This configuration is
     * typically used in testing to limit the number of threads spawned in local mode.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_MAX_TASK_PARALLELISM="topology.max.task.parallelism";

    /**
     * The maximum number of tuples that can be pending on a spout task at any given time.
     * This config applies to individual tasks, not to spouts or topologies as a whole.
     *
     * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
     * Note that this config parameter has no effect for unreliable spouts that don't tag
     * their tuples with a message id.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_MAX_SPOUT_PENDING="topology.max.spout.pending";

    /**
     * A class that implements a strategy for what to do when a spout needs to wait. Waiting is
     * triggered in one of two conditions:
     *
     * 1. nextTuple emits no tuples
     * 2. The spout has hit maxSpoutPending and can't emit any more tuples
     */
    @isString
    public static final String TOPOLOGY_SPOUT_WAIT_STRATEGY="topology.spout.wait.strategy";

    /**
     * The amount of milliseconds the SleepEmptyEmitStrategy should sleep for.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS="topology.sleep.spout.wait.strategy.time.ms";

    /**
     * The maximum amount of time a component gives a source of state to synchronize before it requests
     * synchronization again.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS="topology.state.synchronization.timeout.secs";

    /**
     * The percentage of tuples to sample to produce stats for a task.
     */
    @isPositiveNumber
    public static final String TOPOLOGY_STATS_SAMPLE_RATE="topology.stats.sample.rate";

    /**
     * The time period that builtin metrics data in bucketed into.
     */
    @isInteger
    public static final String TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS="topology.builtin.metrics.bucket.size.secs";

    /**
     * Whether or not to use Java serialization in a topology.
     */
    @isBoolean
    public static final String TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION="topology.fall.back.on.java.serialization";

    /**
     * Topology-specific options for the worker child process. This is used in addition to WORKER_CHILDOPTS.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_WORKER_CHILDOPTS="topology.worker.childopts";

    /**
     * Topology-specific options GC for the worker child process. This overrides WORKER_GC_CHILDOPTS.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_WORKER_GC_CHILDOPTS="topology.worker.gc.childopts";

    /**
     * Topology-specific options for the logwriter process of a worker.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS="topology.worker.logwriter.childopts";

    /**
     * Topology-specific classpath for the worker child process. This is combined to the usual classpath.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_CLASSPATH="topology.classpath";

    /**
     * Topology-specific environment variables for the worker child process.
     * This is added to the existing environment (that of the supervisor)
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String TOPOLOGY_ENVIRONMENT="topology.environment";

    /*
     * Topology-specific option to disable/enable bolt's outgoing overflow buffer.
     * Enabling this option ensures that the bolt can always clear the incoming messages,
     * preventing live-lock for the topology with cyclic flow.
     * The overflow buffer can fill degrading the performance gradually,
     * eventually running out of memory.
     */
    @isBoolean
    public static final String TOPOLOGY_BOLTS_OUTGOING_OVERFLOW_BUFFER_ENABLE="topology.bolts.outgoing.overflow.buffer.enable";

    /*
     * Bolt-specific configuration for windowed bolts to specify the window length as a count of number of tuples
     * in the window.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT = "topology.bolts.window.length.count";

    /*
     * Bolt-specific configuration for windowed bolts to specify the window length in time duration.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS = "topology.bolts.window.length.duration.ms";

    /*
     * Bolt-specific configuration for windowed bolts to specifiy the sliding interval as a count of number of tuples.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT = "topology.bolts.window.sliding.interval.count";

    /*
     * Bolt-specific configuration for windowed bolts to specifiy the sliding interval in time duration.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS = "topology.bolts.window.sliding.interval.duration.ms";

    /**
     * This config is available for TransactionalSpouts, and contains the id ( a String) for
     * the transactional topology. This id is used to store the state of the transactional
     * topology in Zookeeper.
     */
    @isString
    public static final String TOPOLOGY_TRANSACTIONAL_ID="topology.transactional.id";

    /**
     * A list of task hooks that are automatically added to every spout and bolt in the topology. An example
     * of when you'd do this is to add a hook that integrates with your internal
     * monitoring system. These hooks are instantiated using the zero-arg constructor.
     */
    @isStringList
    public static final String TOPOLOGY_AUTO_TASK_HOOKS="topology.auto.task.hooks";

    /**
     * The size of the Disruptor receive queue for each executor. Must be a power of 2.
     */
    @isPowerOf2
    public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE="topology.executor.receive.buffer.size";

    /**
     * The size of the Disruptor send queue for each executor. Must be a power of 2.
     */
    @isPowerOf2
    public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE="topology.executor.send.buffer.size";

    /**
     * The size of the Disruptor transfer queue for each worker.
     */
    @isInteger
    @isPowerOf2
    public static final String TOPOLOGY_TRANSFER_BUFFER_SIZE="topology.transfer.buffer.size";

    /**
     * How often a tick tuple from the "__system" component and "__tick" stream should be sent
     * to tasks. Meant to be used as a component-specific configuration.
     */
    @isInteger
    public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS="topology.tick.tuple.freq.secs";

   /**
    * @deprecated this is no longer supported
    * Configure the wait strategy used for internal queuing. Can be used to tradeoff latency
    * vs. throughput
    */
    @Deprecated
    @isString
    public static final String TOPOLOGY_DISRUPTOR_WAIT_STRATEGY="topology.disruptor.wait.strategy";

    /**
     * The size of the shared thread pool for worker tasks to make use of. The thread pool can be accessed
     * via the TopologyContext.
     */
    @isInteger
    public static final String TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE="topology.worker.shared.thread.pool.size";

    /**
     * The interval in seconds to use for determining whether to throttle error reported to Zookeeper. For example,
     * an interval of 10 seconds with topology.max.error.report.per.interval set to 5 will only allow 5 errors to be
     * reported to Zookeeper per task for every 10 second interval of time.
     */
    @isInteger
    public static final String TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS="topology.error.throttle.interval.secs";

    /**
     * See doc for TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL="topology.max.error.report.per.interval";

    /**
     * How often a batch can be emitted in a Trident topology.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS="topology.trident.batch.emit.interval.millis";

    /**
     * Name of the topology. This config is automatically set by Storm when the topology is submitted.
     */
    @isString
    public final static String TOPOLOGY_NAME="topology.name";

    /**
     * The principal who submitted a topology
     */
    @isString
    public final static String TOPOLOGY_SUBMITTER_PRINCIPAL = "topology.submitter.principal";

    /**
     * The local user name of the user who submitted a topology.
     */
    @isString
    public static final String TOPOLOGY_SUBMITTER_USER = "topology.submitter.user";

    /**
     * Array of components that scheduler should try to place on separate hosts.
     */
    @isStringList
    public static final String TOPOLOGY_SPREAD_COMPONENTS = "topology.spread.components";

    /**
     * A list of IAutoCredentials that the topology should load and use.
     */
    @isStringList
    public static final String TOPOLOGY_AUTO_CREDENTIALS = "topology.auto-credentials";

    /**
     * Max pending tuples in one ShellBolt
     */
    @NotNull
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_SHELLBOLT_MAX_PENDING="topology.shellbolt.max.pending";

    /**
     * How long a subprocess can go without heartbeating before the ShellSpout/ShellBolt tries to
     * suicide itself.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_SUBPROCESS_TIMEOUT_SECS = "topology.subprocess.timeout.secs";

    /**
     * Topology central logging sensitivity to determine who has access to logs in central logging system.
     * The possible values are:
     *   S0 - Public (open to all users on grid)
     *   S1 - Restricted
     *   S2 - Confidential
     *   S3 - Secret (default.)
     */
    @isString(acceptedValues = {"S0", "S1", "S2", "S3"})
    public static final String TOPOLOGY_LOGGING_SENSITIVITY="topology.logging.sensitivity";

    /**
     * The root directory in ZooKeeper for metadata about TransactionalSpouts.
     */
    @isString
    public static final String TRANSACTIONAL_ZOOKEEPER_ROOT="transactional.zookeeper.root";

    /**
     * The list of zookeeper servers in which to keep the transactional state. If null (which is default),
     * will use storm.zookeeper.servers
     */
    @isStringList
    public static final String TRANSACTIONAL_ZOOKEEPER_SERVERS="transactional.zookeeper.servers";

    /**
     * The port to use to connect to the transactional zookeeper servers. If null (which is default),
     * will use storm.zookeeper.port
     */
    @isInteger
    @isPositiveNumber
    public static final String TRANSACTIONAL_ZOOKEEPER_PORT="transactional.zookeeper.port";

    /**
     * The user as which the nimbus client should be acquired to perform the operation.
     */
    @isString
    public static final String STORM_DO_AS_USER="storm.doAsUser";

    /**
     * The number of threads that should be used by the zeromq context in each worker process.
     */
    @Deprecated
    @isInteger
    public static final String ZMQ_THREADS = "zmq.threads";

    /**
     * How long a connection should retry sending messages to a target host when
     * the connection is closed. This is an advanced configuration and can almost
     * certainly be ignored.
     */
    @Deprecated
    @isInteger
    public static final String ZMQ_LINGER_MILLIS = "zmq.linger.millis";

    /**
     * The high water for the ZeroMQ push sockets used for networking. Use this config to prevent buffer explosion
     * on the networking layer.
     */
    @Deprecated
    @isInteger
    public static final String ZMQ_HWM = "zmq.hwm";

    /**
     * This value is passed to spawned JVMs (e.g., Nimbus, Supervisor, and Workers)
     * for the java.library.path value. java.library.path tells the JVM where
     * to look for native libraries. It is necessary to set this config correctly since
     * Storm uses the ZeroMQ and JZMQ native libs.
     */
    @isString
    public static final String JAVA_LIBRARY_PATH = "java.library.path";

    /**
     * The path to use as the zookeeper dir when running a zookeeper server via
     * "storm dev-zookeeper". This zookeeper instance is only intended for development;
     * it is not a production grade zookeeper setup.
     */
    @isString
    public static final String DEV_ZOOKEEPER_PATH = "dev.zookeeper.path";

    /**
     * A map from topology name to the number of machines that should be dedicated for that topology. Set storm.scheduler
     * to backtype.storm.scheduler.IsolationScheduler to make use of the isolation scheduler.
     */
    @isMapEntryType(keyType = String.class, valueType = Number.class)
    public static final String ISOLATION_SCHEDULER_MACHINES = "isolation.scheduler.machines";

    /**
     * A map from the user name to the number of machines that should that user is allowed to use. Set storm.scheduler
     * to backtype.storm.scheduler.multitenant.MultitenantScheduler
     */
    @isMapEntryType(keyType = String.class, valueType = Number.class)
    public static final String MULTITENANT_SCHEDULER_USER_POOLS = "multitenant.scheduler.user.pools";

    /**
     * The number of machines that should be used by this topology to isolate it from all others. Set storm.scheduler
     * to backtype.storm.scheduler.multitenant.MultitenantScheduler
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_ISOLATED_MACHINES = "topology.isolate.machines";

    /**
     * Configure timeout milliseconds used for disruptor queue wait strategy. Can be used to tradeoff latency
     * vs. CPU usage
     */
    @isInteger
    @NotNull
    public static final String TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS="topology.disruptor.wait.timeout.millis";

    /**
     * The number of tuples to batch before sending to the next thread.  This number is just an initial suggestion and
     * the code may adjust it as your topology runs.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String TOPOLOGY_DISRUPTOR_BATCH_SIZE="topology.disruptor.batch.size";

    /**
     * The maximum age in milliseconds a batch can be before being sent to the next thread.  This number is just an
     * initial suggestion and the code may adjust it as your topology runs.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS="topology.disruptor.batch.timeout.millis";

    /**
     * Which implementation of {@link backtype.storm.codedistributor.ICodeDistributor} should be used by storm for code
     * distribution.
     */
    @isString
    public static final String STORM_CODE_DISTRIBUTOR_CLASS = "storm.codedistributor.class";

    /**
     * Minimum number of nimbus hosts where the code must be replicated before leader nimbus
     * is allowed to perform topology activation tasks like setting up heartbeats/assignments
     * and marking the topology as active. default is 0.
     */
    @isNumber
    public static final String TOPOLOGY_MIN_REPLICATION_COUNT = "topology.min.replication.count";

    /**
     * Maximum wait time for the nimbus host replication to achieve the nimbus.min.replication.count.
     * Once this time is elapsed nimbus will go ahead and perform topology activation tasks even
     * if required nimbus.min.replication.count is not achieved. The default is 0 seconds, a value of
     * -1 indicates to wait for ever.
     */
    @isNumber
    public static final String TOPOLOGY_MAX_REPLICATION_WAIT_TIME_SEC = "topology.max.replication.wait.time.sec";

    /**
     * How often nimbus's background thread to sync code for missing topologies should run.
     */
    @isInteger
    public static final String NIMBUS_CODE_SYNC_FREQ_SECS = "nimbus.code.sync.freq.secs";

    public static void setClasspath(Map conf, String cp) {
        conf.put(Config.TOPOLOGY_CLASSPATH, cp);
    }

    public void setClasspath(String cp) {
        setClasspath(this, cp);
    }

    public static void setEnvironment(Map conf, Map env) {
        conf.put(Config.TOPOLOGY_ENVIRONMENT, env);
    }

    public void setEnvironment(Map env) {
        setEnvironment(this, env);
    }

    public static void setDebug(Map conf, boolean isOn) {
        conf.put(Config.TOPOLOGY_DEBUG, isOn);
    }

    public void setDebug(boolean isOn) {
        setDebug(this, isOn);
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

    public static void setNumEventLoggers(Map conf, int numExecutors) {
        conf.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, numExecutors);
    }

    public void setNumEventLoggers(int numExecutors) {
        setNumEventLoggers(this, numExecutors);
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

    public static void registerMetricsConsumer(Map conf, Class klass, Object argument, long parallelismHint) {
        HashMap m = new HashMap();
        m.put("class", klass.getCanonicalName());
        m.put("parallelism.hint", parallelismHint);
        m.put("argument", argument);

        List l = (List)conf.get(TOPOLOGY_METRICS_CONSUMER_REGISTER);
        if (l == null) { l = new ArrayList(); }
        l.add(m);
        conf.put(TOPOLOGY_METRICS_CONSUMER_REGISTER, l);
    }

    public void registerMetricsConsumer(Class klass, Object argument, long parallelismHint) {
        registerMetricsConsumer(this, klass, argument, parallelismHint);
    }

    public static void registerMetricsConsumer(Map conf, Class klass, long parallelismHint) {
        registerMetricsConsumer(conf, klass, null, parallelismHint);
    }

    public void registerMetricsConsumer(Class klass, long parallelismHint) {
        registerMetricsConsumer(this, klass, parallelismHint);
    }

    public static void registerMetricsConsumer(Map conf, Class klass) {
        registerMetricsConsumer(conf, klass, null, 1L);
    }

    public void registerMetricsConsumer(Class klass) {
        registerMetricsConsumer(this, klass);
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

    /**
     * set the max heap size allow per worker for this topology
     * @param size
     */
    public void setTopologyWorkerMaxHeapSize(Number size) {
        if(size != null) {
            this.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, size);
        }
    }
}
