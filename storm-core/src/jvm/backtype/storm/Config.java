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
 * <p>This class also provides constants for all the configurations possible on
 * a Storm cluster and Storm topology. Each constant is paired with a schema
 * that defines the validity criterion of the corresponding field. Default
 * values for these configs can be found in defaults.yaml.</p>
 *
 * <p>Note that you may put other configurations in any of the configs. Storm
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of
 * Spouts.</p>
 */
public class Config extends HashMap<String, Object> {
    /**
     * This is part of a temporary workaround to a ZK bug, it is the 'scheme:acl' for
     * the user Nimbus and Supervisors use to authenticate with ZK.
     */
    public static final String STORM_ZOOKEEPER_SUPERACL = "storm.zookeeper.superACL";
    public static final Object STORM_ZOOKEEPER_SUPERACL_SCHEMA = String.class;

    /**
     * The transporter for communication among Storm tasks
     */
    public static final String STORM_MESSAGING_TRANSPORT = "storm.messaging.transport";
    public static final Object STORM_MESSAGING_TRANSPORT_SCHEMA = String.class;

    /**
     * Netty based messaging: The buffer size for send/recv buffer
     */
    public static final String STORM_MESSAGING_NETTY_BUFFER_SIZE = "storm.messaging.netty.buffer_size";
    public static final Object STORM_MESSAGING_NETTY_BUFFER_SIZE_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Netty based messaging: The max # of retries that a peer will perform when a remote is not accessible
     */
    public static final String STORM_MESSAGING_NETTY_MAX_RETRIES = "storm.messaging.netty.max_retries";
    public static final Object STORM_MESSAGING_NETTY_MAX_RETRIES_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Netty based messaging: The min # of milliseconds that a peer will wait.
     */
    public static final String STORM_MESSAGING_NETTY_MIN_SLEEP_MS = "storm.messaging.netty.min_wait_ms";
    public static final Object STORM_MESSAGING_NETTY_MIN_SLEEP_MS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Netty based messaging: The max # of milliseconds that a peer will wait.
     */
    public static final String STORM_MESSAGING_NETTY_MAX_SLEEP_MS = "storm.messaging.netty.max_wait_ms";
    public static final Object STORM_MESSAGING_NETTY_MAX_SLEEP_MS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Netty based messaging: The # of worker threads for the server.
     */
    public static final String STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS = "storm.messaging.netty.server_worker_threads";
    public static final Object STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Netty based messaging: The # of worker threads for the client.
     */
    public static final String STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS = "storm.messaging.netty.client_worker_threads";
    public static final Object STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * If the Netty messaging layer is busy, the Netty client will try to batch message as more as possible up to the size of STORM_NETTY_MESSAGE_BATCH_SIZE bytes
     */
    public static final String STORM_NETTY_MESSAGE_BATCH_SIZE = "storm.messaging.netty.transfer.batch.size";
    public static final Object STORM_NETTY_MESSAGE_BATCH_SIZE_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * We check with this interval that whether the Netty channel is writable and try to write pending messages
     */
    public static final String STORM_NETTY_FLUSH_CHECK_INTERVAL_MS = "storm.messaging.netty.flush.check.interval.ms";
    public static final Object STORM_NETTY_FLUSH_CHECK_INTERVAL_MS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Netty based messaging: Is authentication required for Netty messaging from client worker process to server worker process.
     */
    public static final String STORM_MESSAGING_NETTY_AUTHENTICATION = "storm.messaging.netty.authentication";
    public static final Object STORM_MESSAGING_NETTY_AUTHENTICATION_SCHEMA = Boolean.class;

    /**
     * The delegate for serializing metadata, should be used for serialized objects stored in zookeeper and on disk.
     * This is NOT used for compressing serialized tuples sent between topologies.
     */
    public static final String STORM_META_SERIALIZATION_DELEGATE = "storm.meta.serialization.delegate";
    public static final Object STORM_META_SERIALIZATION_DELEGATE_SCHEMA = String.class;

    /**
     * A list of hosts of ZooKeeper servers used to manage the cluster.
     */
    public static final String STORM_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";
    public static final Object STORM_ZOOKEEPER_SERVERS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * The port Storm will use to connect to each of the ZooKeeper servers.
     */
    public static final String STORM_ZOOKEEPER_PORT = "storm.zookeeper.port";
    public static final Object STORM_ZOOKEEPER_PORT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * A directory on the local filesystem used by Storm for any local
     * filesystem usage it needs. The directory must exist and the Storm daemons must
     * have permission to read/write from this location.
     */
    public static final String STORM_LOCAL_DIR = "storm.local.dir";
    public static final Object STORM_LOCAL_DIR_SCHEMA = String.class;

    /**
     * A global task scheduler used to assign topologies's tasks to supervisors' wokers.
     *
     * If this is not set, a default system scheduler will be used.
     */
    public static final String STORM_SCHEDULER = "storm.scheduler";
    public static final Object STORM_SCHEDULER_SCHEMA = String.class;

    /**
     * The mode this Storm cluster is running in. Either "distributed" or "local".
     */
    public static final String STORM_CLUSTER_MODE = "storm.cluster.mode";
    public static final Object STORM_CLUSTER_MODE_SCHEMA = String.class;

    /**
     * The hostname the supervisors/workers should report to nimbus. If unset, Storm will
     * get the hostname to report by calling <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     *
     * You should set this config when you dont have a DNS which supervisors/workers
     * can utilize to find each other based on hostname got from calls to
     * <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     */
    public static final String STORM_LOCAL_HOSTNAME = "storm.local.hostname";
    public static final Object STORM_LOCAL_HOSTNAME_SCHEMA = String.class;

    /**
     * The plugin that will convert a principal to a local user.
     */
    public static final String STORM_PRINCIPAL_TO_LOCAL_PLUGIN = "storm.principal.tolocal";
    public static final Object STORM_PRINCIPAL_TO_LOCAL_PLUGIN_SCHEMA = String.class;

    /**
     * The plugin that will provide user groups service
     */
    public static final String STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN = "storm.group.mapping.service";
    public static final Object STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN_SCHEMA = String.class;

    /**
     * Max no.of seconds group mapping service will cache user groups
     */
    public static final String STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS = "storm.group.mapping.service.cache.duration.secs";
    public static final Object STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS_SCHEMA = Number.class;

    /**
     * The default transport plug-in for Thrift client/server communication
     */
    public static final String STORM_THRIFT_TRANSPORT_PLUGIN = "storm.thrift.transport";
    public static final Object STORM_THRIFT_TRANSPORT_PLUGIN_SCHEMA = String.class;

    /**
     * The serializer class for ListDelegate (tuple payload).
     * The default serializer will be ListDelegateSerializer
     */
    public static final String TOPOLOGY_TUPLE_SERIALIZER = "topology.tuple.serializer";
    public static final Object TOPOLOGY_TUPLE_SERIALIZER_SCHEMA = String.class;

    /**
     * Try to serialize all tuples, even for local transfers.  This should only be used
     * for testing, as a sanity check that all of your tuples are setup properly.
     */
    public static final String TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE = "topology.testing.always.try.serialize";
    public static final Object TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE_SCHEMA = Boolean.class;

    /**
     * Whether or not to use ZeroMQ for messaging in local mode. If this is set
     * to false, then Storm will use a pure-Java messaging system. The purpose
     * of this flag is to make it easy to run Storm in local mode by eliminating
     * the need for native dependencies, which can be difficult to install.
     *
     * Defaults to false.
     */
    public static final String STORM_LOCAL_MODE_ZMQ = "storm.local.mode.zmq";
    public static final Object STORM_LOCAL_MODE_ZMQ_SCHEMA = Boolean.class;

    /**
     * The root location at which Storm stores data in ZooKeeper.
     */
    public static final String STORM_ZOOKEEPER_ROOT = "storm.zookeeper.root";
    public static final Object STORM_ZOOKEEPER_ROOT_SCHEMA = String.class;

    /**
     * The session timeout for clients to ZooKeeper.
     */
    public static final String STORM_ZOOKEEPER_SESSION_TIMEOUT = "storm.zookeeper.session.timeout";
    public static final Object STORM_ZOOKEEPER_SESSION_TIMEOUT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The connection timeout for clients to ZooKeeper.
     */
    public static final String STORM_ZOOKEEPER_CONNECTION_TIMEOUT = "storm.zookeeper.connection.timeout";
    public static final Object STORM_ZOOKEEPER_CONNECTION_TIMEOUT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The number of times to retry a Zookeeper operation.
     */
    public static final String STORM_ZOOKEEPER_RETRY_TIMES="storm.zookeeper.retry.times";
    public static final Object STORM_ZOOKEEPER_RETRY_TIMES_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The interval between retries of a Zookeeper operation.
     */
    public static final String STORM_ZOOKEEPER_RETRY_INTERVAL="storm.zookeeper.retry.interval";
    public static final Object STORM_ZOOKEEPER_RETRY_INTERVAL_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The ceiling of the interval between retries of a Zookeeper operation.
     */
    public static final String STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING="storm.zookeeper.retry.intervalceiling.millis";
    public static final Object STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The cluster Zookeeper authentication scheme to use, e.g. "digest". Defaults to no authentication.
     */
    public static final String STORM_ZOOKEEPER_AUTH_SCHEME="storm.zookeeper.auth.scheme";
    public static final Object STORM_ZOOKEEPER_AUTH_SCHEME_SCHEMA = String.class;

    /**
     * A string representing the payload for cluster Zookeeper authentication.
     * It gets serialized using UTF-8 encoding during authentication.
     * Note that if this is set to something with a secret (as when using
     * digest authentication) then it should only be set in the
     * storm-cluster-auth.yaml file.
     * This file storm-cluster-auth.yaml should then be protected with
     * appropriate permissions that deny access from workers.
     */
    public static final String STORM_ZOOKEEPER_AUTH_PAYLOAD="storm.zookeeper.auth.payload";
    public static final Object STORM_ZOOKEEPER_AUTH_PAYLOAD_SCHEMA = String.class;

    /**
     * The topology Zookeeper authentication scheme to use, e.g. "digest". Defaults to no authentication.
     */
    public static final String STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME="storm.zookeeper.topology.auth.scheme";
    public static final Object STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME_SCHEMA = String.class;

    /**
     * A string representing the payload for topology Zookeeper authentication. It gets serialized using UTF-8 encoding during authentication.
     */
    public static final String STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD="storm.zookeeper.topology.auth.payload";
    public static final Object STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD_SCHEMA = String.class;

    /**
     * The id assigned to a running topology. The id is the storm name with a unique nonce appended.
     */
    public static final String STORM_ID = "storm.id";
    public static final Object STORM_ID_SCHEMA = String.class;

    /**
     * The number of times to retry a Nimbus operation.
     */
    public static final String STORM_NIMBUS_RETRY_TIMES="storm.nimbus.retry.times";
    public static final Object STORM_NIMBUS_RETRY_TIMES_SCHEMA = Number.class;

    /**
     * The starting interval between exponential backoff retries of a Nimbus operation.
     */
    public static final String STORM_NIMBUS_RETRY_INTERVAL="storm.nimbus.retry.interval.millis";
    public static final Object STORM_NIMBUS_RETRY_INTERVAL_SCHEMA = Number.class;

    /**
     * The ceiling of the interval between retries of a client connect to Nimbus operation.
     */
    public static final String STORM_NIMBUS_RETRY_INTERVAL_CEILING="storm.nimbus.retry.intervalceiling.millis";
    public static final Object STORM_NIMBUS_RETRY_INTERVAL_CEILING_SCHEMA = Number.class;

    /**
     * The host that the master server is running on.
     */
    public static final String NIMBUS_HOST = "nimbus.host";
    public static final Object NIMBUS_HOST_SCHEMA = String.class;

    /**
     * The Nimbus transport plug-in for Thrift client/server communication
     */
    public static final String NIMBUS_THRIFT_TRANSPORT_PLUGIN = "nimbus.thrift.transport";
    public static final Object NIMBUS_THRIFT_TRANSPORT_PLUGIN_SCHEMA = String.class;

    /**
     * Which port the Thrift interface of Nimbus should run on. Clients should
     * connect to this port to upload jars and submit topologies.
     */
    public static final String NIMBUS_THRIFT_PORT = "nimbus.thrift.port";
    public static final Object NIMBUS_THRIFT_PORT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The number of threads that should be used by the nimbus thrift server.
     */
    public static final String NIMBUS_THRIFT_THREADS = "nimbus.thrift.threads";
    public static final Object NIMBUS_THRIFT_THREADS_SCHEMA = Number.class;

    /**
     * A list of users that are cluster admins and can run any command.  To use this set
     * nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    public static final String NIMBUS_ADMINS = "nimbus.admins";
    public static final Object NIMBUS_ADMINS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * A list of users that run the supervisors and should be authorized to interact with
     * nimbus as a supervisor would.  To use this set
     * nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    public static final String NIMBUS_SUPERVISOR_USERS = "nimbus.supervisor.users";
    public static final Object NIMBUS_SUPERVISOR_USERS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * The maximum buffer size thrift should use when reading messages.
     */
    public static final String NIMBUS_THRIFT_MAX_BUFFER_SIZE = "nimbus.thrift.max_buffer_size";
    public static final Object NIMBUS_THRIFT_MAX_BUFFER_SIZE_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the nimbus daemon.
     */
    public static final String NIMBUS_CHILDOPTS = "nimbus.childopts";
    public static final Object NIMBUS_CHILDOPTS_SCHEMA = String.class;


    /**
     * How long without heartbeating a task can go before nimbus will consider the
     * task dead and reassign it to another location.
     */
    public static final String NIMBUS_TASK_TIMEOUT_SECS = "nimbus.task.timeout.secs";
    public static final Object NIMBUS_TASK_TIMEOUT_SECS_SCHEMA = ConfigValidation.IntegerValidator;


    /**
     * How often nimbus should wake up to check heartbeats and do reassignments. Note
     * that if a machine ever goes down Nimbus will immediately wake up and take action.
     * This parameter is for checking for failures when there's no explicit event like that
     * occuring.
     */
    public static final String NIMBUS_MONITOR_FREQ_SECS = "nimbus.monitor.freq.secs";
    public static final Object NIMBUS_MONITOR_FREQ_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * How often nimbus should wake the cleanup thread to clean the inbox.
     * @see NIMBUS_INBOX_JAR_EXPIRATION_SECS
     */
    public static final String NIMBUS_CLEANUP_INBOX_FREQ_SECS = "nimbus.cleanup.inbox.freq.secs";
    public static final Object NIMBUS_CLEANUP_INBOX_FREQ_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The length of time a jar file lives in the inbox before being deleted by the cleanup thread.
     *
     * Probably keep this value greater than or equal to NIMBUS_CLEANUP_INBOX_JAR_EXPIRATION_SECS.
     * Note that the time it takes to delete an inbox jar file is going to be somewhat more than
     * NIMBUS_CLEANUP_INBOX_JAR_EXPIRATION_SECS (depending on how often NIMBUS_CLEANUP_FREQ_SECS
     * is set to).
     * @see NIMBUS_CLEANUP_FREQ_SECS
     */
    public static final String NIMBUS_INBOX_JAR_EXPIRATION_SECS = "nimbus.inbox.jar.expiration.secs";
    public static final Object NIMBUS_INBOX_JAR_EXPIRATION_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * How long before a supervisor can go without heartbeating before nimbus considers it dead
     * and stops assigning new work to it.
     */
    public static final String NIMBUS_SUPERVISOR_TIMEOUT_SECS = "nimbus.supervisor.timeout.secs";
    public static final Object NIMBUS_SUPERVISOR_TIMEOUT_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * A special timeout used when a task is initially launched. During launch, this is the timeout
     * used until the first heartbeat, overriding nimbus.task.timeout.secs.
     *
     * <p>A separate timeout exists for launch because there can be quite a bit of overhead
     * to launching new JVM's and configuring them.</p>
     */
    public static final String NIMBUS_TASK_LAUNCH_SECS = "nimbus.task.launch.secs";
    public static final Object NIMBUS_TASK_LAUNCH_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Whether or not nimbus should reassign tasks if it detects that a task goes down.
     * Defaults to true, and it's not recommended to change this value.
     */
    public static final String NIMBUS_REASSIGN = "nimbus.reassign";
    public static final Object NIMBUS_REASSIGN_SCHEMA = Boolean.class;

    /**
     * During upload/download with the master, how long an upload or download connection is idle
     * before nimbus considers it dead and drops the connection.
     */
    public static final String NIMBUS_FILE_COPY_EXPIRATION_SECS = "nimbus.file.copy.expiration.secs";
    public static final Object NIMBUS_FILE_COPY_EXPIRATION_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * A custom class that implements ITopologyValidator that is run whenever a
     * topology is submitted. Can be used to provide business-specific logic for
     * whether topologies are allowed to run or not.
     */
    public static final String NIMBUS_TOPOLOGY_VALIDATOR = "nimbus.topology.validator";
    public static final Object NIMBUS_TOPOLOGY_VALIDATOR_SCHEMA = String.class;

    /**
     * Class name for authorization plugin for Nimbus
     */
    public static final String NIMBUS_AUTHORIZER = "nimbus.authorizer";
    public static final Object NIMBUS_AUTHORIZER_SCHEMA = String.class;

    /**
     * How often nimbus should wake up to renew credentials if needed.
     */
    public static final String NIMBUS_CREDENTIAL_RENEW_FREQ_SECS = "nimbus.credential.renewers.freq.secs";
    public static final Object NIMBUS_CREDENTIAL_RENEW_FREQ_SECS_SCHEMA = Number.class;

    /**
     * A list of credential renewers that nimbus should load.
     */
    public static final String NIMBUS_CREDENTIAL_RENEWERS = "nimbus.credential.renewers.classes";
    public static final Object NIMBUS_CREDENTIAL_RENEWERS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * A list of plugins that nimbus should load during submit topology to populate
     * credentials on user's behalf.
     */
    public static final String NIMBUS_AUTO_CRED_PLUGINS = "nimbus.autocredential.plugins.classes";
    public static final Object NIMBUS_AUTO_CRED_PLUGINS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * Storm UI binds to this port.
     */
    public static final String UI_PORT = "ui.port";
    public static final Object UI_PORT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * HTTP UI port for log viewer
     */
    public static final String LOGVIEWER_PORT = "logviewer.port";
    public static final Object LOGVIEWER_PORT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Childopts for log viewer java process.
     */
    public static final String LOGVIEWER_CHILDOPTS = "logviewer.childopts";
    public static final Object LOGVIEWER_CHILDOPTS_SCHEMA = String.class;

    /**
     * How often to clean up old log files
     */
    public static final String LOGVIEWER_CLEANUP_INTERVAL_SECS = "logviewer.cleanup.interval.secs";
    public static final Object LOGVIEWER_CLEANUP_INTERVAL_SECS_SCHEMA = ConfigValidation.PositiveIntegerValidator;

    /**
     * How many minutes since a log was last modified for the log to be considered for clean-up
     */
    public static final String LOGVIEWER_CLEANUP_AGE_MINS = "logviewer.cleanup.age.mins";
    public static final Object LOGVIEWER_CLEANUP_AGE_MINS_SCHEMA = ConfigValidation.PositiveIntegerValidator;

    /**
     * A list of users allowed to view logs via the Log Viewer
     */
    public static final String LOGS_USERS = "logs.users";
    public static final Object LOGS_USERS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * Appender name used by log viewer to determine log directory.
     */
    public static final String LOGVIEWER_APPENDER_NAME = "logviewer.appender.name";
    public static final Object LOGVIEWER_APPENDER_NAME_SCHEMA = String.class;

    /**
     * Childopts for Storm UI Java process.
     */
    public static final String UI_CHILDOPTS = "ui.childopts";
    public static final Object UI_CHILDOPTS_SCHEMA = String.class;

    /**
     * A class implementing javax.servlet.Filter for authenticating/filtering UI requests
     */
    public static final String UI_FILTER = "ui.filter";
    public static final Object UI_FILTER_SCHEMA = String.class;

    /**
     * Initialization parameters for the javax.servlet.Filter
     */
    public static final String UI_FILTER_PARAMS = "ui.filter.params";
    public static final Object UI_FILTER_PARAMS_SCHEMA = Map.class;

    /**
     * The size of the header buffer for the UI in bytes
     */
    public static final String UI_HEADER_BUFFER_BYTES = "ui.header.buffer.bytes";
    public static final Object UI_HEADER_BUFFER_BYTES_SCHEMA = Number.class;

    /**
     * A list of users allowed to view topologies via the UI
     */
    public static final String UI_USERS = "ui.users";
    public static final Object UI_USERS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * List of DRPC servers so that the DRPCSpout knows who to talk to.
     */
    public static final String DRPC_SERVERS = "drpc.servers";
    public static final Object DRPC_SERVERS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * This port is used by Storm DRPC for receiving HTTP DPRC requests from clients.
     */
    public static final String DRPC_HTTP_PORT = "drpc.http.port";
    public static final Object DRPC_HTTP_PORT_SCHEMA = Number.class;

    /**
     * This port is used by Storm DRPC for receiving HTTPS (SSL) DPRC requests from clients.
     */
    public static final String DRPC_HTTPS_PORT = "drpc.https.port";
    public static final Object DRPC_HTTPS_PORT_SCHEMA = Number.class;

    /**
     * Path to the keystore used by Storm DRPC for setting up HTTPS (SSL).
     */
    public static final String DRPC_HTTPS_KEYSTORE_PATH = "drpc.https.keystore.path";
    public static final Object DRPC_HTTPS_KEYSTORE_PATH_SCHEMA = String.class;

    /**
     * Password to the keystore used by Storm DRPC for setting up HTTPS (SSL).
     */
    public static final String DRPC_HTTPS_KEYSTORE_PASSWORD = "drpc.https.keystore.password";
    public static final Object DRPC_HTTPS_KEYSTORE_PASSWORD_SCHEMA = String.class;

    /**
     * Type of keystore used by Storm DRPC for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    public static final String DRPC_HTTPS_KEYSTORE_TYPE = "drpc.https.keystore.type";
    public static final Object DRPC_HTTPS_KEYSTORE_TYPE_SCHEMA = String.class;

    /**
     * The DRPC transport plug-in for Thrift client/server communication
     */
    public static final String DRPC_THRIFT_TRANSPORT_PLUGIN = "drpc.thrift.transport";
    public static final Object DRPC_THRIFT_TRANSPORT_PLUGIN_SCHEMA = String.class;

    /**
     * This port is used by Storm DRPC for receiving DPRC requests from clients.
     */
    public static final String DRPC_PORT = "drpc.port";
    public static final Object DRPC_PORT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Class name for authorization plugin for DRPC client
     */
    public static final String DRPC_AUTHORIZER = "drpc.authorizer";
    public static final Object DRPC_AUTHORIZER_SCHEMA = String.class;

    /**
     * The Access Control List for the DRPC Authorizer.
     * @see DRPCSimpleAclAuthorizer
     */
    public static final String DRPC_AUTHORIZER_ACL = "drpc.authorizer.acl";
    public static final Object DRPC_AUTHORIZER_ACL_SCHEMA = Map.class;

    /**
     * File name of the DRPC Authorizer ACL.
     * @see DRPCSimpleAclAuthorizer
     */
    public static final String DRPC_AUTHORIZER_ACL_FILENAME = "drpc.authorizer.acl.filename";
    public static final Object DRPC_AUTHORIZER_ACL_FILENAME_SCHEMA = String.class;

    /**
     * Whether the DRPCSimpleAclAuthorizer should deny requests for operations
     * involving functions that have no explicit ACL entry. When set to false
     * (the default) DRPC functions that have no entry in the ACL will be
     * permitted, which is appropriate for a development environment. When set
     * to true, explicit ACL entries are required for every DRPC function, and
     * any request for functions will be denied.
     * @see DRPCSimpleAclAuthorizer
     */
    public static final String DRPC_AUTHORIZER_ACL_STRICT = "drpc.authorizer.acl.strict";
    public static final Object DRPC_AUTHORIZER_ACL_STRICT_SCHEMA = Boolean.class;

    /**
     * DRPC thrift server worker threads
     */
    public static final String DRPC_WORKER_THREADS = "drpc.worker.threads";
    public static final Object DRPC_WORKER_THREADS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The maximum buffer size thrift should use when reading messages for DRPC.
     */
    public static final String DRPC_MAX_BUFFER_SIZE = "drpc.max_buffer_size";
    public static final Object DRPC_MAX_BUFFER_SIZE_SCHEMA = Number.class;

    /**
     * DRPC thrift server queue size
     */
    public static final String DRPC_QUEUE_SIZE = "drpc.queue.size";
    public static final Object DRPC_QUEUE_SIZE_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The DRPC invocations transport plug-in for Thrift client/server communication
     */
    public static final String DRPC_INVOCATIONS_THRIFT_TRANSPORT_PLUGIN = "drpc.invocations.thrift.transport";
    public static final Object DRPC_INVOCATIONS_THRIFT_TRANSPORT_PLUGIN_SCHEMA = String.class;

    /**
     * This port on Storm DRPC is used by DRPC topologies to receive function invocations and send results back.
     */
    public static final String DRPC_INVOCATIONS_PORT = "drpc.invocations.port";
    public static final Object DRPC_INVOCATIONS_PORT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * DRPC invocations thrift server worker threads
     */
    public static final String DRPC_INVOCATIONS_THREADS = "drpc.invocations.threads";
    public static final Object DRPC_INVOCATIONS_THREADS_SCHEMA = Number.class;

    /**
     * The timeout on DRPC requests within the DRPC server. Defaults to 10 minutes. Note that requests can also
     * timeout based on the socket timeout on the DRPC client, and separately based on the topology message
     * timeout for the topology implementing the DRPC function.
     */
    public static final String DRPC_REQUEST_TIMEOUT_SECS  = "drpc.request.timeout.secs";
    public static final Object DRPC_REQUEST_TIMEOUT_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Childopts for Storm DRPC Java process.
     */
    public static final String DRPC_CHILDOPTS = "drpc.childopts";
    public static final Object DRPC_CHILDOPTS_SCHEMA = String.class;

    /**
     * Class name of the HTTP credentials plugin for the UI.
     */
    public static final String UI_HTTP_CREDS_PLUGIN = "ui.http.creds.plugin";
    public static final Object UI_HTTP_CREDS_PLUGIN_SCHEMA = String.class;

    /**
     * Class name of the HTTP credentials plugin for DRPC.
     */
    public static final String DRPC_HTTP_CREDS_PLUGIN = "drpc.http.creds.plugin";
    public static final Object DRPC_HTTP_CREDS_PLUGIN_SCHEMA = String.class;

    /**
     * the metadata configured on the supervisor
     */
    public static final String SUPERVISOR_SCHEDULER_META = "supervisor.scheduler.meta";
    public static final Object SUPERVISOR_SCHEDULER_META_SCHEMA = Map.class;
    /**
     * A list of ports that can run workers on this supervisor. Each worker uses one port, and
     * the supervisor will only run one worker per port. Use this configuration to tune
     * how many workers run on each machine.
     */
    public static final String SUPERVISOR_SLOTS_PORTS = "supervisor.slots.ports";
    public static final Object SUPERVISOR_SLOTS_PORTS_SCHEMA = ConfigValidation.IntegersValidator;

    /**
     * A number representing the maximum number of workers any single topology can acquire.
     */
    public static final String NIMBUS_SLOTS_PER_TOPOLOGY = "nimbus.slots.perTopology";
    public static final Object NIMBUS_SLOTS_PER_TOPOLOGY_SCHEMA = Number.class;

    /**
     * A class implementing javax.servlet.Filter for DRPC HTTP requests
     */
    public static final String DRPC_HTTP_FILTER = "drpc.http.filter";
    public static final Object DRPC_HTTP_FILTER_SCHEMA = String.class;

    /**
     * Initialization parameters for the javax.servlet.Filter of the DRPC HTTP
     * service
     */
    public static final String DRPC_HTTP_FILTER_PARAMS = "drpc.http.filter.params";
    public static final Object DRPC_HTTP_FILTER_PARAMS_SCHEMA = Map.class;

    /**
     * A number representing the maximum number of executors any single topology can acquire.
     */
    public static final String NIMBUS_EXECUTORS_PER_TOPOLOGY = "nimbus.executors.perTopology";
    public static final Object NIMBUS_EXECUTORS_PER_TOPOLOGY_SCHEMA = Number.class;

    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the supervisor daemon.
     */
    public static final String SUPERVISOR_CHILDOPTS = "supervisor.childopts";
    public static final Object SUPERVISOR_CHILDOPTS_SCHEMA = String.class;

    /**
     * How long a worker can go without heartbeating before the supervisor tries to
     * restart the worker process.
     */
    public static final String SUPERVISOR_WORKER_TIMEOUT_SECS = "supervisor.worker.timeout.secs";
    public static final Object SUPERVISOR_WORKER_TIMEOUT_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * How long a worker can go without heartbeating during the initial launch before
     * the supervisor tries to restart the worker process. This value override
     * supervisor.worker.timeout.secs during launch because there is additional
     * overhead to starting and configuring the JVM on launch.
     */
    public static final String SUPERVISOR_WORKER_START_TIMEOUT_SECS = "supervisor.worker.start.timeout.secs";
    public static final Object SUPERVISOR_WORKER_START_TIMEOUT_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Whether or not the supervisor should launch workers assigned to it. Defaults
     * to true -- and you should probably never change this value. This configuration
     * is used in the Storm unit tests.
     */
    public static final String SUPERVISOR_ENABLE = "supervisor.enable";
    public static final Object SUPERVISOR_ENABLE_SCHEMA = Boolean.class;

    /**
     * how often the supervisor sends a heartbeat to the master.
     */
    public static final String SUPERVISOR_HEARTBEAT_FREQUENCY_SECS = "supervisor.heartbeat.frequency.secs";
    public static final Object SUPERVISOR_HEARTBEAT_FREQUENCY_SECS_SCHEMA = ConfigValidation.IntegerValidator;


    /**
     * How often the supervisor checks the worker heartbeats to see if any of them
     * need to be restarted.
     */
    public static final String SUPERVISOR_MONITOR_FREQUENCY_SECS = "supervisor.monitor.frequency.secs";
    public static final Object SUPERVISOR_MONITOR_FREQUENCY_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Should the supervior try to run the worker as the lauching user or not.  Defaults to false.
     */
    public static final String SUPERVISOR_RUN_WORKER_AS_USER = "supervisor.run.worker.as.user";
    public static final Object SUPERVISOR_RUN_WORKER_AS_USER_SCHEMA = Boolean.class;

    /**
     * Full path to the worker-laucher executable that will be used to lauch workers when
     * SUPERVISOR_RUN_WORKER_AS_USER is set to true.
     */
    public static final String SUPERVISOR_WORKER_LAUNCHER = "supervisor.worker.launcher";
    public static final Object SUPERVISOR_WORKER_LAUNCHER_SCHEMA = String.class;

    /**
     * The jvm opts provided to workers launched by this supervisor. All "%ID%", "%WORKER-ID%", "%TOPOLOGY-ID%"
     * and "%WORKER-PORT%" substrings are replaced with:
     * %ID%          -> port (for backward compatibility),
     * %WORKER-ID%   -> worker-id,
     * %TOPOLOGY-ID%    -> topology-id,
     * %WORKER-PORT% -> port.
     */
    public static final String WORKER_CHILDOPTS = "worker.childopts";
    public static final Object WORKER_CHILDOPTS_SCHEMA = ConfigValidation.StringOrStringListValidator;

    /**
     * The jvm opts provided to workers launched by this supervisor for GC. All "%ID%" substrings are replaced
     * with an identifier for this worker.  Because the JVM complains about multiple GC opts the topology
     * can override this default value by setting topology.worker.gc.childopts.
     */
    public static final String WORKER_GC_CHILDOPTS = "worker.gc.childopts";
    public static final Object WORKER_GC_CHILDOPTS_SCHEMA = ConfigValidation.StringOrStringListValidator;

    /**
     * control how many worker receiver threads we need per worker
     */
    public static final String WORKER_RECEIVER_THREAD_COUNT = "topology.worker.receiver.thread.count";
    public static final Object WORKER_RECEIVER_THREAD_COUNT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * How often this worker should heartbeat to the supervisor.
     */
    public static final String WORKER_HEARTBEAT_FREQUENCY_SECS = "worker.heartbeat.frequency.secs";
    public static final Object WORKER_HEARTBEAT_FREQUENCY_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * How often a task should heartbeat its status to the master.
     */
    public static final String TASK_HEARTBEAT_FREQUENCY_SECS = "task.heartbeat.frequency.secs";
    public static final Object TASK_HEARTBEAT_FREQUENCY_SECS_SCHEMA = ConfigValidation.IntegerValidator;


    /**
     * How often a task should sync its connections with other tasks (if a task is
     * reassigned, the other tasks sending messages to it need to refresh their connections).
     * In general though, when a reassignment happens other tasks will be notified
     * almost immediately. This configuration is here just in case that notification doesn't
     * come through.
     */
    public static final String TASK_REFRESH_POLL_SECS = "task.refresh.poll.secs";
    public static final Object TASK_REFRESH_POLL_SECS_SCHEMA = ConfigValidation.IntegerValidator;


    /**
     * How often a task should sync credentials, worst case.
     */
    public static final String TASK_CREDENTIALS_POLL_SECS = "task.credentials.poll.secs";
    public static final Object TASK_CREDENTIALS_POLL_SECS_SCHEMA = Number.class;


    /**
     * A list of users that are allowed to interact with the topology.  To use this set
     * nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    public static final String TOPOLOGY_USERS = "topology.users";
    public static final Object TOPOLOGY_USERS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * A list of groups that are allowed to interact with the topology.  To use this set
     * nimbus.authorizer to backtype.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    public static final String TOPOLOGY_GROUPS = "topology.groups";
    public static final Object TOPOLOGY_GROUPS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * True if Storm should timeout messages or not. Defaults to true. This is meant to be used
     * in unit tests to prevent tuples from being accidentally timed out during the test.
     */
    public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
    public static final Object TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS_SCHEMA = Boolean.class;

    /**
     * When set to true, Storm will log every message that's emitted.
     */
    public static final String TOPOLOGY_DEBUG = "topology.debug";
    public static final Object TOPOLOGY_DEBUG_SCHEMA = Boolean.class;

    /**
     * The serializer for communication between shell components and non-JVM
     * processes
     */
    public static final String TOPOLOGY_MULTILANG_SERIALIZER = "topology.multilang.serializer";
    public static final Object TOPOLOGY_MULTILANG_SERIALIZER_SCHEMA = String.class;

    /**
     * How many processes should be spawned around the cluster to execute this
     * topology. Each process will execute some number of tasks as threads within
     * them. This parameter should be used in conjunction with the parallelism hints
     * on each component in the topology to tune the performance of a topology.
     */
    public static final String TOPOLOGY_WORKERS = "topology.workers";
    public static final Object TOPOLOGY_WORKERS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * How many instances to create for a spout/bolt. A task runs on a thread with zero or more
     * other tasks for the same spout/bolt. The number of tasks for a spout/bolt is always
     * the same throughout the lifetime of a topology, but the number of executors (threads) for
     * a spout/bolt can change over time. This allows a topology to scale to more or less resources
     * without redeploying the topology or violating the constraints of Storm (such as a fields grouping
     * guaranteeing that the same value goes to the same task).
     */
    public static final String TOPOLOGY_TASKS = "topology.tasks";
    public static final Object TOPOLOGY_TASKS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * How many executors to spawn for ackers.
     *
     * <p>If this is set to 0, then Storm will immediately ack tuples as soon
     * as they come off the spout, effectively disabling reliability.</p>
     */
    public static final String TOPOLOGY_ACKER_EXECUTORS = "topology.acker.executors";
    public static final Object TOPOLOGY_ACKER_EXECUTORS_SCHEMA = ConfigValidation.IntegerValidator;


    /**
     * The maximum amount of time given to the topology to fully process a message
     * emitted by a spout. If the message is not acked within this time frame, Storm
     * will fail the message on the spout. Some spouts implementations will then replay
     * the message at a later time.
     */
    public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
    public static final Object TOPOLOGY_MESSAGE_TIMEOUT_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * A list of serialization registrations for Kryo ( http://code.google.com/p/kryo/ ),
     * the underlying serialization framework for Storm. A serialization can either
     * be the name of a class (in which case Kryo will automatically create a serializer for the class
     * that saves all the object's fields), or an implementation of com.esotericsoftware.kryo.Serializer.
     *
     * See Kryo's documentation for more information about writing custom serializers.
     */
    public static final String TOPOLOGY_KRYO_REGISTER = "topology.kryo.register";
    public static final Object TOPOLOGY_KRYO_REGISTER_SCHEMA = ConfigValidation.KryoRegValidator;

    /**
     * A list of classes that customize storm's kryo instance during start-up.
     * Each listed class name must implement IKryoDecorator. During start-up the
     * listed class is instantiated with 0 arguments, then its 'decorate' method
     * is called with storm's kryo instance as the only argument.
     */
    public static final String TOPOLOGY_KRYO_DECORATORS = "topology.kryo.decorators";
    public static final Object TOPOLOGY_KRYO_DECORATORS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * Class that specifies how to create a Kryo instance for serialization. Storm will then apply
     * topology.kryo.register and topology.kryo.decorators on top of this. The default implementation
     * implements topology.fall.back.on.java.serialization and turns references off.
     */
    public static final String TOPOLOGY_KRYO_FACTORY = "topology.kryo.factory";
    public static final Object TOPOLOGY_KRYO_FACTORY_SCHEMA = String.class;


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
    public static final String TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS= "topology.skip.missing.kryo.registrations";
    public static final Object TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS_SCHEMA = Boolean.class;

    /*
     * A list of classes implementing IMetricsConsumer (See storm.yaml.example for exact config format).
     * Each listed class will be routed all the metrics data generated by the storm metrics API.
     * Each listed class maps 1:1 to a system bolt named __metrics_ClassName#N, and it's parallelism is configurable.
     */
    public static final String TOPOLOGY_METRICS_CONSUMER_REGISTER = "topology.metrics.consumer.register";
    public static final Object TOPOLOGY_METRICS_CONSUMER_REGISTER_SCHEMA = ConfigValidation.MapsValidator;


    /**
     * The maximum parallelism allowed for a component in this topology. This configuration is
     * typically used in testing to limit the number of threads spawned in local mode.
     */
    public static final String TOPOLOGY_MAX_TASK_PARALLELISM="topology.max.task.parallelism";
    public static final Object TOPOLOGY_MAX_TASK_PARALLELISM_SCHEMA = ConfigValidation.IntegerValidator;


    /**
     * The maximum number of tuples that can be pending on a spout task at any given time.
     * This config applies to individual tasks, not to spouts or topologies as a whole.
     *
     * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
     * Note that this config parameter has no effect for unreliable spouts that don't tag
     * their tuples with a message id.
     */
    public static final String TOPOLOGY_MAX_SPOUT_PENDING="topology.max.spout.pending";
    public static final Object TOPOLOGY_MAX_SPOUT_PENDING_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * A class that implements a strategy for what to do when a spout needs to wait. Waiting is
     * triggered in one of two conditions:
     *
     * 1. nextTuple emits no tuples
     * 2. The spout has hit maxSpoutPending and can't emit any more tuples
     */
    public static final String TOPOLOGY_SPOUT_WAIT_STRATEGY="topology.spout.wait.strategy";
    public static final Object TOPOLOGY_SPOUT_WAIT_STRATEGY_SCHEMA = String.class;

    /**
     * The amount of milliseconds the SleepEmptyEmitStrategy should sleep for.
     */
    public static final String TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS="topology.sleep.spout.wait.strategy.time.ms";
    public static final Object TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The maximum amount of time a component gives a source of state to synchronize before it requests
     * synchronization again.
     */
    public static final String TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS="topology.state.synchronization.timeout.secs";
    public static final Object TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The percentage of tuples to sample to produce stats for a task.
     */
    public static final String TOPOLOGY_STATS_SAMPLE_RATE="topology.stats.sample.rate";
    public static final Object TOPOLOGY_STATS_SAMPLE_RATE_SCHEMA = ConfigValidation.DoubleValidator;

    /**
     * The time period that builtin metrics data in bucketed into.
     */
    public static final String TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS="topology.builtin.metrics.bucket.size.secs";
    public static final Object TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Whether or not to use Java serialization in a topology.
     */
    public static final String TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION="topology.fall.back.on.java.serialization";
    public static final Object TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_SCHEMA = Boolean.class;

    /**
     * Topology-specific options for the worker child process. This is used in addition to WORKER_CHILDOPTS.
     */
    public static final String TOPOLOGY_WORKER_CHILDOPTS="topology.worker.childopts";
    public static final Object TOPOLOGY_WORKER_CHILDOPTS_SCHEMA = ConfigValidation.StringOrStringListValidator;

    /**
     * Topology-specific options GC for the worker child process. This overrides WORKER_GC_CHILDOPTS.
     */
    public static final String TOPOLOGY_WORKER_GC_CHILDOPTS="topology.worker.gc.childopts";
    public static final Object TOPOLOGY_WORKER_GC_CHILDOPTS_SCHEMA = ConfigValidation.StringOrStringListValidator;

    /**
     * Topology-specific classpath for the worker child process. This is combined to the usual classpath.
     */
    public static final String TOPOLOGY_CLASSPATH="topology.classpath";
    public static final Object TOPOLOGY_CLASSPATH_SCHEMA = ConfigValidation.StringOrStringListValidator;

    /**
     * Topology-specific environment variables for the worker child process.
     * This is added to the existing environment (that of the supervisor)
     */
     public static final String TOPOLOGY_ENVIRONMENT="topology.environment";
     public static final Object TOPOLOGY_ENVIRONMENT_SCHEMA = Map.class;

    /**
     * This config is available for TransactionalSpouts, and contains the id ( a String) for
     * the transactional topology. This id is used to store the state of the transactional
     * topology in Zookeeper.
     */
    public static final String TOPOLOGY_TRANSACTIONAL_ID="topology.transactional.id";
    public static final Object TOPOLOGY_TRANSACTIONAL_ID_SCHEMA = String.class;

    /**
     * A list of task hooks that are automatically added to every spout and bolt in the topology. An example
     * of when you'd do this is to add a hook that integrates with your internal
     * monitoring system. These hooks are instantiated using the zero-arg constructor.
     */
    public static final String TOPOLOGY_AUTO_TASK_HOOKS="topology.auto.task.hooks";
    public static final Object TOPOLOGY_AUTO_TASK_HOOKS_SCHEMA = ConfigValidation.StringsValidator;


    /**
     * The size of the Disruptor receive queue for each executor. Must be a power of 2.
     */
    public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE="topology.executor.receive.buffer.size";
    public static final Object TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE_SCHEMA = ConfigValidation.PowerOf2Validator;

    /**
     * The maximum number of messages to batch from the thread receiving off the network to the
     * executor queues. Must be a power of 2.
     */
    public static final String TOPOLOGY_RECEIVER_BUFFER_SIZE="topology.receiver.buffer.size";
    public static final Object TOPOLOGY_RECEIVER_BUFFER_SIZE_SCHEMA = ConfigValidation.PowerOf2Validator;

    /**
     * The size of the Disruptor send queue for each executor. Must be a power of 2.
     */
    public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE="topology.executor.send.buffer.size";
    public static final Object TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE_SCHEMA = ConfigValidation.PowerOf2Validator;

    /**
     * The size of the Disruptor transfer queue for each worker.
     */
    public static final String TOPOLOGY_TRANSFER_BUFFER_SIZE="topology.transfer.buffer.size";
    public static final Object TOPOLOGY_TRANSFER_BUFFER_SIZE_SCHEMA = ConfigValidation.IntegerValidator;

   /**
    * How often a tick tuple from the "__system" component and "__tick" stream should be sent
    * to tasks. Meant to be used as a component-specific configuration.
    */
    public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS="topology.tick.tuple.freq.secs";
    public static final Object TOPOLOGY_TICK_TUPLE_FREQ_SECS_SCHEMA = ConfigValidation.IntegerValidator;


   /**
    * Configure the wait strategy used for internal queuing. Can be used to tradeoff latency
    * vs. throughput
    */
    public static final String TOPOLOGY_DISRUPTOR_WAIT_STRATEGY="topology.disruptor.wait.strategy";
    public static final Object TOPOLOGY_DISRUPTOR_WAIT_STRATEGY_SCHEMA = String.class;

   /**
    * The size of the shared thread pool for worker tasks to make use of. The thread pool can be accessed
    * via the TopologyContext.
    */
    public static final String TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE="topology.worker.shared.thread.pool.size";
    public static final Object TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The interval in seconds to use for determining whether to throttle error reported to Zookeeper. For example,
     * an interval of 10 seconds with topology.max.error.report.per.interval set to 5 will only allow 5 errors to be
     * reported to Zookeeper per task for every 10 second interval of time.
     */
    public static final String TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS="topology.error.throttle.interval.secs";
    public static final Object TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * See doc for TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS
     */
    public static final String TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL="topology.max.error.report.per.interval";
    public static final Object TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL_SCHEMA = ConfigValidation.IntegerValidator;


    /**
     * How often a batch can be emitted in a Trident topology.
     */
    public static final String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS="topology.trident.batch.emit.interval.millis";
    public static final Object TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * Name of the topology. This config is automatically set by Storm when the topology is submitted.
     */
    public final static String TOPOLOGY_NAME="topology.name";
    public static final Object TOPOLOGY_NAME_SCHEMA = String.class;

    /**
     * The principal who submitted a topology
     */
    public final static String TOPOLOGY_SUBMITTER_PRINCIPAL = "topology.submitter.principal";
    public static final Object TOPOLOGY_SUBMITTER_PRINCIPAL_SCHEMA = String.class;

    /**
     * The local user name of the user who submitted a topology.
     */
    public static final String TOPOLOGY_SUBMITTER_USER = "topology.submitter.user";
    public static final Object TOPOLOGY_SUBMITTER_USER_SCHEMA = String.class;

    /**
     * Array of components that scheduler should try to place on separate hosts.
     */
    public static final String TOPOLOGY_SPREAD_COMPONENTS = "topology.spread.components";
    public static final Object TOPOLOGY_SPREAD_COMPONENTS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * A list of IAutoCredentials that the topology should load and use.
     */
    public static final String TOPOLOGY_AUTO_CREDENTIALS = "topology.auto-credentials";
    public static final Object TOPOLOGY_AUTO_CREDENTIALS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * Max pending tuples in one ShellBolt
     */
    public static final String TOPOLOGY_SHELLBOLT_MAX_PENDING="topology.shellbolt.max.pending";
    public static final Object TOPOLOGY_SHELLBOLT_MAX_PENDING_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The root directory in ZooKeeper for metadata about TransactionalSpouts.
     */
    public static final String TRANSACTIONAL_ZOOKEEPER_ROOT="transactional.zookeeper.root";
    public static final Object TRANSACTIONAL_ZOOKEEPER_ROOT_SCHEMA = String.class;

    /**
     * The list of zookeeper servers in which to keep the transactional state. If null (which is default),
     * will use storm.zookeeper.servers
     */
    public static final String TRANSACTIONAL_ZOOKEEPER_SERVERS="transactional.zookeeper.servers";
    public static final Object TRANSACTIONAL_ZOOKEEPER_SERVERS_SCHEMA = ConfigValidation.StringsValidator;

    /**
     * The port to use to connect to the transactional zookeeper servers. If null (which is default),
     * will use storm.zookeeper.port
     */
    public static final String TRANSACTIONAL_ZOOKEEPER_PORT="transactional.zookeeper.port";
    public static final Object TRANSACTIONAL_ZOOKEEPER_PORT_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The number of threads that should be used by the zeromq context in each worker process.
     */
    public static final String ZMQ_THREADS = "zmq.threads";
    public static final Object ZMQ_THREADS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * How long a connection should retry sending messages to a target host when
     * the connection is closed. This is an advanced configuration and can almost
     * certainly be ignored.
     */
    public static final String ZMQ_LINGER_MILLIS = "zmq.linger.millis";
    public static final Object ZMQ_LINGER_MILLIS_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * The high water for the ZeroMQ push sockets used for networking. Use this config to prevent buffer explosion
     * on the networking layer.
     */
    public static final String ZMQ_HWM = "zmq.hwm";
    public static final Object ZMQ_HWM_SCHEMA = ConfigValidation.IntegerValidator;

    /**
     * This value is passed to spawned JVMs (e.g., Nimbus, Supervisor, and Workers)
     * for the java.library.path value. java.library.path tells the JVM where
     * to look for native libraries. It is necessary to set this config correctly since
     * Storm uses the ZeroMQ and JZMQ native libs.
     */
    public static final String JAVA_LIBRARY_PATH = "java.library.path";
    public static final Object JAVA_LIBRARY_PATH_SCHEMA = String.class;

    /**
     * The path to use as the zookeeper dir when running a zookeeper server via
     * "storm dev-zookeeper". This zookeeper instance is only intended for development;
     * it is not a production grade zookeeper setup.
     */
    public static final String DEV_ZOOKEEPER_PATH = "dev.zookeeper.path";
    public static final Object DEV_ZOOKEEPER_PATH_SCHEMA = String.class;

    /**
     * A map from topology name to the number of machines that should be dedicated for that topology. Set storm.scheduler
     * to backtype.storm.scheduler.IsolationScheduler to make use of the isolation scheduler.
     */
    public static final String ISOLATION_SCHEDULER_MACHINES = "isolation.scheduler.machines";
    public static final Object ISOLATION_SCHEDULER_MACHINES_SCHEMA = ConfigValidation.MapOfStringToNumberValidator;

    /**
     * A map from the user name to the number of machines that should that user is allowed to use. Set storm.scheduler
     * to backtype.storm.scheduler.multitenant.MultitenantScheduler
     */
    public static final String MULTITENANT_SCHEDULER_USER_POOLS = "multitenant.scheduler.user.pools";
    public static final Object MULTITENANT_SCHEDULER_USER_POOLS_SCHEMA = ConfigValidation.MapOfStringToNumberValidator;

    /**
     * The number of machines that should be used by this topology to isolate it from all others. Set storm.scheduler
     * to backtype.storm.scheduler.multitenant.MultitenantScheduler
     */
    public static final String TOPOLOGY_ISOLATED_MACHINES = "topology.isolate.machines";
    public static final Object TOPOLOGY_ISOLATED_MACHINES_SCHEMA = Number.class;

    /**
     * HDFS information, used to get the delegation token on behalf of the topology
     * submitter user and renew the tokens. see {@link backtype.storm.security.auth.hadoop.AutoHDFS}
     * kerberos principal name with realm should be provided.
     */
    public static final Object TOPOLOGY_HDFS_PRINCIPAL = "topology.hdfs.user";
    public static final Object TOPOLOGY_HDFS_PRINCIPAL_SCHEMA = String.class;

    /**
     * The HDFS URI to be used by AutoHDFS.java to grab the delegation token on topology
     * submitter user's behalf by the nimbus. If this is not provided the default URI provided
     * in the hdfs configuration files will be used.
     */
    public static final Object TOPOLOGY_HDFS_URI = "topology.hdfs.uri";
    public static final Object TOPOLOGY_HDFS_URI_SCHEMA = String.class;

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
}
