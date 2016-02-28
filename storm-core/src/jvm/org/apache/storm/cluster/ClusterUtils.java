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
package org.apache.storm.cluster;

import org.apache.storm.Config;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterUtils {

    public static final String ZK_SEPERATOR = "/";

    public static final String ASSIGNMENTS_ROOT = "assignments";
    public static final String CODE_ROOT = "code";
    public static final String STORMS_ROOT = "storms";
    public static final String SUPERVISORS_ROOT = "supervisors";
    public static final String WORKERBEATS_ROOT = "workerbeats";
    public static final String BACKPRESSURE_ROOT = "backpressure";
    public static final String ERRORS_ROOT = "errors";
    public static final String BLOBSTORE_ROOT = "blobstore";
    public static final String BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_ROOT = "blobstoremaxkeysequencenumber";
    public static final String NIMBUSES_ROOT = "nimbuses";
    public static final String CREDENTIALS_ROOT = "credentials";
    public static final String LOGCONFIG_ROOT = "logconfigs";
    public static final String PROFILERCONFIG_ROOT = "profilerconfigs";

    public static final String ASSIGNMENTS_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_ROOT;
    public static final String STORMS_SUBTREE = ZK_SEPERATOR + STORMS_ROOT;
    public static final String SUPERVISORS_SUBTREE = ZK_SEPERATOR + SUPERVISORS_ROOT;
    public static final String WORKERBEATS_SUBTREE = ZK_SEPERATOR + WORKERBEATS_ROOT;
    public static final String BACKPRESSURE_SUBTREE = ZK_SEPERATOR + BACKPRESSURE_ROOT;
    public static final String ERRORS_SUBTREE = ZK_SEPERATOR + ERRORS_ROOT;
    public static final String BLOBSTORE_SUBTREE = ZK_SEPERATOR + BLOBSTORE_ROOT;
    public static final String BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE = ZK_SEPERATOR + BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_ROOT;
    public static final String NIMBUSES_SUBTREE = ZK_SEPERATOR + NIMBUSES_ROOT;
    public static final String CREDENTIALS_SUBTREE = ZK_SEPERATOR + CREDENTIALS_ROOT;
    public static final String LOGCONFIG_SUBTREE = ZK_SEPERATOR + LOGCONFIG_ROOT;
    public static final String PROFILERCONFIG_SUBTREE = ZK_SEPERATOR + PROFILERCONFIG_ROOT;

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static final ClusterUtils INSTANCE = new ClusterUtils();
    private static ClusterUtils _instance = INSTANCE;

    /**
     * Provide an instance of this class for delegates to use. To mock out delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     *
     * @param u a Cluster instance
     */
    public static void setInstance(ClusterUtils u) {
        _instance = u;
    }

    /**
     * Resets the singleton instance to the default. This is helpful to reset the class to its original functionality when mocking is no longer desired.
     */
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    public static List<ACL> mkTopoOnlyAcls(Map topoConf) throws NoSuchAlgorithmException {
        List<ACL> aclList = null;
        String payload = (String) topoConf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
        if (Utils.isZkAuthenticationConfiguredTopology(topoConf)) {
            aclList = new ArrayList<>();
            ACL acl1 = ZooDefs.Ids.CREATOR_ALL_ACL.get(0);
            aclList.add(acl1);
            ACL acl2 = new ACL(ZooDefs.Perms.READ, new Id("digest", DigestAuthenticationProvider.generateDigest(payload)));
            aclList.add(acl2);
        }
        return aclList;
    }

    public static String supervisorPath(String id) {
        return SUPERVISORS_SUBTREE + ZK_SEPERATOR + id;
    }

    public static String assignmentPath(String id) {
        return ASSIGNMENTS_SUBTREE + ZK_SEPERATOR + id;
    }

    public static String blobstorePath(String key) {
        return BLOBSTORE_SUBTREE + ZK_SEPERATOR + key;
    }

    public static String blobstoreMaxKeySequenceNumberPath(String key) {
        return BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE + ZK_SEPERATOR + key;
    }

    public static String nimbusPath(String id) {
        return NIMBUSES_SUBTREE + ZK_SEPERATOR + id;
    }

    public static String stormPath(String id) {
        return STORMS_SUBTREE + ZK_SEPERATOR + id;
    }

    public static String workerbeatStormRoot(String stormId) {
        return WORKERBEATS_SUBTREE + ZK_SEPERATOR + stormId;
    }

    public static String workerbeatPath(String stormId, String node, Long port) {
        return workerbeatStormRoot(stormId) + ZK_SEPERATOR + node + "-" + port;
    }

    public static String backpressureStormRoot(String stormId) {
        return BACKPRESSURE_SUBTREE + ZK_SEPERATOR + stormId;
    }

    public static String backpressurePath(String stormId, String node, Long port) {
        return backpressureStormRoot(stormId) + ZK_SEPERATOR + node + "-" + port;
    }

    public static String errorStormRoot(String stormId) {
        return ERRORS_SUBTREE + ZK_SEPERATOR + stormId;
    }

    public static String errorPath(String stormId, String componentId) {
        try {
            return errorStormRoot(stormId) + ZK_SEPERATOR + URLEncoder.encode(componentId, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static String lastErrorPath(String stormId, String componentId) {
        return errorPath(stormId, componentId) + "-last-error";
    }

    public static String credentialsPath(String stormId) {
        return CREDENTIALS_SUBTREE + ZK_SEPERATOR + stormId;
    }

    public static String logConfigPath(String stormId) {
        return LOGCONFIG_SUBTREE + ZK_SEPERATOR + stormId;
    }

    public static String profilerConfigPath(String stormId) {
        return PROFILERCONFIG_SUBTREE + ZK_SEPERATOR + stormId;
    }

    public static String profilerConfigPath(String stormId, String host, Long port, ProfileAction requestType) {
        return profilerConfigPath(stormId) + ZK_SEPERATOR + host + "_" + port + "_" + requestType;
    }

    public static <T> T maybeDeserialize(byte[] serialized, Class<T> clazz) {
        if (serialized != null) {
            return Utils.deserialize(serialized, clazz);
        }
        return null;
    }

    /**
     * Ensures that we only return heartbeats for executors assigned to this worker
     * @param executors
     * @param workerHeartbeat
     * @return
     */
    public static Map<ExecutorInfo, ExecutorBeat> convertExecutorBeats(List<ExecutorInfo> executors, ClusterWorkerHeartbeat workerHeartbeat) {
        Map<ExecutorInfo, ExecutorBeat> executorWhb = new HashMap<>();
        Map<ExecutorInfo, ExecutorStats> executorStatsMap = workerHeartbeat.get_executor_stats();
        for (ExecutorInfo executor : executors) {
            if (executorStatsMap.containsKey(executor)) {
                int time = workerHeartbeat.get_time_secs();
                int uptime = workerHeartbeat.get_uptime_secs();
                ExecutorStats executorStats = workerHeartbeat.get_executor_stats().get(executor);
                ExecutorBeat executorBeat = new ExecutorBeat(time, uptime, executorStats);
                executorWhb.put(executor, executorBeat);
            }
        }
        return executorWhb;
    }

    public IStormClusterState mkStormClusterStateImpl(Object stateStorage, List<ACL> acls, ClusterStateContext context) throws Exception {
        if (stateStorage instanceof IStateStorage) {
            return new StormClusterStateImpl((IStateStorage) stateStorage, acls, context, false);
        } else {
            IStateStorage Storage = _instance.mkStateStorageImpl((Map) stateStorage, (Map) stateStorage, acls, context);
            return new StormClusterStateImpl(Storage, acls, context, true);
        }
    }

    public IStateStorage mkStateStorageImpl(Map config, Map auth_conf, List<ACL> acls, ClusterStateContext context) throws Exception {
        String className = null;
        IStateStorage stateStorage = null;
        if (config.get(Config.STORM_CLUSTER_STATE_STORE) != null) {
            className = (String) config.get(Config.STORM_CLUSTER_STATE_STORE);
        } else {
            className = "org.apache.storm.cluster.ZKStateStorageFactory";
        }
        Class clazz = Class.forName(className);
        StateStorageFactory storageFactory = (StateStorageFactory) clazz.newInstance();
        stateStorage = storageFactory.mkStore(config, auth_conf, acls, context);
        return stateStorage;
    }

    public static IStateStorage mkStateStorage(Map config, Map auth_conf, List<ACL> acls, ClusterStateContext context) throws Exception {
        return _instance.mkStateStorageImpl(config, auth_conf, acls, context);
    }

    public static IStormClusterState mkStormClusterState(Object StateStorage, List<ACL> acls, ClusterStateContext context) throws Exception {
        return _instance.mkStormClusterStateImpl(StateStorage, acls, context);
    }

    public static String stringifyError(Throwable error) {
        StringWriter result = new StringWriter();
        PrintWriter printWriter = new PrintWriter(result);
        error.printStackTrace(printWriter);
        return result.toString();
    }
}
