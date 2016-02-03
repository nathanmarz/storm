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


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cluster {

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

    public static final String ASSIGNMENTS_SUBTREE;
    public static final String STORMS_SUBTREE;
    public static final String SUPERVISORS_SUBTREE;
    public static final String WORKERBEATS_SUBTREE;
    public static final String BACKPRESSURE_SUBTREE;
    public static final String ERRORS_SUBTREE;
    public static final String BLOBSTORE_SUBTREE;
    public static final String BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE;
    public static final String NIMBUSES_SUBTREE;
    public static final String CREDENTIALS_SUBTREE;
    public static final String LOGCONFIG_SUBTREE;
    public static final String PROFILERCONFIG_SUBTREE;

    static {
        ASSIGNMENTS_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_ROOT;
        STORMS_SUBTREE = ZK_SEPERATOR + STORMS_ROOT;
        SUPERVISORS_SUBTREE = ZK_SEPERATOR + SUPERVISORS_ROOT;
        WORKERBEATS_SUBTREE = ZK_SEPERATOR + WORKERBEATS_ROOT;
        BACKPRESSURE_SUBTREE = ZK_SEPERATOR + BACKPRESSURE_ROOT;
        ERRORS_SUBTREE = ZK_SEPERATOR + ERRORS_ROOT;
        BLOBSTORE_SUBTREE = ZK_SEPERATOR + BLOBSTORE_ROOT;
        BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE = ZK_SEPERATOR + BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_ROOT;
        NIMBUSES_SUBTREE = ZK_SEPERATOR + NIMBUSES_ROOT;
        CREDENTIALS_SUBTREE = ZK_SEPERATOR + CREDENTIALS_ROOT;
        LOGCONFIG_SUBTREE = ZK_SEPERATOR + LOGCONFIG_ROOT;
        PROFILERCONFIG_SUBTREE = ZK_SEPERATOR + PROFILERCONFIG_ROOT;
    }

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static final Cluster INSTANCE = new Cluster();
    private static Cluster _instance = INSTANCE;

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     *
     * @param u a Zookeeper instance
     */
    public static void setInstance(Cluster u) {
        _instance = u;
    }

    /**
     * Resets the singleton instance to the default. This is helpful to reset
     * the class to its original functionality when mocking is no longer
     * desired.
     */
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    public static List<ACL> mkTopoOnlyAcls(Map topoConf) throws NoSuchAlgorithmException {
        List<ACL> aclList = null;
        String payload = (String)topoConf.get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD);
        if (Utils.isZkAuthenticationConfiguredStormServer(topoConf)){
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

    public static String errorPath(String stormId, String componentId) throws UnsupportedEncodingException {
        return errorStormRoot(stormId) + ZK_SEPERATOR + URLEncoder.encode(componentId, "UTF-8");
    }

    public static String lastErrorPath(String stormId, String componentId) throws UnsupportedEncodingException {
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

    public static <T> T maybeDeserialize(byte[] serialized, Class<T> clazz){
        if (serialized != null){
            return Utils.deserialize(serialized, clazz);
        }
        return null;
    }

    //Ensures that we only return heartbeats for executors assigned to this worker
    public static Map<ExecutorInfo, ClusterWorkerHeartbeat> convertExecutorBeats(List<ExecutorInfo> executors, ClusterWorkerHeartbeat workerHeartbeat){
        Map<ExecutorInfo, ClusterWorkerHeartbeat> executorWhb = new HashMap<>();
        Map<ExecutorInfo, ExecutorStats> executorStatsMap = workerHeartbeat.get_executor_stats();
        for (ExecutorInfo executor : executors){
            if(executorStatsMap.containsKey(executor)){
                executorWhb.put(executor, workerHeartbeat);
            }
        }
        return executorWhb;
    }

    public  StormClusterState mkStormClusterStateImpl(Object clusterState, List<ACL> acls, ClusterStateContext context) throws Exception{
        return new StormZkClusterState(clusterState, acls, context);
    }
    public static StormClusterState mkStormClusterState(Object clusterState, List<ACL> acls, ClusterStateContext context) throws Exception{
        return _instance.mkStormClusterStateImpl(clusterState, acls, context);
    }
    
    // TO be remove
    public static <K, V> HashMap<V, List<K>> reverseMap(Map<K, V> map) {
        HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
        if (map == null) {
            return rtn;
        }
        for (Map.Entry<K, V> entry : map.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            List<K> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<K>();
                rtn.put(entry.getValue(), list);
            }
            list.add(key);
        }
        return rtn;
    }
}
