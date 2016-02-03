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

import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.storm.callback.Callback;
import org.apache.storm.generated.*;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class StormZkClusterState implements StormClusterState {

    private static Logger LOG = LoggerFactory.getLogger(StormZkClusterState.class);

    private ClusterState clusterState;

    private ConcurrentHashMap<String, IFn> assignmentInfoCallback;
    private ConcurrentHashMap<String, IFn> assignmentInfoWithVersionCallback;
    private ConcurrentHashMap<String, IFn> assignmentVersionCallback;
    private AtomicReference<IFn> supervisorsCallback;
    // we want to reigister a topo directory getChildren callback for all workers of this dir
    private ConcurrentHashMap<String, IFn> backPressureCallback;
    private AtomicReference<IFn> assignmentsCallback;
    private ConcurrentHashMap<String, IFn> stormBaseCallback;
    private AtomicReference<IFn> blobstoreCallback;
    private ConcurrentHashMap<String, IFn> credentialsCallback;
    private ConcurrentHashMap<String, IFn> logConfigCallback;

    private List<ACL> acls;
    private String stateId;
    private boolean solo;

    public StormZkClusterState(Object clusterState, List<ACL> acls, ClusterStateContext context) throws Exception {

        if (clusterState instanceof ClusterState) {
            solo = false;
            this.clusterState = (ClusterState) clusterState;
        } else {

            solo = true;
            this.clusterState = new DistributedClusterState((Map) clusterState, (Map) clusterState, acls, context);
        }

        assignmentInfoCallback = new ConcurrentHashMap<>();
        assignmentInfoWithVersionCallback = new ConcurrentHashMap<>();
        assignmentVersionCallback = new ConcurrentHashMap<>();
        supervisorsCallback = new AtomicReference<>();
        backPressureCallback = new ConcurrentHashMap<>();
        assignmentsCallback = new AtomicReference<>();
        stormBaseCallback = new ConcurrentHashMap<>();
        credentialsCallback = new ConcurrentHashMap<>();
        logConfigCallback = new ConcurrentHashMap<>();
        blobstoreCallback = new AtomicReference<>();

        stateId = this.clusterState.register(new Callback() {

            public <T> Object execute(T... args) {
                if (args == null) {
                    LOG.warn("Input args is null");
                    return null;
                } else if (args.length < 2) {
                    LOG.warn("Input args is invalid, args length:" + args.length);
                    return null;
                }
                String path = (String) args[1];

                List<String> toks = Zookeeper.tokenizePath(path);
                int size = toks.size();
                if (size >= 1) {
                    String params = null;
                    String root = toks.get(0);
                    IFn fn = null;
                    if (root.equals(Cluster.ASSIGNMENTS_ROOT)) {
                        if (size == 1) {
                            // set null and get the old value
                            issueCallback(assignmentsCallback);
                        } else {
                            issueMapCallback(assignmentInfoCallback, toks.get(1));
                            issueMapCallback(assignmentVersionCallback, toks.get(1));
                            issueMapCallback(assignmentInfoWithVersionCallback, toks.get(1));
                        }

                    } else if (root.equals(Cluster.SUPERVISORS_ROOT)) {
                        issueCallback(supervisorsCallback);
                    } else if (root.equals(Cluster.BLOBSTORE_ROOT)) {
                        issueCallback(blobstoreCallback);
                    } else if (root.equals(Cluster.STORMS_ROOT) && size > 1) {
                        issueMapCallback(stormBaseCallback, toks.get(1));
                    } else if (root.equals(Cluster.CREDENTIALS_ROOT) && size > 1) {
                        issueMapCallback(credentialsCallback, toks.get(1));
                    } else if (root.equals(Cluster.LOGCONFIG_ROOT) && size > 1) {
                        issueMapCallback(logConfigCallback, toks.get(1));
                    } else if (root.equals(Cluster.BACKPRESSURE_ROOT) && size > 1) {
                        issueMapCallback(logConfigCallback, toks.get(1));
                    } else {
                        LOG.error("{} Unknown callback for subtree {}", new RuntimeException("Unknown callback for this path"), path);
                        Runtime.getRuntime().exit(30);
                    }

                }

                return null;
            }

        });

        String[] pathlist = { Cluster.ASSIGNMENTS_SUBTREE, Cluster.STORMS_SUBTREE, Cluster.SUPERVISORS_SUBTREE, Cluster.WORKERBEATS_SUBTREE,
                Cluster.ERRORS_SUBTREE, Cluster.BLOBSTORE_SUBTREE, Cluster.NIMBUSES_SUBTREE, Cluster.LOGCONFIG_SUBTREE };
        for (String path : pathlist) {
            this.clusterState.mkdirs(path, acls);
        }

    }

    protected void issueCallback(AtomicReference<IFn> cb) {
        IFn callback = cb.getAndSet(null);
        if (callback != null)
            callback.invoke();
    }

    protected void issueMapCallback(ConcurrentHashMap<String, IFn> callbackConcurrentHashMap, String key) {
        IFn callback = callbackConcurrentHashMap.remove(key);
        if (callback != null)
            callback.invoke();
    }

    @Override
    public List<String> assignments(IFn callback) {
        if (callback != null) {
            assignmentsCallback.set(callback);
        }
        return clusterState.get_children(Cluster.ASSIGNMENTS_SUBTREE, callback != null);
    }

    @Override
    public Assignment assignmentInfo(String stormId, IFn callback) {
        if (callback != null) {
            assignmentInfoCallback.put(stormId, callback);
        }
        byte[] serialized = clusterState.get_data(Cluster.assignmentPath(stormId), callback != null);
        return Cluster.maybeDeserialize(serialized, Assignment.class);
    }

    @Override
    public APersistentMap assignmentInfoWithVersion(String stormId, IFn callback) {
        if (callback != null) {
            assignmentInfoWithVersionCallback.put(stormId, callback);
        }
        APersistentMap aPersistentMap = clusterState.get_data_with_version(Cluster.assignmentPath(stormId), callback != null);
        Assignment assignment = Cluster.maybeDeserialize((byte[]) aPersistentMap.get("data"), Assignment.class);
        Integer version = (Integer) aPersistentMap.get("version");
        APersistentMap map = new PersistentArrayMap(new Object[] { RT.keyword(null, "data"), assignment, RT.keyword(null, "version"), version });
        return map;
    }

    @Override
    public Integer assignmentVersion(String stormId, IFn callback) throws Exception {
        if (callback != null) {
            assignmentVersionCallback.put(stormId, callback);
        }
        return clusterState.get_version(Cluster.assignmentPath(stormId), callback != null);
    }

    // blobstore state
    @Override
    public List<String> blobstoreInfo(String blobKey) {
        String path = Cluster.blobstorePath(blobKey);
        clusterState.sync_path(path);
        return clusterState.get_children(path, false);
    }

    @Override
    public List nimbuses() {
        List<NimbusSummary> nimbusSummaries = new ArrayList<>();
        List<String> nimbusIds = clusterState.get_children(Cluster.NIMBUSES_SUBTREE, false);
        for (String nimbusId : nimbusIds) {
            byte[] serialized = clusterState.get_data(Cluster.nimbusPath(nimbusId), false);
            NimbusSummary nimbusSummary = Cluster.maybeDeserialize(serialized, NimbusSummary.class);
            nimbusSummaries.add(nimbusSummary);
        }
        return nimbusSummaries;
    }

    @Override
    public void addNimbusHost(final String nimbusId, final NimbusSummary nimbusSummary) {
        // explicit delete for ephmeral node to ensure this session creates the entry.
        clusterState.delete_node(Cluster.nimbusPath(nimbusId));
        clusterState.add_listener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                LOG.info("Connection state listener invoked, zookeeper connection state has changed to {}", connectionState);
                if (connectionState.equals(ConnectionState.RECONNECTED)) {
                    LOG.info("Connection state has changed to reconnected so setting nimbuses entry one more time");
                    clusterState.set_ephemeral_node(Cluster.nimbusPath(nimbusId), Utils.serialize(nimbusSummary), acls);
                }

            }
        });

        clusterState.set_ephemeral_node(Cluster.nimbusPath(nimbusId), Utils.serialize(nimbusSummary), acls);
    }

    @Override
    public List<String> activeStorms() {
        return clusterState.get_children(Cluster.STORMS_SUBTREE, false);
    }

    @Override
    public StormBase stormBase(String stormId, IFn callback) {
        if (callback != null) {
            stormBaseCallback.put(stormId, callback);
        }
        return Cluster.maybeDeserialize(clusterState.get_data(Cluster.stormPath(stormId), callback != null), StormBase.class);
    }

    @Override
    public ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, Long port) {
        byte[] bytes = clusterState.get_worker_hb(Cluster.workerbeatPath(stormId, node, port), false);
        if (bytes != null) {
            return Cluster.maybeDeserialize(bytes, ClusterWorkerHeartbeat.class);
        }
        return null;
    }

    @Override
    public List<ProfileRequest> getWorkerProfileRequests(String stormId, NodeInfo nodeInfo, boolean isThrift) {
        List<ProfileRequest> requests = new ArrayList<>();
        List<ProfileRequest> profileRequests = getTopologyProfileRequests(stormId, isThrift);
        for (ProfileRequest profileRequest : profileRequests) {
            NodeInfo nodeInfo1 = profileRequest.get_nodeInfo();
            if (nodeInfo1.equals(nodeInfo))
                requests.add(profileRequest);
        }
        return requests;
    }

    @Override
    public List<ProfileRequest> getTopologyProfileRequests(String stormId, boolean isThrift) {
        List<ProfileRequest> profileRequests = new ArrayList<>();
        String path = Cluster.profilerConfigPath(stormId);
        if (clusterState.node_exists(path, false)) {
            List<String> strs = clusterState.get_children(path, false);
            for (String str : strs) {
                String childPath = path + Cluster.ZK_SEPERATOR + str;
                byte[] raw = clusterState.get_data(childPath, false);
                ProfileRequest request = Cluster.maybeDeserialize(raw, ProfileRequest.class);
                if (request != null)
                    profileRequests.add(request);
            }
        }
        return profileRequests;
    }

    @Override
    public void setWorkerProfileRequest(String stormId, ProfileRequest profileRequest) {
        ProfileAction profileAction = profileRequest.get_action();
        String host = profileRequest.get_nodeInfo().get_node();
        Long port = profileRequest.get_nodeInfo().get_port_iterator().next();
        String path = Cluster.profilerConfigPath(stormId, host, port, profileAction);
        clusterState.set_data(path, Utils.serialize(profileRequest), acls);
    }

    @Override
    public void deleteTopologyProfileRequests(String stormId, ProfileRequest profileRequest) {
        ProfileAction profileAction = profileRequest.get_action();
        String host = profileRequest.get_nodeInfo().get_node();
        Long port = profileRequest.get_nodeInfo().get_port_iterator().next();
        String path = Cluster.profilerConfigPath(stormId, host, port, profileAction);
        clusterState.delete_node(path);
    }

    // need to take executor->node+port in explicitly so that we don't run into a situation where a
    // long dead worker with a skewed clock overrides all the timestamps. By only checking heartbeats
    // with an assigned node+port, and only reading executors from that heartbeat that are actually assigned,
    // we avoid situations like that
    @Override
    public Map<ExecutorInfo, ClusterWorkerHeartbeat> executorBeats(String stormId, Map<List<Long>, NodeInfo> executorNodePort) {
        Map<ExecutorInfo, ClusterWorkerHeartbeat> executorWhbs = new HashMap<>();

        LOG.info(executorNodePort.toString());
        Map<NodeInfo, List<List<Long>>> nodePortExecutors = Cluster.reverseMap(executorNodePort);
        LOG.info(nodePortExecutors.toString());

        for (Map.Entry<NodeInfo, List<List<Long>>> entry : nodePortExecutors.entrySet()) {

            String node = entry.getKey().get_node();
            Long port = entry.getKey().get_port_iterator().next();
            ClusterWorkerHeartbeat whb = getWorkerHeartbeat(stormId, node, port);
            List<ExecutorInfo> executorInfoList = new ArrayList<>();
            for (List<Long> list : entry.getValue()) {
                executorInfoList.add(new ExecutorInfo(list.get(0).intValue(), list.get(list.size() - 1).intValue()));
            }
            executorWhbs.putAll(Cluster.convertExecutorBeats(executorInfoList, whb));
        }
        return executorWhbs;
    }

    @Override
    public List<String> supervisors(IFn callback) {
        if (callback != null) {
            supervisorsCallback.set(callback);
        }
        return clusterState.get_children(Cluster.SUPERVISORS_SUBTREE, callback != null);
    }

    @Override
    public SupervisorInfo supervisorInfo(String supervisorId) {
        String path = Cluster.supervisorPath(supervisorId);
        return Cluster.maybeDeserialize(clusterState.get_data(path, false), SupervisorInfo.class);
    }

    @Override
    public void setupHeatbeats(String stormId) {
        clusterState.mkdirs(Cluster.workerbeatStormRoot(stormId), acls);
    }

    @Override
    public void teardownHeartbeats(String stormId) {
        try {
            clusterState.delete_worker_hb(Cluster.workerbeatStormRoot(stormId));
        } catch (Exception e) {
            if (Zookeeper.exceptionCause(KeeperException.class, e)) {
                // do nothing
                LOG.warn("Could not teardown heartbeats for {}.", stormId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void teardownTopologyErrors(String stormId) {
        try {
            clusterState.delete_node(Cluster.errorStormRoot(stormId));
        } catch (Exception e) {
            if (Zookeeper.exceptionCause(KeeperException.class, e)) {
                // do nothing
                LOG.warn("Could not teardown errors for {}.", stormId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public List<String> heartbeatStorms() {
        return clusterState.get_worker_hb_children(Cluster.WORKERBEATS_SUBTREE, false);
    }

    @Override
    public List<String> errorTopologies() {
        return clusterState.get_children(Cluster.ERRORS_SUBTREE, false);
    }

    @Override
    public void setTopologyLogConfig(String stormId, LogConfig logConfig) {
        clusterState.set_data(Cluster.logConfigPath(stormId), Utils.serialize(logConfig), acls);
    }

    @Override
    public LogConfig topologyLogConfig(String stormId, IFn cb) {
        String path = Cluster.logConfigPath(stormId);
        return Cluster.maybeDeserialize(clusterState.get_data(path, cb != null), LogConfig.class);
    }

    @Override
    public void workerHeartbeat(String stormId, String node, Long port, ClusterWorkerHeartbeat info) {
        if (info != null) {
            String path = Cluster.workerbeatPath(stormId, node, port);
            clusterState.set_worker_hb(path, Utils.serialize(info), acls);
        }
    }

    @Override
    public void removeWorkerHeartbeat(String stormId, String node, Long port) {
        String path = Cluster.workerbeatPath(stormId, node, port);
        clusterState.delete_worker_hb(path);
    }

    @Override
    public void supervisorHeartbeat(String supervisorId, SupervisorInfo info) {
        String path = Cluster.supervisorPath(supervisorId);
        clusterState.set_ephemeral_node(path, Utils.serialize(info), acls);
    }

    // if znode exists and to be not on?, delete; if exists and on?, do nothing;
    // if not exists and to be on?, create; if not exists and not on?, do nothing;
    @Override
    public void workerBackpressure(String stormId, String node, Long port, boolean on) {
        String path = Cluster.backpressurePath(stormId, node, port);
        boolean existed = clusterState.node_exists(path, false);
        if (existed) {
            if (on == false)
                clusterState.delete_node(path);

        } else {
            if (on == true) {
                clusterState.set_ephemeral_node(path, null, acls);
            }
        }
    }

    // if the backpresure/storm-id dir is empty, this topology has throttle-on, otherwise not.
    @Override
    public boolean topologyBackpressure(String stormId, IFn callback) {
        if (callback != null) {
            backPressureCallback.put(stormId, callback);
        }
        String path = Cluster.backpressureStormRoot(stormId);
        List<String> childrens = clusterState.get_children(path, callback != null);
        return childrens.size() > 0;

    }

    @Override
    public void setupBackpressure(String stormId) {
        clusterState.mkdirs(Cluster.backpressureStormRoot(stormId), acls);
    }

    @Override
    public void removeWorkerBackpressure(String stormId, String node, Long port) {
        clusterState.delete_node(Cluster.backpressurePath(stormId, node, port));
    }

    @Override
    public void activateStorm(String stormId, StormBase stormBase) {
        String path = Cluster.stormPath(stormId);
        clusterState.set_data(path, Utils.serialize(stormBase), acls);
    }

    // maybe exit some questions for updateStorm
    @Override
    public void updateStorm(String stormId, StormBase newElems) {

        StormBase stormBase = stormBase(stormId, null);
        if (stormBase.get_component_executors() != null) {

            Map<String, Integer> newComponentExecutors = new HashMap<>();
            Map<String, Integer> componentExecutors = newElems.get_component_executors();
            //componentExecutors maybe be APersistentMap, which don't support put
            for (Map.Entry<String, Integer> entry : componentExecutors.entrySet()) {
                    newComponentExecutors.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, Integer> entry : stormBase.get_component_executors().entrySet()) {
                if (!componentExecutors.containsKey(entry.getKey())) {
                    newComponentExecutors.put(entry.getKey(), entry.getValue());
                }
            }
            if (newComponentExecutors.size() > 0)
                newElems.set_component_executors(newComponentExecutors);
        }

        Map<String, DebugOptions> ComponentDebug = new HashMap<>();
        Map<String, DebugOptions> oldComponentDebug = stormBase.get_component_debug();

        Map<String, DebugOptions> newComponentDebug = newElems.get_component_debug();

        Set<String> debugOptionsKeys = oldComponentDebug.keySet();
        debugOptionsKeys.addAll(newComponentDebug.keySet());
        for (String key : debugOptionsKeys) {
            boolean enable = false;
            double samplingpct = 0;
            if (oldComponentDebug.containsKey(key)) {
                enable = oldComponentDebug.get(key).is_enable();
                samplingpct = oldComponentDebug.get(key).get_samplingpct();
            }
            if (newComponentDebug.containsKey(key)) {
                enable = newComponentDebug.get(key).is_enable();
                samplingpct += newComponentDebug.get(key).get_samplingpct();
            }
            DebugOptions debugOptions = new DebugOptions();
            debugOptions.set_enable(enable);
            debugOptions.set_samplingpct(samplingpct);
            ComponentDebug.put(key, debugOptions);
        }
        if (ComponentDebug.size() > 0) {
            newElems.set_component_debug(ComponentDebug);
        }


        if (StringUtils.isBlank(newElems.get_name())) {
            newElems.set_name(stormBase.get_name());
        }
        if (newElems.get_status() == null){
            newElems.set_status(stormBase.get_status());
        }
        if (newElems.get_num_workers() == 0){
            newElems.set_num_workers(stormBase.get_num_workers());
        }
        if (newElems.get_launch_time_secs() == 0) {
            newElems.set_launch_time_secs(stormBase.get_launch_time_secs());
        }
        if (StringUtils.isBlank(newElems.get_owner())) {
            newElems.set_owner(stormBase.get_owner());
        }
        if (newElems.get_topology_action_options() == null) {
            newElems.set_topology_action_options(stormBase.get_topology_action_options());
        }
        if (newElems.get_status() == null) {
            newElems.set_status(stormBase.get_status());
        }
        clusterState.set_data(Cluster.stormPath(stormId), Utils.serialize(newElems), acls);
    }

    @Override
    public void removeStormBase(String stormId) {
        clusterState.delete_node(Cluster.stormPath(stormId));
    }

    @Override
    public void setAssignment(String stormId, Assignment info) {
        clusterState.set_data(Cluster.assignmentPath(stormId), Utils.serialize(info), acls);
    }

    @Override
    public void setupBlobstore(String key, NimbusInfo nimbusInfo, Integer versionInfo) {
        String path = Cluster.blobstorePath(key) + Cluster.ZK_SEPERATOR + nimbusInfo.toHostPortString() + "-" + versionInfo;
        LOG.info("set-path: {}", path);
        clusterState.mkdirs(Cluster.blobstorePath(key), acls);
        clusterState.delete_node_blobstore(Cluster.blobstorePath(key), nimbusInfo.toHostPortString());
        clusterState.set_ephemeral_node(path, null, acls);
    }

    @Override
    public List<String> activeKeys() {
        return clusterState.get_children(Cluster.BLOBSTORE_SUBTREE, false);
    }

    // blobstore state
    @Override
    public List<String> blobstore(IFn callback) {
        if (callback != null) {
            blobstoreCallback.set(callback);
        }
        clusterState.sync_path(Cluster.BLOBSTORE_SUBTREE);
        return clusterState.get_children(Cluster.BLOBSTORE_SUBTREE, callback != null);

    }

    @Override
    public void removeStorm(String stormId) {
        clusterState.delete_node(Cluster.assignmentPath(stormId));
        clusterState.delete_node(Cluster.credentialsPath(stormId));
        clusterState.delete_node(Cluster.logConfigPath(stormId));
        clusterState.delete_node(Cluster.profilerConfigPath(stormId));
        removeStormBase(stormId);
    }

    @Override
    public void removeBlobstoreKey(String blobKey) {
        LOG.debug("remove key {}", blobKey);
        clusterState.delete_node(Cluster.blobstorePath(blobKey));
    }

    @Override
    public void removeKeyVersion(String blobKey) {
        clusterState.delete_node(Cluster.blobstoreMaxKeySequenceNumberPath(blobKey));
    }

    @Override
    public void reportError(String stormId, String componentId, String node, Integer port, String error) {

        try {
            String path = Cluster.errorPath(stormId, componentId);
            String lastErrorPath = Cluster.lastErrorPath(stormId, componentId);
            ErrorInfo errorInfo = new ErrorInfo(error, Time.currentTimeSecs());
            errorInfo.set_host(node);
            errorInfo.set_port(port.intValue());
            byte[] serData = Utils.serialize(errorInfo);
            clusterState.mkdirs(path, acls);
            clusterState.create_sequential(path + Cluster.ZK_SEPERATOR + "e", serData, acls);
            clusterState.set_data(lastErrorPath, serData, acls);
            List<String> childrens = clusterState.get_children(path, false);

            Collections.sort(childrens);

            while (childrens.size() >= 10) {
                clusterState.delete_node(path + Cluster.ZK_SEPERATOR + childrens.remove(0));
            }
        } catch (UnsupportedEncodingException e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    @Override
    public List<ErrorInfo> errors(String stormId, String componentId) {
        List<ErrorInfo> errorInfos = new ArrayList<>();
        try {
            String path = Cluster.errorPath(stormId, componentId);
            if (clusterState.node_exists(path, false)) {
                List<String> childrens = clusterState.get_children(path, false);
                for (String child : childrens) {
                    String childPath = path + Cluster.ZK_SEPERATOR + child;
                    ErrorInfo errorInfo = Cluster.maybeDeserialize(clusterState.get_data(childPath, false), ErrorInfo.class);
                    if (errorInfo != null)
                        errorInfos.add(errorInfo);
                }
            }
            Collections.sort(errorInfos, new Comparator<ErrorInfo>() {
                public int compare(ErrorInfo arg0, ErrorInfo arg1) {
                    return Integer.compare(arg0.get_error_time_secs(), arg1.get_error_time_secs());
                }
            });
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }

        return errorInfos;
    }

    @Override
    public ErrorInfo lastError(String stormId, String componentId) {
        try {
            String path = Cluster.lastErrorPath(stormId, componentId);
            if (clusterState.node_exists(path, false)) {
                ErrorInfo errorInfo = Cluster.maybeDeserialize(clusterState.get_data(path, false), ErrorInfo.class);
                return errorInfo;
            }
        } catch (UnsupportedEncodingException e) {
            throw Utils.wrapInRuntime(e);
        }
        return null;
    }

    @Override
    public void setCredentials(String stormId, Credentials creds, Map topoConf) throws NoSuchAlgorithmException {
        List<ACL> aclList = Cluster.mkTopoOnlyAcls(topoConf);
        String path = Cluster.credentialsPath(stormId);
        clusterState.set_data(path, Utils.serialize(creds), aclList);

    }

    @Override
    public Credentials credentials(String stormId, IFn callback) {
        if (callback != null) {
            credentialsCallback.put(stormId, callback);
        }
        String path = Cluster.credentialsPath(stormId);
        return Cluster.maybeDeserialize(clusterState.get_data(path, callback != null), Credentials.class);

    }

    @Override
    public void disconnect() {
        clusterState.unregister(stateId);
        if (solo)
            clusterState.close();
    }
}
