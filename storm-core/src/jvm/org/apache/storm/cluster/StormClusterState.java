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
import org.apache.storm.generated.*;
import org.apache.storm.nimbus.NimbusInfo;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

public interface StormClusterState {
    public List<String> assignments(IFn callback);

    public Assignment assignmentInfo(String stormId, IFn callback);

    public APersistentMap assignmentInfoWithVersion(String stormId, IFn callback);

    public Integer assignmentVersion(String stormId, IFn callback) throws Exception;

    // returns key information under /storm/blobstore/key
    public List<String> blobstoreInfo(String blobKey);

    // returns list of nimbus summaries stored under /stormroot/nimbuses/<nimbus-ids> -> <data>
    public List nimbuses();

    // adds the NimbusSummary to /stormroot/nimbuses/nimbus-id
    public void addNimbusHost(String nimbusId, NimbusSummary nimbusSummary);

    public List<String> activeStorms();

    public StormBase stormBase(String stormId, IFn callback);

    public ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, Long port);

    public List<ProfileRequest> getWorkerProfileRequests(String stormId, NodeInfo nodeInfo, boolean isThrift);

    public List<ProfileRequest> getTopologyProfileRequests(String stormId, boolean isThrift);

    public void setWorkerProfileRequest(String stormId, ProfileRequest profileRequest);

    public void deleteTopologyProfileRequests(String stormId, ProfileRequest profileRequest);

    public Map<ExecutorInfo, ClusterWorkerHeartbeat> executorBeats(String stormId, Map<List<Long>, NodeInfo> executorNodePort);

    public List<String> supervisors(IFn callback);

    public SupervisorInfo supervisorInfo(String supervisorId); // returns nil if doesn't exist

    public void setupHeatbeats(String stormId);

    public void teardownHeartbeats(String stormId);

    public void teardownTopologyErrors(String stormId);

    public List<String> heartbeatStorms();

    public List<String> errorTopologies();

    public void setTopologyLogConfig(String stormId, LogConfig logConfig);

    public LogConfig topologyLogConfig(String stormId, IFn cb);

    public void workerHeartbeat(String stormId, String node, Long port, ClusterWorkerHeartbeat info);

    public void removeWorkerHeartbeat(String stormId, String node, Long port);

    public void supervisorHeartbeat(String supervisorId, SupervisorInfo info);

    public void workerBackpressure(String stormId, String node, Long port, boolean on);

    public boolean topologyBackpressure(String stormId, IFn callback);

    public void setupBackpressure(String stormId);

    public void removeWorkerBackpressure(String stormId, String node, Long port);

    public void activateStorm(String stormId, StormBase stormBase);

    public void updateStorm(String stormId, StormBase newElems);

    public void removeStormBase(String stormId);

    public void setAssignment(String stormId, Assignment info);

    // sets up information related to key consisting of nimbus
    // host:port and version info of the blob
    public void setupBlobstore(String key, NimbusInfo nimbusInfo, Integer versionInfo);

    public List<String> activeKeys();

    public List<String> blobstore(IFn callback);

    public void removeStorm(String stormId);

    public void removeBlobstoreKey(String blobKey);

    public void removeKeyVersion(String blobKey);

    public void reportError(String stormId, String componentId, String node, Integer port, String error);

    public List<ErrorInfo> errors(String stormId, String componentId);

    public ErrorInfo lastError(String stormId, String componentId);

    public void setCredentials(String stormId, Credentials creds, Map topoConf) throws NoSuchAlgorithmException;

    public Credentials credentials(String stormId, IFn callback);

    public void disconnect();

}
