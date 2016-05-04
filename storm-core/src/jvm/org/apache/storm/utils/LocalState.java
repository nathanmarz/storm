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
package org.apache.storm.utils;

import org.apache.commons.io.FileUtils;
import org.apache.storm.generated.LSApprovedWorkers;
import org.apache.storm.generated.LSSupervisorAssignments;
import org.apache.storm.generated.LSSupervisorId;
import org.apache.storm.generated.LSTopoHistory;
import org.apache.storm.generated.LSTopoHistoryList;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.LocalStateData;
import org.apache.storm.generated.ThriftSerializedObject;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple, durable, atomic K/V database. *Very inefficient*, should only be used for occasional reads/writes.
 * Every read/write hits disk.
 */
public class LocalState {
    public static final Logger LOG = LoggerFactory.getLogger(LocalState.class);
    public static final String LS_WORKER_HEARTBEAT = "worker-heartbeat";
    public static final String LS_ID = "supervisor-id";
    public static final String LS_LOCAL_ASSIGNMENTS = "local-assignments";
    public static final String LS_APPROVED_WORKERS = "approved-workers";
    public static final String LS_TOPO_HISTORY = "topo-hist";
    private VersionedStore _vs;
    
    public LocalState(String backingDir) throws IOException {
        LOG.debug("New Local State for {}", backingDir);
        _vs = new VersionedStore(backingDir);
    }

    public synchronized Map<String, TBase> snapshot() {
        int attempts = 0;
        while(true) {
            try {
                return deserializeLatestVersion();
            } catch (Exception e) {
                attempts++;
                if (attempts >= 10) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private Map<String, TBase> deserializeLatestVersion() throws IOException {
        Map<String, TBase> result = new HashMap<>();
        TDeserializer td = new TDeserializer();
        for (Map.Entry<String, ThriftSerializedObject> ent: partialDeserializeLatestVersion(td).entrySet()) {
            result.put(ent.getKey(), deserialize(ent.getValue(), td));
        }
        return result;
    }

    private TBase deserialize(ThriftSerializedObject obj, TDeserializer td) {
        try {
            Class<?> clazz = Class.forName(obj.get_name());
            TBase instance = (TBase) clazz.newInstance();
            td.deserialize(instance, obj.get_bits());
            return instance;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, ThriftSerializedObject> partialDeserializeLatestVersion(TDeserializer td) {
        try {
            String latestPath = _vs.mostRecentVersionPath();
            Map<String, ThriftSerializedObject> result = new HashMap<>();
            if (latestPath != null) {
                byte[] serialized = FileUtils.readFileToByteArray(new File(latestPath));
                if (serialized.length == 0) {
                    LOG.warn("LocalState file '{}' contained no data, resetting state", latestPath);
                } else {
                    if (td == null) {
                        td = new TDeserializer();
                    }
                    LocalStateData data = new LocalStateData();
                    td.deserialize(data, serialized);
                    result = data.get_serialized_parts();
                }
            }
            return result;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized Map<String, ThriftSerializedObject> partialSnapshot(TDeserializer td) {
        int attempts = 0;
        while(true) {
            try {
                return partialDeserializeLatestVersion(td);
            } catch (Exception e) {
                attempts++;
                if (attempts >= 10) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public TBase get(String key) {
        TDeserializer td = new TDeserializer();
        Map<String, ThriftSerializedObject> partial = partialSnapshot(td);
        ThriftSerializedObject tso = partial.get(key);
        TBase ret = null;
        if (tso != null) {
            ret = deserialize(tso, td);
        }
        return ret;
    }
    
    public void put(String key, TBase val) {
        put(key, val, true);
    }

    public synchronized void put(String key, TBase val, boolean cleanup) {
        Map<String, ThriftSerializedObject> curr = partialSnapshot(null);
        TSerializer ser = new TSerializer();
        curr.put(key, serialize(val, ser));
        persistInternal(curr, ser, cleanup);
    }

    public void remove(String key) {
        remove(key, true);
    }

    public synchronized void remove(String key, boolean cleanup) {
        Map<String, ThriftSerializedObject> curr = partialSnapshot(null);
        curr.remove(key);
        persistInternal(curr, null, cleanup);
    }

    public synchronized void cleanup(int keepVersions) throws IOException {
        _vs.cleanup(keepVersions);
    }

    public List<LSTopoHistory> getTopoHistoryList() {
        LSTopoHistoryList lsTopoHistoryListWrapper = (LSTopoHistoryList) get(LS_TOPO_HISTORY);
        if (null != lsTopoHistoryListWrapper) {
            return lsTopoHistoryListWrapper.get_topo_history();
        }
        return null;
    }

    /**
     * Remove topologies from local state which are older than cutOffAge.
     * @param cutOffAge
     */
    public void filterOldTopologies(long cutOffAge) {
        LSTopoHistoryList lsTopoHistoryListWrapper = (LSTopoHistoryList) get(LS_TOPO_HISTORY);
        List<LSTopoHistory> filteredTopoHistoryList = new ArrayList<>();
        if (null != lsTopoHistoryListWrapper) {
            for (LSTopoHistory topoHistory : lsTopoHistoryListWrapper.get_topo_history()) {
                if (topoHistory.get_time_stamp() > cutOffAge) {
                    filteredTopoHistoryList.add(topoHistory);
                }
            }
        }
        put(LS_TOPO_HISTORY, new LSTopoHistoryList(filteredTopoHistoryList));
    }

    public void addTopologyHistory(LSTopoHistory lsTopoHistory) {
        LSTopoHistoryList lsTopoHistoryListWrapper = (LSTopoHistoryList) get(LS_TOPO_HISTORY);
        List<LSTopoHistory> currentTopoHistoryList = new ArrayList<>();
        if (null != lsTopoHistoryListWrapper) {
            currentTopoHistoryList.addAll(lsTopoHistoryListWrapper.get_topo_history());
        }
        currentTopoHistoryList.add(lsTopoHistory);
        put(LS_TOPO_HISTORY, new LSTopoHistoryList(currentTopoHistoryList));
    }

    public String getSupervisorId() {
        LSSupervisorId lsSupervisorId = (LSSupervisorId) get(LS_ID);
        if (null != lsSupervisorId) {
            return lsSupervisorId.get_supervisor_id();
        }
        return null;
    }

    public void setSupervisorId(String supervisorId) {
        put(LS_ID, new LSSupervisorId(supervisorId));
    }

    public Map<String, Integer> getApprovedWorkers() {
        LSApprovedWorkers lsApprovedWorkers = (LSApprovedWorkers) get(LS_APPROVED_WORKERS);
        if (null != lsApprovedWorkers) {
            return lsApprovedWorkers.get_approved_workers();
        }
        return null;
    }

    public void setApprovedWorkers(Map<String, Integer> approvedWorkers) {
        put(LS_APPROVED_WORKERS, new LSApprovedWorkers(approvedWorkers));
    }

    public LSWorkerHeartbeat getWorkerHeartBeat() {
        return (LSWorkerHeartbeat) get(LS_WORKER_HEARTBEAT);
    }

    public void setWorkerHeartBeat(LSWorkerHeartbeat workerHeartBeat) {
        put(LS_WORKER_HEARTBEAT, workerHeartBeat, false);
    }

    public Map<Integer, LocalAssignment> getLocalAssignmentsMap() {
        LSSupervisorAssignments assignments = (LSSupervisorAssignments) get(LS_LOCAL_ASSIGNMENTS);
        if (null != assignments) {
            return assignments.get_assignments();
        }
        return null;
    }

    public void setLocalAssignmentsMap(Map<Integer, LocalAssignment> localAssignmentMap) {
        put(LS_LOCAL_ASSIGNMENTS, new LSSupervisorAssignments(localAssignmentMap));
    }

    private void persistInternal(Map<String, ThriftSerializedObject> serialized, TSerializer ser, boolean cleanup) {
        try {
            if (ser == null) {
                ser = new TSerializer();
            }
            byte[] toWrite = ser.serialize(new LocalStateData(serialized));

            String newPath = _vs.createVersion();
            File file = new File(newPath);
            FileUtils.writeByteArrayToFile(file, toWrite);
            if (toWrite.length != file.length()) {
                throw new IOException("Tried to serialize " + toWrite.length + 
                        " bytes to " + file.getCanonicalPath() + ", but " +
                        file.length() + " bytes were written.");
            }
            _vs.succeedVersion(newPath);
            if(cleanup) _vs.cleanup(4);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ThriftSerializedObject serialize(TBase o, TSerializer ser) {
        try {
            return new ThriftSerializedObject(o.getClass().getName(), ByteBuffer.wrap(ser.serialize(o)));
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
