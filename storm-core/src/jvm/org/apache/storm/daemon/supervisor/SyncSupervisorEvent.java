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
package org.apache.storm.daemon.supervisor;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.event.EventManager;
import org.apache.storm.generated.*;
import org.apache.storm.localizer.LocalResource;
import org.apache.storm.localizer.LocalizedResource;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.utils.*;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncSupervisorEvent implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SyncSupervisorEvent.class);

    private EventManager syncSupEventManager;
    private EventManager syncProcessManager;
    private IStormClusterState stormClusterState;
    private LocalState localState;
    private SyncProcessEvent syncProcesses;
    private SupervisorData supervisorData;

    public SyncSupervisorEvent(SupervisorData supervisorData, SyncProcessEvent syncProcesses, EventManager syncSupEventManager,
            EventManager syncProcessManager) {

        this.syncProcesses = syncProcesses;
        this.syncSupEventManager = syncSupEventManager;
        this.syncProcessManager = syncProcessManager;
        this.stormClusterState = supervisorData.getStormClusterState();
        this.localState = supervisorData.getLocalState();
        this.supervisorData = supervisorData;
    }

    @Override
    public void run() {
        try {
            Map conf = supervisorData.getConf();
            Runnable syncCallback = new EventManagerPushCallback(this, syncSupEventManager);
            List<String> stormIds = stormClusterState.assignments(syncCallback);
            Map<String, Map<String, Object>> assignmentsSnapshot =
                    getAssignmentsSnapshot(stormClusterState, stormIds, supervisorData.getAssignmentVersions().get(), syncCallback);
            Map<String, List<ProfileRequest>> stormIdToProfilerActions = getProfileActions(stormClusterState, stormIds);

            Set<String> allDownloadedTopologyIds = SupervisorUtils.readDownLoadedStormIds(conf);
            Map<String, String> stormcodeMap = readStormCodeLocations(assignmentsSnapshot);
            Map<Integer, LocalAssignment> existingAssignment = localState.getLocalAssignmentsMap();
            if (existingAssignment == null) {
                existingAssignment = new HashMap<>();
            }

            Map<Integer, LocalAssignment> allAssignment =
                    readAssignments(assignmentsSnapshot, existingAssignment, supervisorData.getAssignmentId(), supervisorData.getSyncRetry());

            Map<Integer, LocalAssignment> newAssignment = new HashMap<>();
            Set<String> assignedStormIds = new HashSet<>();

            for (Map.Entry<Integer, LocalAssignment> entry : allAssignment.entrySet()) {
                if (supervisorData.getiSupervisor().confirmAssigned(entry.getKey())) {
                    newAssignment.put(entry.getKey(), entry.getValue());
                    assignedStormIds.add(entry.getValue().get_topology_id());
                }
            }

            Set<String> crashedStormIds = verifyDownloadedFiles(conf, supervisorData.getLocalizer(), assignedStormIds, allDownloadedTopologyIds);
            Set<String> downloadedStormIds = new HashSet<>();
            downloadedStormIds.addAll(allDownloadedTopologyIds);
            downloadedStormIds.removeAll(crashedStormIds);

            LOG.debug("Synchronizing supervisor");
            LOG.debug("Storm code map: {}", stormcodeMap);
            LOG.debug("All assignment: {}", allAssignment);
            LOG.debug("New assignment: {}", newAssignment);
            LOG.debug("Assigned Storm Ids {}", assignedStormIds);
            LOG.debug("All Downloaded Ids {}", allDownloadedTopologyIds);
            LOG.debug("Checked Downloaded Ids {}", crashedStormIds);
            LOG.debug("Downloaded Ids {}", downloadedStormIds);
            LOG.debug("Storm Ids Profiler Actions {}", stormIdToProfilerActions);

            // download code first
            // This might take awhile
            // - should this be done separately from usual monitoring?
            // should we only download when topology is assigned to this supervisor?
            for (Map.Entry<String, String> entry : stormcodeMap.entrySet()) {
                String stormId = entry.getKey();
                if (!downloadedStormIds.contains(stormId) && assignedStormIds.contains(stormId)) {
                    LOG.info("Downloading code for storm id {}.", stormId);
                    try {
                        downloadStormCode(conf, stormId, entry.getValue(), supervisorData.getLocalizer());
                    } catch (Exception e) {
                        if (Utils.exceptionCauseIsInstanceOf(NimbusLeaderNotFoundException.class, e)) {
                            LOG.warn("Nimbus leader was not available.", e);
                        } else if (Utils.exceptionCauseIsInstanceOf(TTransportException.class, e)) {
                            LOG.warn("There was a connection problem with nimbus.", e);
                        } else {
                            throw e;
                        }
                    }
                    LOG.info("Finished downloading code for storm id {}", stormId);
                }
            }

            LOG.debug("Writing new assignment {}", newAssignment);

            Set<Integer> killWorkers = new HashSet<>();
            killWorkers.addAll(existingAssignment.keySet());
            killWorkers.removeAll(newAssignment.keySet());
            for (Integer port : killWorkers) {
                supervisorData.getiSupervisor().killedWorker(port);
            }

            killExistingWorkersWithChangeInComponents(supervisorData, existingAssignment, newAssignment);

            supervisorData.getiSupervisor().assigned(newAssignment.keySet());
            localState.setLocalAssignmentsMap(newAssignment);
            supervisorData.setAssignmentVersions(assignmentsSnapshot);
            supervisorData.setStormIdToProfilerActions(stormIdToProfilerActions);

            Map<Long, LocalAssignment> convertNewAssignment = new HashMap<>();
            for (Map.Entry<Integer, LocalAssignment> entry : newAssignment.entrySet()) {
                convertNewAssignment.put(entry.getKey().longValue(), entry.getValue());
            }
            supervisorData.setCurrAssignment(convertNewAssignment);
            // remove any downloaded code that's no longer assigned or active
            // important that this happens after setting the local assignment so that
            // synchronize-supervisor doesn't try to launch workers for which the
            // resources don't exist
            if (Utils.isOnWindows()) {
                shutdownDisallowedWorkers();
            }
            for (String stormId : allDownloadedTopologyIds) {
                if (!stormcodeMap.containsKey(stormId)) {
                    LOG.info("Removing code for storm id {}.", stormId);
                    rmTopoFiles(conf, stormId, supervisorData.getLocalizer(), true);
                }
            }
            syncProcessManager.add(syncProcesses);
        } catch (Exception e) {
            LOG.error("Failed to Sync Supervisor", e);
            throw new RuntimeException(e);
        }

    }

    private void killExistingWorkersWithChangeInComponents(SupervisorData supervisorData, Map<Integer, LocalAssignment> existingAssignment,
            Map<Integer, LocalAssignment> newAssignment) throws Exception {
        int now = Time.currentTimeSecs();
        Map<String, StateHeartbeat> workerIdHbstate = syncProcesses.getLocalWorkerStats(supervisorData, existingAssignment, now);
        Map<Integer, String> vaildPortToWorkerIds = new HashMap<>();
        for (Map.Entry<String, StateHeartbeat> entry : workerIdHbstate.entrySet()) {
            String workerId = entry.getKey();
            StateHeartbeat stateHeartbeat = entry.getValue();
            if (stateHeartbeat != null && stateHeartbeat.getState() == State.VALID) {
                vaildPortToWorkerIds.put(stateHeartbeat.getHeartbeat().get_port(), workerId);
            }
        }

        Map<Integer, LocalAssignment> intersectAssignment = new HashMap<>();
        for (Map.Entry<Integer, LocalAssignment> entry : newAssignment.entrySet()) {
            Integer port = entry.getKey();
            if (existingAssignment.containsKey(port)) {
                intersectAssignment.put(port, entry.getValue());
            }
        }

        for (Integer port : intersectAssignment.keySet()) {
            List<ExecutorInfo> existExecutors = existingAssignment.get(port).get_executors();
            List<ExecutorInfo> newExecutors = newAssignment.get(port).get_executors();
            Set<ExecutorInfo> setExitExecutors = new HashSet<>(existExecutors);
            Set<ExecutorInfo>  setNewExecutors = new HashSet<>(newExecutors);
            if (!setExitExecutors.equals(setNewExecutors)){
                syncProcesses.killWorker(supervisorData, supervisorData.getWorkerManager(), vaildPortToWorkerIds.get(port));
            }
        }
    }

    protected Map<String, Map<String, Object>> getAssignmentsSnapshot(IStormClusterState stormClusterState, List<String> stormIds,
            Map<String, Map<String, Object>> localAssignmentVersion, Runnable callback) throws Exception {
        Map<String, Map<String, Object>> updateAssignmentVersion = new HashMap<>();
        for (String stormId : stormIds) {
            Integer recordedVersion = -1;
            Integer version = stormClusterState.assignmentVersion(stormId, callback);
            if (localAssignmentVersion.containsKey(stormId) && localAssignmentVersion.get(stormId) != null) {
                recordedVersion = (Integer) localAssignmentVersion.get(stormId).get(IStateStorage.VERSION);
            }
            if (version == null) {
                // ignore
            } else if (version == recordedVersion) {
                updateAssignmentVersion.put(stormId, localAssignmentVersion.get(stormId));
            } else {
                Map<String, Object> assignmentVersion = (Map<String, Object>) stormClusterState.assignmentInfoWithVersion(stormId, callback);
                updateAssignmentVersion.put(stormId, assignmentVersion);
            }
        }
        return updateAssignmentVersion;
    }

    protected Map<String, List<ProfileRequest>> getProfileActions(IStormClusterState stormClusterState, List<String> stormIds) throws Exception {
        Map<String, List<ProfileRequest>> ret = new HashMap<String, List<ProfileRequest>>();
        for (String stormId : stormIds) {
            List<ProfileRequest> profileRequests = stormClusterState.getTopologyProfileRequests(stormId);
            ret.put(stormId, profileRequests);
        }
        return ret;
    }

    protected Map<String, String> readStormCodeLocations(Map<String, Map<String, Object>> assignmentsSnapshot) {
        Map<String, String> stormcodeMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : assignmentsSnapshot.entrySet()) {
            Assignment assignment = (Assignment) (entry.getValue().get(IStateStorage.DATA));
            if (assignment != null) {
                stormcodeMap.put(entry.getKey(), assignment.get_master_code_dir());
            }
        }
        return stormcodeMap;
    }

    /**
     * Remove a reference to a blob when its no longer needed.
     * 
     * @param localizer
     * @param stormId
     * @param conf
     */
    protected void removeBlobReferences(Localizer localizer, String stormId, Map conf) throws Exception {
        Map stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) stormConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        String user = (String) stormConf.get(Config.TOPOLOGY_SUBMITTER_USER);
        String topoName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        if (blobstoreMap != null) {
            for (Map.Entry<String, Map<String, Object>> entry : blobstoreMap.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> blobInfo = entry.getValue();
                localizer.removeBlobReference(key, user, topoName, SupervisorUtils.shouldUncompressBlob(blobInfo));
            }
        }
    }

    protected void rmTopoFiles(Map conf, String stormId, Localizer localizer, boolean isrmBlobRefs) throws IOException {
        String path = ConfigUtils.supervisorStormDistRoot(conf, stormId);
        try {
            if (isrmBlobRefs) {
                removeBlobReferences(localizer, stormId, conf);
            }
            if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
                SupervisorUtils.rmrAsUser(conf, stormId, path);
            } else {
                Utils.forceDelete(ConfigUtils.supervisorStormDistRoot(conf, stormId));
            }
        } catch (Exception e) {
            LOG.info("Exception removing: {} ", stormId, e);
        }
    }

    /**
     * Check for the files exists to avoid supervisor crashing Also makes sure there is no necessity for locking"
     * 
     * @param conf
     * @param localizer
     * @param assignedStormIds
     * @param allDownloadedTopologyIds
     * @return
     */
    protected Set<String> verifyDownloadedFiles(Map conf, Localizer localizer, Set<String> assignedStormIds, Set<String> allDownloadedTopologyIds)
            throws IOException {
        Set<String> srashStormIds = new HashSet<>();
        for (String stormId : allDownloadedTopologyIds) {
            if (assignedStormIds.contains(stormId)) {
                if (!SupervisorUtils.doRequiredTopoFilesExist(conf, stormId)) {
                    LOG.debug("Files not present in topology directory");
                    rmTopoFiles(conf, stormId, localizer, false);
                    srashStormIds.add(stormId);
                }
            }
        }
        return srashStormIds;
    }

    /**
     * download code ; two cluster mode: local and distributed
     *
     * @param conf
     * @param stormId
     * @param masterCodeDir
     * @throws IOException
     */
    private void downloadStormCode(Map conf, String stormId, String masterCodeDir, Localizer localizer) throws Exception {
        String clusterMode = ConfigUtils.clusterMode(conf);

        if (clusterMode.endsWith("distributed")) {
            downloadDistributeStormCode(conf, stormId, masterCodeDir, localizer);
        } else if (clusterMode.endsWith("local")) {
            downloadLocalStormCode(conf, stormId, masterCodeDir, localizer);
        }
    }

    private void downloadLocalStormCode(Map conf, String stormId, String masterCodeDir, Localizer localizer) throws Exception {

        String tmproot = ConfigUtils.supervisorTmpDir(conf) + Utils.FILE_PATH_SEPARATOR + Utils.uuid();
        String stormroot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
        BlobStore blobStore = Utils.getNimbusBlobStore(conf, masterCodeDir, null);
        FileOutputStream codeOutStream = null;
        FileOutputStream confOutStream = null;
        try {
            FileUtils.forceMkdir(new File(tmproot));
            String stormCodeKey = ConfigUtils.masterStormCodeKey(stormId);
            String stormConfKey = ConfigUtils.masterStormConfKey(stormId);
            String codePath = ConfigUtils.supervisorStormCodePath(tmproot);
            String confPath = ConfigUtils.supervisorStormConfPath(tmproot);
            codeOutStream = new FileOutputStream(codePath);
            blobStore.readBlobTo(stormCodeKey, codeOutStream, null);
            confOutStream = new FileOutputStream(confPath);
            blobStore.readBlobTo(stormConfKey, confOutStream, null);
        } finally {
            if (codeOutStream != null)
                codeOutStream.close();
            if (confOutStream != null)
                codeOutStream.close();
            blobStore.shutdown();
        }
        FileUtils.moveDirectory(new File(tmproot), new File(stormroot));
        SupervisorUtils.setupStormCodeDir(conf, ConfigUtils.readSupervisorStormConf(conf, stormId), stormroot);
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();

        String resourcesJar = resourcesJar();

        URL url = classloader.getResource(ConfigUtils.RESOURCES_SUBDIR);

        String targetDir = stormroot + Utils.FILE_PATH_SEPARATOR + ConfigUtils.RESOURCES_SUBDIR;

        if (resourcesJar != null) {
            LOG.info("Extracting resources from jar at {} to {}", resourcesJar, targetDir);
            Utils.extractDirFromJar(resourcesJar, ConfigUtils.RESOURCES_SUBDIR, stormroot);
        } else if (url != null) {

            LOG.info("Copying resources at {} to {} ", url.toString(), targetDir);
            if (url.getProtocol() == "jar") {
                JarURLConnection urlConnection = (JarURLConnection) url.openConnection();
                Utils.extractDirFromJar(urlConnection.getJarFileURL().getFile(), ConfigUtils.RESOURCES_SUBDIR, stormroot);
            } else {
                FileUtils.copyDirectory(new File(url.getFile()), (new File(targetDir)));
            }
        }
    }

    /**
     * Downloading to permanent location is atomic
     * 
     * @param conf
     * @param stormId
     * @param masterCodeDir
     * @param localizer
     * @throws Exception
     */
    private void downloadDistributeStormCode(Map conf, String stormId, String masterCodeDir, Localizer localizer) throws Exception {

        String tmproot = ConfigUtils.supervisorTmpDir(conf) + Utils.FILE_PATH_SEPARATOR + Utils.uuid();
        String stormroot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
        ClientBlobStore blobStore = Utils.getClientBlobStoreForSupervisor(conf);
        FileUtils.forceMkdir(new File(tmproot));
        if (Utils.isOnWindows()) {
            if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
                throw new RuntimeException("ERROR: Windows doesn't implement setting the correct permissions");
            }
        } else {
            Utils.restrictPermissions(tmproot);
        }
        String stormJarKey = ConfigUtils.masterStormJarKey(stormId);
        String stormCodeKey = ConfigUtils.masterStormCodeKey(stormId);
        String stormConfKey = ConfigUtils.masterStormConfKey(stormId);
        String jarPath = ConfigUtils.supervisorStormJarPath(tmproot);
        String codePath = ConfigUtils.supervisorStormCodePath(tmproot);
        String confPath = ConfigUtils.supervisorStormConfPath(tmproot);
        Utils.downloadResourcesAsSupervisor(stormJarKey, jarPath, blobStore);
        Utils.downloadResourcesAsSupervisor(stormCodeKey, codePath, blobStore);
        Utils.downloadResourcesAsSupervisor(stormConfKey, confPath, blobStore);
        blobStore.shutdown();
        Utils.extractDirFromJar(jarPath, ConfigUtils.RESOURCES_SUBDIR, tmproot);
        downloadBlobsForTopology(conf, confPath, localizer, tmproot);
        if (didDownloadBlobsForTopologySucceed(confPath, tmproot)) {
            LOG.info("Successfully downloaded blob resources for storm-id {}", stormId);
            if (Utils.isOnWindows()) {
                // Files/move with non-empty directory doesn't work well on Windows
                FileUtils.moveDirectory(new File(tmproot), new File(stormroot));
            } else {
                FileUtils.forceMkdir(new File(stormroot));
                Files.move(new File(tmproot).toPath(), new File(stormroot).toPath(), StandardCopyOption.ATOMIC_MOVE);
            }
        } else {
            LOG.info("Failed to download blob resources for storm-id ", stormId);
            Utils.forceDelete(tmproot);
        }
    }

    /**
     * Assert if all blobs are downloaded for the given topology
     * 
     * @param stormconfPath
     * @param targetDir
     * @return
     */
    protected boolean didDownloadBlobsForTopologySucceed(String stormconfPath, String targetDir) throws IOException {
        Map stormConf = Utils.fromCompressedJsonConf(FileUtils.readFileToByteArray(new File(stormconfPath)));
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) stormConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        List<String> blobFileNames = new ArrayList<>();
        if (blobstoreMap != null) {
            for (Map.Entry<String, Map<String, Object>> entry : blobstoreMap.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> blobInfo = entry.getValue();
                String ret = null;
                if (blobInfo != null && blobInfo.containsKey("localname")) {
                    ret = (String) blobInfo.get("localname");
                } else {
                    ret = key;
                }
                blobFileNames.add(ret);
            }
        }
        for (String string : blobFileNames) {
            if (!Utils.checkFileExists(string))
                return false;
        }
        return true;
    }

    /**
     * Download all blobs listed in the topology configuration for a given topology.
     * 
     * @param conf
     * @param stormconfPath
     * @param localizer
     * @param tmpRoot
     */
    protected void downloadBlobsForTopology(Map conf, String stormconfPath, Localizer localizer, String tmpRoot) throws IOException {
        Map stormConf = ConfigUtils.readSupervisorStormConfGivenPath(conf, stormconfPath);
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) stormConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        String user = (String) stormConf.get(Config.TOPOLOGY_SUBMITTER_USER);
        String topoName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        File userDir = localizer.getLocalUserFileCacheDir(user);
        List<LocalResource> localResourceList = SupervisorUtils.blobstoreMapToLocalresources(blobstoreMap);
        if (localResourceList.size() > 0) {
            if (!userDir.exists()) {
                FileUtils.forceMkdir(userDir);
            }
            try {
                List<LocalizedResource> localizedResources = localizer.getBlobs(localResourceList, user, topoName, userDir);
                setupBlobPermission(conf, user, userDir.toString());
                for (LocalizedResource localizedResource : localizedResources) {
                    File rsrcFilePath = new File(localizedResource.getFilePath());
                    String keyName = rsrcFilePath.getName();
                    String blobSymlinkTargetName = new File(localizedResource.getCurrentSymlinkPath()).getName();

                    String symlinkName = null;
                    if (blobstoreMap != null) {
                        Map<String, Object> blobInfo = blobstoreMap.get(keyName);
                        if (blobInfo != null && blobInfo.containsKey("localname")) {
                            symlinkName = (String) blobInfo.get("localname");
                        } else {
                            symlinkName = keyName;
                        }
                    }
                    Utils.createSymlink(tmpRoot, rsrcFilePath.getParent(), symlinkName, blobSymlinkTargetName);
                }
            } catch (AuthorizationException authExp) {
                LOG.error("AuthorizationException error {}", authExp);
            } catch (KeyNotFoundException knf) {
                LOG.error("KeyNotFoundException error {}", knf);
            }
        }
    }

    protected void setupBlobPermission(Map conf, String user, String path) throws IOException {
        if (Utils.getBoolean(Config.SUPERVISOR_RUN_WORKER_AS_USER, false)) {
            String logPrefix = "setup blob permissions for " + path;
            SupervisorUtils.processLauncherAndWait(conf, user, Arrays.asList("blob", path), null, logPrefix);
        }

    }

    private String resourcesJar() throws IOException {

        String path = Utils.currentClasspath();
        if (path == null) {
            return null;
        }
        String[] paths = path.split(File.pathSeparator);
        List<String> jarPaths = new ArrayList<String>();
        for (String s : paths) {
            if (s.endsWith(".jar")) {
                jarPaths.add(s);
            }
        }

        List<String> rtn = new ArrayList<String>();
        int size = jarPaths.size();
        for (int i = 0; i < size; i++) {
            if (Utils.zipDoesContainDir(jarPaths.get(i), ConfigUtils.RESOURCES_SUBDIR)) {
                rtn.add(jarPaths.get(i));
            }
        }
        if (rtn.size() == 0)
            return null;

        return rtn.get(0);
    }

    protected Map<Integer, LocalAssignment> readAssignments(Map<String, Map<String, Object>> assignmentsSnapshot,
            Map<Integer, LocalAssignment> existingAssignment, String assignmentId, AtomicInteger retries) {
        try {
            Map<Integer, LocalAssignment> portLA = new HashMap<Integer, LocalAssignment>();
            for (Map.Entry<String, Map<String, Object>> assignEntry : assignmentsSnapshot.entrySet()) {
                String stormId = assignEntry.getKey();
                Assignment assignment = (Assignment) assignEntry.getValue().get(IStateStorage.DATA);

                Map<Integer, LocalAssignment> portTasks = readMyExecutors(stormId, assignmentId, assignment);

                for (Map.Entry<Integer, LocalAssignment> entry : portTasks.entrySet()) {

                    Integer port = entry.getKey();

                    LocalAssignment la = entry.getValue();

                    if (!portLA.containsKey(port)) {
                        portLA.put(port, la);
                    } else {
                        throw new RuntimeException("Should not have multiple topologys assigned to one port");
                    }
                }
            }
            retries.set(0);
            return portLA;
        } catch (RuntimeException e) {
            if (retries.get() > 2) {
                throw e;
            } else {
                retries.addAndGet(1);
            }
            LOG.warn("{} : retrying {} of 3", e.getMessage(), retries.get());
            return existingAssignment;
        }
    }

    protected Map<Integer, LocalAssignment> readMyExecutors(String stormId, String assignmentId, Assignment assignment) {
        Map<Integer, LocalAssignment> portTasks = new HashMap<>();
        Map<Long, WorkerResources> slotsResources = new HashMap<>();
        Map<NodeInfo, WorkerResources> nodeInfoWorkerResourcesMap = assignment.get_worker_resources();
        if (nodeInfoWorkerResourcesMap != null) {
            for (Map.Entry<NodeInfo, WorkerResources> entry : nodeInfoWorkerResourcesMap.entrySet()) {
                if (entry.getKey().get_node().equals(assignmentId)) {
                    Set<Long> ports = entry.getKey().get_port();
                    for (Long port : ports) {
                        slotsResources.put(port, entry.getValue());
                    }
                }
            }
        }
        Map<List<Long>, NodeInfo> executorNodePort = assignment.get_executor_node_port();
        if (executorNodePort != null) {
            for (Map.Entry<List<Long>, NodeInfo> entry : executorNodePort.entrySet()) {
                if (entry.getValue().get_node().equals(assignmentId)) {
                    for (Long port : entry.getValue().get_port()) {
                        LocalAssignment localAssignment = portTasks.get(port.intValue());
                        if (localAssignment == null) {
                            List<ExecutorInfo> executors = new ArrayList<ExecutorInfo>();
                            localAssignment = new LocalAssignment(stormId, executors);
                            if (slotsResources.containsKey(port)) {
                                localAssignment.set_resources(slotsResources.get(port));
                            }
                            portTasks.put(port.intValue(), localAssignment);
                        }
                        List<ExecutorInfo> executorInfoList = localAssignment.get_executors();
                        executorInfoList.add(new ExecutorInfo(entry.getKey().get(0).intValue(), entry.getKey().get(entry.getKey().size() - 1).intValue()));
                    }
                }
            }
        }
        return portTasks;
    }

    // I konw it's not a good idea to create SyncProcessEvent, but I only hope SyncProcessEvent is responsible for start/shutdown
    // workers, and SyncSupervisorEvent is responsible for download/remove topologys' binary.
    protected void shutdownDisallowedWorkers() throws Exception {
        LocalState localState = supervisorData.getLocalState();
        Map<Integer, LocalAssignment> assignedExecutors = localState.getLocalAssignmentsMap();
        if (assignedExecutors == null) {
            assignedExecutors = new HashMap<>();
        }
        int now = Time.currentTimeSecs();
        Map<String, StateHeartbeat> workerIdHbstate = syncProcesses.getLocalWorkerStats(supervisorData, assignedExecutors, now);
        LOG.debug("Allocated workers ", assignedExecutors);
        for (Map.Entry<String, StateHeartbeat> entry : workerIdHbstate.entrySet()) {
            String workerId = entry.getKey();
            StateHeartbeat stateHeartbeat = entry.getValue();
            if (stateHeartbeat.getState() == State.DISALLOWED) {
                syncProcesses.killWorker(supervisorData, supervisorData.getWorkerManager(), workerId);
                LOG.debug("{}'s state disallowed, so shutdown this worker");
            }
        }
    }
}
