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

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.ProcessSimulator;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.localizer.LocalResource;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;

public class SupervisorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorUtils.class);

    private static final SupervisorUtils INSTANCE = new SupervisorUtils();
    private static SupervisorUtils _instance = INSTANCE;
    public static void setInstance(SupervisorUtils u) {
        _instance = u;
    }
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    public static Process workerLauncher(Map conf, String user, List<String> args, Map<String, String> environment, final String logPreFix,
            final Utils.ExitCodeCallable exitCodeCallback, File dir) throws IOException {
        if (StringUtils.isBlank(user)) {
            throw new IllegalArgumentException("User cannot be blank when calling workerLauncher.");
        }
        String wlinitial = (String) (conf.get(Config.SUPERVISOR_WORKER_LAUNCHER));
        String stormHome = ConfigUtils.concatIfNotNull(System.getProperty("storm.home"));
        String wl;
        if (StringUtils.isNotBlank(wlinitial)) {
            wl = wlinitial;
        } else {
            wl = stormHome + "/bin/worker-launcher";
        }
        List<String> commands = new ArrayList<>();
        commands.add(wl);
        commands.add(user);
        commands.addAll(args);
        LOG.info("Running as user: {} command: {}", user, commands);
        return Utils.launchProcess(commands, environment, logPreFix, exitCodeCallback, dir);
    }

    public static int workerLauncherAndWait(Map conf, String user, List<String> args, final Map<String, String> environment, final String logPreFix)
            throws IOException {
        int ret = 0;
        Process process = workerLauncher(conf, user, args, environment, logPreFix, null, null);
        if (StringUtils.isNotBlank(logPreFix))
            Utils.readAndLogStream(logPreFix, process.getInputStream());
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            LOG.info("{} interrupted.", logPreFix);
        }
        ret = process.exitValue();
        return ret;
    }

    public static void setupStormCodeDir(Map conf, Map stormConf, String dir) throws IOException {
        if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            String logPrefix = "setup conf for " + dir;
            List<String> commands = new ArrayList<>();
            commands.add("code-dir");
            commands.add(dir);
            workerLauncherAndWait(conf, (String) (stormConf.get(Config.TOPOLOGY_SUBMITTER_USER)), commands, null, logPrefix);
        }
    }

    public static void rmrAsUser(Map conf, String id, String path) throws IOException {
        String user = Utils.getFileOwner(path);
        String logPreFix = "rmr " + id;
        List<String> commands = new ArrayList<>();
        commands.add("rmr");
        commands.add(path);
        SupervisorUtils.workerLauncherAndWait(conf, user, commands, null, logPreFix);
        if (Utils.checkFileExists(path)) {
            throw new RuntimeException(path + " was not deleted.");
        }
    }

    /**
     * Given the blob information returns the value of the uncompress field, handling it either being a string or a boolean value, or if it's not specified then
     * returns false
     * 
     * @param blobInfo
     * @return
     */
    public static Boolean shouldUncompressBlob(Map<String, Object> blobInfo) {
        return new Boolean((String) blobInfo.get("uncompress"));
    }

    /**
     * Remove a reference to a blob when its no longer needed
     * 
     * @param blobstoreMap
     * @return
     */
    public static List<LocalResource> blobstoreMapToLocalresources(Map<String, Map<String, Object>> blobstoreMap) {
        List<LocalResource> localResourceList = new ArrayList<>();
        if (blobstoreMap != null) {
            for (Map.Entry<String, Map<String, Object>> map : blobstoreMap.entrySet()) {
                LocalResource localResource = new LocalResource(map.getKey(), shouldUncompressBlob(map.getValue()));
                localResourceList.add(localResource);
            }
        }
        return localResourceList;
    }

    /**
     * For each of the downloaded topologies, adds references to the blobs that the topologies are using. This is used to reconstruct the cache on restart.
     * 
     * @param localizer
     * @param stormId
     * @param conf
     */
    public static void addBlobReferences(Localizer localizer, String stormId, Map conf) throws IOException {
        Map stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) stormConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        String user = (String) stormConf.get(Config.TOPOLOGY_SUBMITTER_USER);
        String topoName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        List<LocalResource> localresources = SupervisorUtils.blobstoreMapToLocalresources(blobstoreMap);
        if (blobstoreMap != null) {
            localizer.addReferences(localresources, user, topoName);
        }
    }

    public static Set<String> readDownLoadedStormIds(Map conf) throws IOException {
        Set<String> stormIds = new HashSet<>();
        String path = ConfigUtils.supervisorStormDistRoot(conf);
        Collection<String> rets = Utils.readDirContents(path);
        for (String ret : rets) {
            stormIds.add(URLDecoder.decode(ret));
        }
        return stormIds;
    }

    public static Collection<String> supervisorWorkerIds(Map conf) {
        String workerRoot = ConfigUtils.workerRoot(conf);
        return Utils.readDirContents(workerRoot);
    }

    public static boolean doRequiredTopoFilesExist(Map conf, String stormId) throws IOException {
        String stormroot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
        String stormjarpath = ConfigUtils.supervisorStormJarPath(stormroot);
        String stormcodepath = ConfigUtils.supervisorStormCodePath(stormroot);
        String stormconfpath = ConfigUtils.supervisorStormConfPath(stormroot);
        if (!Utils.checkFileExists(stormroot))
            return false;
        if (!Utils.checkFileExists(stormcodepath))
            return false;
        if (!Utils.checkFileExists(stormconfpath))
            return false;
        if (ConfigUtils.isLocalMode(conf) || Utils.checkFileExists(stormjarpath))
            return true;
        return false;
    }

    /**
     * Returns map from worr id to heartbeat
     *
     * @param conf
     * @return
     * @throws Exception
     */
    public static Map<String, LSWorkerHeartbeat> readWorkerHeartbeats(Map conf) throws Exception {
        return _instance.readWorkerHeartbeatsImpl(conf);
    }

    public  Map<String, LSWorkerHeartbeat> readWorkerHeartbeatsImpl(Map conf) throws Exception {
        Map<String, LSWorkerHeartbeat> workerHeartbeats = new HashMap<>();

        Collection<String> workerIds = SupervisorUtils.supervisorWorkerIds(conf);

        for (String workerId : workerIds) {
            LSWorkerHeartbeat whb = readWorkerHeartbeat(conf, workerId);
            // ATTENTION: whb can be null
            workerHeartbeats.put(workerId, whb);
        }
        return workerHeartbeats;
    }


    /**
     * get worker heartbeat by workerId
     *
     * @param conf
     * @param workerId
     * @return
     * @throws IOException
     */
    public static LSWorkerHeartbeat readWorkerHeartbeat(Map conf, String workerId) {
        return _instance.readWorkerHeartbeatImpl(conf, workerId);
    }

    public  LSWorkerHeartbeat readWorkerHeartbeatImpl(Map conf, String workerId) {
        try {
            LocalState localState = ConfigUtils.workerState(conf, workerId);
            return localState.getWorkerHeartBeat();
        } catch (Exception e) {
            LOG.warn("Failed to read local heartbeat for workerId : {},Ignoring exception.", workerId, e);
            return null;
        }
    }

    public static boolean  isWorkerHbTimedOut(int now, LSWorkerHeartbeat whb, Map conf) {
        return _instance.isWorkerHbTimedOutImpl(now, whb, conf);
    }

    public  boolean  isWorkerHbTimedOutImpl(int now, LSWorkerHeartbeat whb, Map conf) {
        boolean result = false;
        if ((now - whb.get_time_secs()) > Utils.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS))) {
            result = true;
        }
        return result;
    }

    public static String javaCmd(String cmd) {
        return _instance.javaCmdImpl(cmd);
    }

    public String javaCmdImpl(String cmd) {
        String ret = null;
        String javaHome = System.getenv().get("JAVA_HOME");
        if (StringUtils.isNotBlank(javaHome)) {
            ret = javaHome + Utils.FILE_PATH_SEPARATOR + "bin" + Utils.FILE_PATH_SEPARATOR + cmd;
        } else {
            ret = cmd;
        }
        return ret;
    }
    
    public final static List<ACL> supervisorZkAcls() {
        final List<ACL> acls = new ArrayList<>();
        acls.add(ZooDefs.Ids.CREATOR_ALL_ACL.get(0));
        acls.add(new ACL((ZooDefs.Perms.READ ^ ZooDefs.Perms.CREATE), ZooDefs.Ids.ANYONE_ID_UNSAFE));
        return acls;
    }

    public static void shutWorker(SupervisorData supervisorData, String workerId) throws IOException, InterruptedException {
        LOG.info("Shutting down {}:{}", supervisorData.getSupervisorId(), workerId);
        Map conf = supervisorData.getConf();
        Collection<String> pids = Utils.readDirContents(ConfigUtils.workerPidsRoot(conf, workerId));
        Integer shutdownSleepSecs = Utils.getInt(conf.get(Config.SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS));
        Boolean asUser = Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false);
        String user = ConfigUtils.getWorkerUser(conf, workerId);
        String threadPid = supervisorData.getWorkerThreadPids().get(workerId);
        if (StringUtils.isNotBlank(threadPid)) {
            ProcessSimulator.killProcess(threadPid);
        }

        for (String pid : pids) {
            if (asUser) {
                List<String> commands = new ArrayList<>();
                commands.add("signal");
                commands.add(pid);
                commands.add("15");
                String logPrefix = "kill -15 " + pid;
                SupervisorUtils.workerLauncherAndWait(conf, user, commands, null, logPrefix);
            } else {
                Utils.killProcessWithSigTerm(pid);
            }
        }

        if (pids.size() > 0) {
            LOG.info("Sleep {} seconds for execution of cleanup threads on worker.", shutdownSleepSecs);
            Time.sleepSecs(shutdownSleepSecs);
        }

        for (String pid : pids) {
            if (asUser) {
                List<String> commands = new ArrayList<>();
                commands.add("signal");
                commands.add(pid);
                commands.add("9");
                String logPrefix = "kill -9 " + pid;
                SupervisorUtils.workerLauncherAndWait(conf, user, commands, null, logPrefix);
            } else {
                Utils.forceKillProcess(pid);
            }
            String path = ConfigUtils.workerPidPath(conf, workerId, pid);
            if (asUser) {
                SupervisorUtils.rmrAsUser(conf, workerId, path);
            } else {
                try {
                    LOG.debug("Removing path {}", path);
                    new File(path).delete();
                } catch (Exception e) {
                    // on windows, the supervisor may still holds the lock on the worker directory
                    // ignore
                }
            }
        }
        tryCleanupWorker(conf, supervisorData, workerId);
        LOG.info("Shut down {}:{}", supervisorData.getSupervisorId(), workerId);

    }

    public static void tryCleanupWorker(Map conf, SupervisorData supervisorData, String workerId) {
        try {
            String workerRoot = ConfigUtils.workerRoot(conf, workerId);
            if (Utils.checkFileExists(workerRoot)) {
                if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
                    SupervisorUtils.rmrAsUser(conf, workerId, workerRoot);
                } else {
                    Utils.forceDelete(ConfigUtils.workerHeartbeatsRoot(conf, workerId));
                    Utils.forceDelete(ConfigUtils.workerPidsRoot(conf, workerId));
                    Utils.forceDelete(ConfigUtils.workerTmpRoot(conf, workerId));
                    Utils.forceDelete(ConfigUtils.workerRoot(conf, workerId));
                }
                ConfigUtils.removeWorkerUserWSE(conf, workerId);
                supervisorData.getDeadWorkers().remove(workerId);
            }
            if (Utils.getBoolean(conf.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE), false)){
                supervisorData.getResourceIsolationManager().releaseResourcesForWorker(workerId);
            }
        } catch (IOException e) {
            LOG.warn("Failed to cleanup worker {}. Will retry later", workerId, e);
        } catch (RuntimeException e) {
            LOG.warn("Failed to cleanup worker {}. Will retry later", workerId, e);
        }
    }

}
