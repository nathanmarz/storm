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
import org.apache.curator.utils.PathUtils;
import org.apache.storm.Config;
import org.apache.storm.localizer.LocalResource;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;

public class SupervisorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorUtils.class);

    public static Process workerLauncher(Map conf, String user, List<String> args, Map<String, String> environment, final String logPreFix,
            final Utils.ExitCodeCallable exitCodeCallback, File dir) throws IOException {
        if (StringUtils.isBlank(user)) {
            throw new IllegalArgumentException("User cannot be blank when calling workerLauncher.");
        }
        String wlinitial = (String) (conf.get(Config.SUPERVISOR_WORKER_LAUNCHER));
        String stormHome = System.getProperty("storm.home");
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
    public static Boolean isShouldUncompressBlob(Map<String, Object> blobInfo) {
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
                LocalResource localResource = new LocalResource(map.getKey(), isShouldUncompressBlob(map.getValue()));
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

    public static boolean checkTopoFilesExist(Map conf, String stormId) throws IOException {
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
        if (!ConfigUtils.isLocalMode(conf) && !Utils.checkFileExists(stormjarpath))
            return false;
        return true;
    }

}
