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

import org.apache.storm.Config;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

public class ConfigUtils {
    private final static Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);
    public final static String RESOURCES_SUBDIR = "resources";
    public final static String NIMBUS_DO_NOT_REASSIGN = "NIMBUS-DO-NOT-REASSIGN";
    public static final String FILE_SEPARATOR = File.separator;
    public final static String LOG_DIR;

    static {
        String dir;
        Map conf;
        if (System.getProperty("storm.log.dir") != null) {
            dir = System.getProperty("storm.log.dir");
        } else if ((conf = readStormConfig()).get("storm.log.dir") != null) {
            dir = String.valueOf(conf.get("storm.log.dir"));
        } else {
            dir = System.getProperty("storm.log.dir") + FILE_SEPARATOR + "logs";
        }
        try {
            LOG_DIR = new File(dir).getCanonicalPath();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Illegal storm.log.dir in conf: " + dir);
        }
    }

    //public final static String WORKER_DATA_SUBDIR = "worker_shared_data";


    public static String clojureConfigName(String name) {
        return name.toUpperCase().replace("_", "-");
    }

    // ALL-CONFIGS is only used by executor.clj once, do we want to do it here? TODO
    public static List<Object> All_CONFIGS() {
        List<Object> ret = new ArrayList<Object>();
        Config config = new Config();
        Class<?> ConfigClass = config.getClass();
        Field[] fields = ConfigClass.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                Object obj = fields[i].get(null);
                ret.add(obj);
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return ret;
    }

    public static String clusterMode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        return mode;

    }

    public static boolean isLocalMode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        if (mode != null) {
            if ("local".equals(mode)) {
                return true;
            }
            if ("distributed".equals(mode)) {
                return false;
            }
        }
        throw new IllegalArgumentException("Illegal cluster mode in conf: " + mode);
    }

    public static int samplingRate(Map conf) {
        double rate = Utils.getDouble(conf.get(Config.TOPOLOGY_STATS_SAMPLE_RATE));
        if (rate != 0) {
            return (int) (1 / rate);
        }
        throw new IllegalArgumentException("Illegal topology.stats.sample.rate in conf: " + rate);
    }

    // public static mkStatsSampler // depends on Utils.evenSampler() TODO, this is sth we have to do

    // public static readDefaultConfig // depends on Utils.clojurifyStructure and Utils.readDefaultConfig // TODO

    // validate-configs-with-schemas is just a wrapper of ConfigValidation.validateFields(conf) TODO

    //For testing only
    // for java
    // try (SetMockedStormConfig mocked = new SetMockedStormConfig(conf)) {
    //    run test ...
    // }
    //
    // for clojure
    // (with-open [mock (SetMockedStormConfig. conf)]
    //     run test ...)
    public static class SetMockedStormConfig implements Closeable {
        public SetMockedStormConfig(Map conf) {
            mockedStormConfig = conf;
        }

        @Override
        public void close() {
            mockedStormConfig = null;
        }
    }
    private static Map mockedStormConfig = null;
    public static Map readStormConfig() {
        if (mockedStormConfig != null) return mockedStormConfig;
        Map conf = Utils.readStormConfig();
        ConfigValidation.validateFields(conf);
        return conf; // TODO, should this be clojurify-sturecture and then return? Otherwise, the clj files who call it fail
    }

    public static Map readYamlConfig(String name, boolean mustExist) {
        Map conf = Utils.findAndReadConfigFile(name, mustExist);
        ConfigValidation.validateFields(conf);
        return conf;
    }

    public static Map readYamlConfig(String name) {
        return  readYamlConfig(name, true);
    }

    public static String absoluteStormLocalDir(Map conf) {
        LOG.info("zliu conf map is " + conf);
        String stormHome = System.getProperty("storm.home");
        LOG.info("zliu stormhome is " + stormHome);
        String localDir = (String) conf.get(Config.STORM_LOCAL_DIR);
        if (localDir == null) {
            return (stormHome + FILE_SEPARATOR + "storm-local");
        } else {
            LOG.info("zliu java local dir is " + localDir + ", isAbsolute:" + (new File(localDir).isAbsolute()));
            if (new File(localDir).isAbsolute()) {
                return localDir;
            } else {
                return (stormHome + FILE_SEPARATOR + localDir);
            }
        }

    }

    public static String absoluteHealthCheckDir(Map conf) {
        String stormHome = System.getProperty("storm.home");
        String healthCheckDir = String.valueOf(conf.get(Config.STORM_HEALTH_CHECK_DIR));
        if (healthCheckDir.equals("null")) {
            return (stormHome + FILE_SEPARATOR + "healthchecks");
        } else {
            if (new File(healthCheckDir).isAbsolute()) {
                return healthCheckDir;
            } else {
                return (stormHome + FILE_SEPARATOR + healthCheckDir);
            }
        }
    }

    public static String masterLocalDir(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPARATOR + "nimbus";
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }


    public static String masterStormJarKey(String topologyId) {
        return (topologyId + "-stormjar.jar");
    }

    public static String masterStormCodeKey(String topologyId) {
        return (topologyId + "-stormcode.ser");
    }

    public static String masterStormConfKey(String topologyId) {
        return (topologyId + "-stormconf.ser");
    }

    public static String masterStormDistRoot(Map conf) throws IOException {
        String ret = stormDistPath(masterLocalDir(conf));
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterStormDistRoot(Map conf, String stormId) throws IOException {
        return (masterStormDistRoot(conf) + FILE_SEPARATOR + stormId);
    }

    public static String stormDistPath(String stormRoot) {
        String ret = "";
        // we do this since to concat a null String will actually concat a "null", which is not the expected: ""
        if (stormRoot != null) {
            ret = stormRoot;
        }
        return ret + FILE_SEPARATOR + "stormdist";
    }

    public static String stormTmpPath(String stormRoot) {
        return stormRoot + FILE_SEPARATOR + "tmp";
    }

    /* Never get used TODO : delete it*/
    public static String masterTmpDir(Map conf) throws IOException {
        String ret = stormTmpPath(masterLocalDir(conf));
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static Map readSupervisorStormConfGivenPath(Map conf, String stormConfPath) throws  IOException {
        Map ret = new HashMap(conf);
        ret.putAll(Utils.fromCompressedJsonConf(FileUtils.readFileToByteArray(new File(stormConfPath)))); // TODO we do not need clojurify-structure, right?
        return ret;
    }

    /* Never get used TODO : may delete it*/
    public static String masterStormMetaFilePath(String stormRoot) {
        return (stormRoot + FILE_SEPARATOR + "storm-code-distributor.meta");
    }

    public static String masterStormJarPath(String stormRoot) {
        return (stormRoot + FILE_SEPARATOR + "stormjar.jar");
    }

    /* Never get used TODO : may delete it*/
    public static String masterStormCodePath(String stormRoot) {
        return (stormRoot + FILE_SEPARATOR + "stormcode.ser");
    }

    /* Never get used TODO : may delete it*/
    public static String masterStormConfPath(String stormRoot) {
        return (stormRoot + FILE_SEPARATOR + "stormconf.ser");
    }

    public static String masterInbox(Map conf) throws IOException {
        String ret = masterLocalDir(conf) + FILE_SEPARATOR + "inbox";
        try {
            FileUtils.forceMkdir(new File(ret));
        } catch (IOException e) {
            LOG.error("Failed to create dir " + ret, e);
            throw e;
        }
        return ret;
    }

    public static String masterInimbusDir(Map conf) throws IOException {
        return (masterLocalDir(conf) + FILE_SEPARATOR + "inimbus");
    }

    public static String supervisorLocalDir(Map conf) throws IOException {
        String ret = absoluteStormLocalDir(conf) + FILE_SEPARATOR + "supervisor";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String supervisorIsupervisorDir(Map conf) throws IOException {
        return ((supervisorLocalDir(conf) + FILE_SEPARATOR + "isupervisor"));
    }

    //For testing only
    // for java
    // try (SetMockedSupervisorStormDistRoot mocked = new SetMockedSupervisorStormDistRoot(conf)) {
    //    run test ...
    // }
    //
    // for clojure
    // (with-open [mock (SetMockedSupervisorStormDistRoot. conf)]
    //     run test ...)
    public static class SetMockedSupervisorStormDistRoot implements Closeable {
        public SetMockedSupervisorStormDistRoot(Map conf) {
            mockedSupervisorStormDistRoot = conf;
        }
        @Override
        public void close() {
            mockedSupervisorStormDistRoot = null;
        }
    }
    private static Map mockedSupervisorStormDistRoot = null;
    public static String supervisorStormDistRoot(Map conf) throws IOException {
        LOG.info("zliu supervisorStormDistRoot resl is: " + stormDistPath(supervisorLocalDir(conf)) + "mocked set is " + mockedSupervisorStormDistRoot);
        if (mockedSupervisorStormDistRoot != null) {
            return null;
        }
        return stormDistPath(supervisorLocalDir(conf)); // TODO: no need to forceMake here?, clj does not.
    }

    public static String supervisorStormDistRoot(Map conf, String stormId) throws IOException {
        if (mockedSupervisorStormDistRoot != null) {
            return null;
        }
        return supervisorStormDistRoot(conf) + FILE_SEPARATOR + stormId; // TODO: need to (url-encode storm-id)? Not.
    }

    public static String supervisorStormJarPath(String stormRoot) {
        String ret = "";
        // we do this since to concat a null String will actually concat a "null", which is not the expected: ""
        if (stormRoot != null) {
            ret = stormRoot;
        }
        return (ret + FILE_SEPARATOR + "stormjar.jar");
    }

    /* Never get used TODO : may delete it*/
    public static String supervisorStormMetaFilePath(String stormRoot) {
        String ret = "";
        if (stormRoot != null) {
            ret = stormRoot;
        }
        return (ret + FILE_SEPARATOR + "storm-code-distributor.meta");
    }

    public static String supervisorStormCodePath(String stormRoot) {
        String ret = "";
        if (stormRoot != null) {
            ret = stormRoot;
        }
        return (ret + FILE_SEPARATOR + "stormcode.ser");
    }

    public static String supervisorStormConfPath(String stormRoot) {
        String ret = "";
        if (stormRoot != null) {
            ret = stormRoot;
        }
        return (ret + FILE_SEPARATOR + "stormconf.ser");
    }

    public static String supervisorTmpDir(Map conf) throws IOException {
        String ret = supervisorLocalDir(conf) + FILE_SEPARATOR + "tmp";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String supervisorStormResourcesPath(String stormRoot) {
        String ret = "";
        // we do this since to concat a null String will actually concat a "null", which is not the expected: ""
        if (stormRoot != null) {
            ret = stormRoot;
        }
        return (ret + FILE_SEPARATOR + RESOURCES_SUBDIR);
    }

    public static LocalState supervisorState(Map conf) throws IOException {
        return new LocalState((supervisorLocalDir(conf) + FILE_SEPARATOR + "localstate"));
    }

    public static LocalState nimbusTopoHistoryState(Map conf) throws IOException {
        return new LocalState((masterLocalDir(conf) + FILE_SEPARATOR + "history"));
    }

    public static Map readSupervisorStormConf(Map conf, String stormId) throws IOException {
        String stormRoot = supervisorStormDistRoot(conf, stormId);
        String confPath = supervisorStormConfPath(stormRoot);
        return readSupervisorStormConfGivenPath(conf, confPath);
    }

    public static StormTopology readSupervisorTopology(Map conf, String stormId) throws IOException {
        String stormRoot = supervisorStormDistRoot(conf, stormId);
        String topologyPath = supervisorStormCodePath(stormRoot);
        return Utils.deserialize(FileUtils.readFileToByteArray(new File(topologyPath)), StormTopology.class);
    }

    public static String workerUserRoot(Map conf) {
        return (absoluteStormLocalDir(conf) + FILE_SEPARATOR + "/workers-users");
    }

    public static String workerUserFile(Map conf, String workerId) {
        return (workerUserRoot(conf) + FILE_SEPARATOR + workerId);
    }

    public static String getWorkerUser(Map conf, String workerId) throws IOException {
        LOG.info("GET worker-user for {}", workerId);
        File file = new File(workerUserFile(conf, workerId));

        try (InputStream in = new FileInputStream(file);
             Reader reader = new InputStreamReader(in);
             BufferedReader br = new BufferedReader(reader);) {
            StringBuilder sb = new StringBuilder();
            int r;
            while ((r = br.read()) != -1) {
                char ch = (char)r;
                sb.append(ch);
            }
            String ret = sb.toString().trim();
            return ret;
        }
    }

    public static String getIdFromBlobKey(String key) {
        if (key == null) return null;
        String ret = null;
        if (key.endsWith("-stormjar.jar")) {
            ret = key.substring(0, key.length() - 13);
        } else if (key.endsWith("-stormcode.ser")) {
            ret = key.substring(0, key.length() - 14);
        } else if (key.endsWith("-stormconf.ser")) {
            ret = key.substring(0, key.length() - 14);
        }
        return ret;
    }

    public static void setWorkerUserWSE(Map conf, String workerId, String user) throws IOException {
        LOG.info("SET worker-user {} {}", workerId, user);
        File file = new File(workerUserFile(conf, workerId));
        file.getParentFile().mkdirs();

        try (FileWriter fw = new FileWriter(file);
             BufferedWriter writer = new BufferedWriter(fw);) {
            writer.write(user);
        }
    }

    public static void removeWorkerUserWSE(Map conf, String workerId) {
        LOG.info("REMOVE worker-user {}", workerId);
        new File(workerUserFile(conf, workerId)).delete();
    }

    public static String workerArtifactsRoot(Map conf) {
        String artifactsDir = (String)conf.get(Config.STORM_WORKERS_ARTIFACTS_DIR);
        if (artifactsDir == null) {
            return (LOG_DIR + FILE_SEPARATOR + "workers-artifacts");
        } else {
            if (new File(artifactsDir).isAbsolute()) {
                return artifactsDir;
            } else {
                return (LOG_DIR + FILE_SEPARATOR + artifactsDir);
            }
        }
    }

    public static String workerArtifactsRoot(Map conf, String id) {
        return (workerArtifactsRoot(conf) + FILE_SEPARATOR + id);
    }

    public static String workerArtifactsRoot(Map conf, String id, String port) {
        return (workerArtifactsRoot(conf, id) + FILE_SEPARATOR + port);
    }

    public static String workerArtifactsPidPath(Map conf, String id, String port) {
        return (workerArtifactsRoot(conf, id, port) + FILE_SEPARATOR +  "worker.pid");
    }

    public static File getLogMetaDataFile(String fname) {
        String[] subStrings = fname.split(FILE_SEPARATOR); // TODO: does this work well on windows?
        String id = subStrings[0];
        String port = subStrings[1];
        return getLogMetaDataFile(Utils.readStormConfig(), id, port);
    }

    public static File getLogMetaDataFile(Map conf, String id, String port) {
        String fname = workerArtifactsRoot(conf, id, port) + FILE_SEPARATOR + "worker.yaml";
        return new File(fname);
    }

    public static File getWorkerDirFromRoot(String logRoot, String id, String port) {
        return new File((logRoot + FILE_SEPARATOR + id + FILE_SEPARATOR + port));
    }

    public static String workerRoot(Map conf) {
        return (absoluteStormLocalDir(conf) + FILE_SEPARATOR + "workers");
    }

    public static String workerRoot(Map conf, String id) {
        return (workerRoot(conf) + FILE_SEPARATOR + id);
    }

    public static String workerPidsRoot(Map conf, String id) {
        return (workerRoot(conf, id) + FILE_SEPARATOR + "pids");
    }

    public static String workerPidsRoot(Map conf, String id, String pid) {
        return (workerPidsRoot(conf, id) + FILE_SEPARATOR + pid);
    }

    public static String workerHeartbeatRoot(Map conf, String id) {
        return (workerRoot(conf, id) + FILE_SEPARATOR + "heartbeats");
    }

    public static LocalState workerState(Map conf, String id) throws IOException {
        return new LocalState(workerHeartbeatRoot(conf, id));
    }

    public static void overrideLoginConfigWithSystemProperty(Map conf) { // note that we delete the return value
        String loginConfFile = System.getProperty("java.security.auth.login.config");
        if (loginConfFile != null) {
             conf.put("java.security.auth.login.config", loginConfFile);
        }
    }


    public List<String> getTopoLogsUsers(Map topologyConf) {
        List<String> logsUsers = (List<String>)topologyConf.get(Config.LOGS_USERS);
        // TODO: can we do force type trans? how about "userA, userB" format
        List<String> topologyUsers = (List<String>)topologyConf.get(Config.TOPOLOGY_USERS);
        Set<String> mergedUsers = new HashSet<String>();
        for (String user : logsUsers) {
            if (!user.equals("null")) {
                mergedUsers.add(user);
            }
        }
        for (String user : topologyUsers) {
            if (!user.equals("null")) {
                mergedUsers.add(user);
            }
        }
        List<String> ret = new ArrayList<String>(mergedUsers);
        Collections.sort(ret);
        return ret;
    }


    public List<String> getTopoLogsGroups(Map topologyConf) {
        List<String> logsGroups = (List<String>)topologyConf.get(Config.LOGS_GROUPS);
        // TODO: can we do force type trans? how about "groupA, groupB" format // better tested it by changes at called side
        List<String> topologyGroups = (List<String>)topologyConf.get(Config.TOPOLOGY_GROUPS);
        Set<String> mergedGroups = new HashSet<String>();
        for (String user : logsGroups) {
            if (!user.equals("null")) {
                mergedGroups.add(user);
            }
        }
        for (String user : topologyGroups) {
            if (!user.equals("null")) {
                mergedGroups.add(user);
            }
        }
        List<String> ret = new ArrayList<String>(mergedGroups);
        Collections.sort(ret);
        return ret;
    }

}
