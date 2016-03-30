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
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.net.URLEncoder;

public class ConfigUtils {
    private final static Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);
    public final static String RESOURCES_SUBDIR = "resources";
    public final static String NIMBUS_DO_NOT_REASSIGN = "NIMBUS-DO-NOT-REASSIGN";
    public static final String FILE_SEPARATOR = File.separator;

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static ConfigUtils _instance = new ConfigUtils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     * @param u a Utils instance
     * @return the previously set instance
     */
    public static ConfigUtils setInstance(ConfigUtils u) {
        ConfigUtils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    public static String getLogDir() {
        String dir;
        Map conf;
        if (System.getProperty("storm.log.dir") != null) {
            dir = System.getProperty("storm.log.dir");
        } else if ((conf = readStormConfig()).get("storm.log.dir") != null) {
            dir = String.valueOf(conf.get("storm.log.dir"));
        } else if (System.getProperty("storm.home") != null) {
            dir = System.getProperty("storm.home") + FILE_SEPARATOR + "logs";
        } else {
            dir = "logs";
        }
        try {
            return new File(dir).getCanonicalPath();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Illegal storm.log.dir in conf: " + dir);
        }
    }

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

    // public static mkStatsSampler // depends on Utils.evenSampler() TODO, this is sth we need to do after util

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static Map readStormConfig() {
        return _instance.readStormConfigImpl();
    }

    public Map readStormConfigImpl() {
        Map conf = Utils.readStormConfig();
        ConfigValidation.validateFields(conf);
        return conf;
    }

    public static Map readYamlConfig(String name, boolean mustExist) {
        Map conf = Utils.findAndReadConfigFile(name, mustExist);
        ConfigValidation.validateFields(conf);
        return conf;
    }

    public static Map readYamlConfig(String name) {
        return readYamlConfig(name, true);
    }

    public static String absoluteStormLocalDir(Map conf) {
        String stormHome = System.getProperty("storm.home");
        String localDir = (String) conf.get(Config.STORM_LOCAL_DIR);
        if (localDir == null) {
            return (stormHome + FILE_SEPARATOR + "storm-local");
        } else {
            if (new File(localDir).isAbsolute()) {
                return localDir;
            } else {
                return (stormHome + FILE_SEPARATOR + localDir);
            }
        }
    }

    public static String absoluteHealthCheckDir(Map conf) {
        String stormHome = System.getProperty("storm.home");
        String healthCheckDir = (String) conf.get(Config.STORM_HEALTH_CHECK_DIR);
        if (healthCheckDir == null) {
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
        FileUtils.forceMkdir(new File(ret));
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

    public static Map readSupervisorStormConfGivenPath(Map conf, String stormConfPath) throws IOException {
        Map ret = new HashMap(conf);
        ret.putAll(Utils.fromCompressedJsonConf(FileUtils.readFileToByteArray(new File(stormConfPath))));
        return ret;
    }

    public static String masterStormJarPath(String stormRoot) {
        return (stormRoot + FILE_SEPARATOR + "stormjar.jar");
    }

    public static String masterInbox(Map conf) throws IOException {
        String ret = masterLocalDir(conf) + FILE_SEPARATOR + "inbox";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterInimbusDir(Map conf) throws IOException {
        return (masterLocalDir(conf) + FILE_SEPARATOR + "inimbus");
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String supervisorLocalDir(Map conf) throws IOException {
        return _instance.supervisorLocalDirImpl(conf);
    }

    public String supervisorLocalDirImpl(Map conf) throws IOException {
        String ret = absoluteStormLocalDir(conf) + FILE_SEPARATOR + "supervisor";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String supervisorIsupervisorDir(Map conf) throws IOException {
        return (supervisorLocalDir(conf) + FILE_SEPARATOR + "isupervisor");
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String supervisorStormDistRoot(Map conf) throws IOException {
        return _instance.supervisorStormDistRootImpl(conf);
    }

    public String supervisorStormDistRootImpl(Map conf) throws IOException {
        return stormDistPath(supervisorLocalDir(conf));
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String supervisorStormDistRoot(Map conf, String stormId) throws IOException {
        return _instance.supervisorStormDistRootImpl(conf, stormId);
    }

    public String supervisorStormDistRootImpl(Map conf, String stormId) throws IOException {
        return supervisorStormDistRoot(conf) + FILE_SEPARATOR + URLEncoder.encode(stormId, "UTF-8");
    }

    public static String concatIfNotNull(String dir) {
        String ret = "";
        // we do this since to concat a null String will actually concat a "null", which is not the expected: ""
        if (dir != null) {
            ret = dir;
        }
        return ret;
    }

    public static String supervisorStormJarPath(String stormRoot) {
        return (concatIfNotNull(stormRoot) + FILE_SEPARATOR + "stormjar.jar");
    }

    public static String supervisorStormCodePath(String stormRoot) {
        return (concatIfNotNull(stormRoot) + FILE_SEPARATOR + "stormcode.ser");
    }

    public static String supervisorStormConfPath(String stormRoot) {
        return (concatIfNotNull(stormRoot) + FILE_SEPARATOR + "stormconf.ser");
    }

    public static String supervisorTmpDir(Map conf) throws IOException {
        String ret = supervisorLocalDir(conf) + FILE_SEPARATOR + "tmp";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String supervisorStormResourcesPath(String stormRoot) {
        return (concatIfNotNull(stormRoot) + FILE_SEPARATOR + RESOURCES_SUBDIR);
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static LocalState supervisorState(Map conf) throws IOException {
        return _instance.supervisorStateImpl(conf);
    }

    public LocalState supervisorStateImpl(Map conf) throws IOException {
        return new LocalState((supervisorLocalDir(conf) + FILE_SEPARATOR + "localstate"));
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static LocalState nimbusTopoHistoryState(Map conf) throws IOException {
        return _instance.nimbusTopoHistoryStateImpl(conf);
    }

    public LocalState nimbusTopoHistoryStateImpl(Map conf) throws IOException {
        return new LocalState((masterLocalDir(conf) + FILE_SEPARATOR + "history"));
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static Map readSupervisorStormConf(Map conf, String stormId) throws IOException {
        return _instance.readSupervisorStormConfImpl(conf, stormId);
    }

    public Map readSupervisorStormConfImpl(Map conf, String stormId) throws IOException {
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
        return (absoluteStormLocalDir(conf) + FILE_SEPARATOR + "workers-users");
    }

    public static String workerUserFile(Map conf, String workerId) {
        return (workerUserRoot(conf) + FILE_SEPARATOR + workerId);
    }

    public static String getWorkerUser(Map conf, String workerId) {
        LOG.info("GET worker-user for {}", workerId);
        File file = new File(workerUserFile(conf, workerId));

        try (InputStream in = new FileInputStream(file);
             Reader reader = new InputStreamReader(in);
             BufferedReader br = new BufferedReader(reader);) {
            StringBuilder sb = new StringBuilder();
            int r;
            while ((r = br.read()) != -1) {
                char ch = (char) r;
                sb.append(ch);
            }
            String ret = sb.toString().trim();
            return ret;
        } catch (IOException e) {
            LOG.error("Failed to get worker user for {}.", workerId);
            return null;
        }
    }

    public static String getIdFromBlobKey(String key) {
        if (key == null) return null;
        final String STORM_JAR_SUFFIX = "-stormjar.jar";
        final String STORM_CODE_SUFFIX = "-stormcode.ser";
        final String STORM_CONF_SUFFIX = "-stormconf.ser";

        String ret = null;
        if (key.endsWith(STORM_JAR_SUFFIX)) {
            ret = key.substring(0, key.length() - STORM_JAR_SUFFIX.length());
        } else if (key.endsWith(STORM_CODE_SUFFIX)) {
            ret = key.substring(0, key.length() - STORM_CODE_SUFFIX.length());
        } else if (key.endsWith(STORM_CONF_SUFFIX)) {
            ret = key.substring(0, key.length() - STORM_CONF_SUFFIX.length());
        }
        return ret;
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static void setWorkerUserWSE(Map conf, String workerId, String user) throws IOException {
        _instance.setWorkerUserWSEImpl(conf, workerId, user);
    }

    public void setWorkerUserWSEImpl(Map conf, String workerId, String user) throws IOException {
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

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String workerArtifactsRoot(Map conf) {
        return _instance.workerArtifactsRootImpl(conf);
    }

    public String workerArtifactsRootImpl(Map conf) {
        String artifactsDir = (String)conf.get(Config.STORM_WORKERS_ARTIFACTS_DIR);
        if (artifactsDir == null) {
            return (getLogDir() + FILE_SEPARATOR + "workers-artifacts");
        } else {
            if (new File(artifactsDir).isAbsolute()) {
                return artifactsDir;
            } else {
                return (getLogDir() + FILE_SEPARATOR + artifactsDir);
            }
        }
    }

    public static String workerArtifactsRoot(Map conf, String id) {
        return (workerArtifactsRoot(conf) + FILE_SEPARATOR + id);
    }

    public static String workerArtifactsRoot(Map conf, String id, Integer port) {
        return (workerArtifactsRoot(conf, id) + FILE_SEPARATOR + port);
    }

    public static String workerArtifactsPidPath(Map conf, String id, Integer port) {
        return (workerArtifactsRoot(conf, id, port) + FILE_SEPARATOR +  "worker.pid");
    }

    public static File getLogMetaDataFile(String fname) {
        String[] subStrings = fname.split(FILE_SEPARATOR); // TODO: does this work well on windows?
        String id = subStrings[0];
        Integer port = Integer.parseInt(subStrings[1]);
        return getLogMetaDataFile(Utils.readStormConfig(), id, port);
    }

    public static File getLogMetaDataFile(Map conf, String id, Integer port) {
        String fname = workerArtifactsRoot(conf, id, port) + FILE_SEPARATOR + "worker.yaml";
        return new File(fname);
    }

    public static File getWorkerDirFromRoot(String logRoot, String id, Integer port) {
        return new File((logRoot + FILE_SEPARATOR + id + FILE_SEPARATOR + port));
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String workerRoot(Map conf) {
        return _instance.workerRootImpl(conf);
    }

    public String workerRootImpl(Map conf) {
        return (absoluteStormLocalDir(conf) + FILE_SEPARATOR + "workers");
    }

    public static String workerRoot(Map conf, String id) {
        return (workerRoot(conf) + FILE_SEPARATOR + id);
    }

    public static String workerPidsRoot(Map conf, String id) {
        return (workerRoot(conf, id) + FILE_SEPARATOR + "pids");
    }

    public static String workerTmpRoot(Map conf, String id) {
        return (workerRoot(conf, id) + FILE_SEPARATOR + "tmp");
    }


    public static String workerPidPath(Map conf, String id, String pid) {
        return (workerPidsRoot(conf, id) + FILE_SEPARATOR + pid);
    }

    public static String workerHeartbeatsRoot(Map conf, String id) {
        return (workerRoot(conf, id) + FILE_SEPARATOR + "heartbeats");
    }

    public static LocalState workerState(Map conf, String id) throws IOException {
        return new LocalState(workerHeartbeatsRoot(conf, id));
    }

    public static Map overrideLoginConfigWithSystemProperty(Map conf) { // note that we delete the return value
        String loginConfFile = System.getProperty("java.security.auth.login.config");
        if (loginConfFile != null) {
             conf.put("java.security.auth.login.config", loginConfFile);
        }
        return conf;
    }

    /* TODO: make sure test these two functions in manual tests */
    public static List<String> getTopoLogsUsers(Map topologyConf) {
        List<String> logsUsers = (List<String>)topologyConf.get(Config.LOGS_USERS);
        List<String> topologyUsers = (List<String>)topologyConf.get(Config.TOPOLOGY_USERS);
        Set<String> mergedUsers = new HashSet<String>();
        if (logsUsers != null) {
            for (String user : logsUsers) {
                if (user != null) {
                    mergedUsers.add(user);
                }
            }
        }
        if (topologyUsers != null) {
            for (String user : topologyUsers) {
                if (user != null) {
                    mergedUsers.add(user);
                }
            }
        }
        List<String> ret = new ArrayList<String>(mergedUsers);
        Collections.sort(ret);
        return ret;
    }

    public static List<String> getTopoLogsGroups(Map topologyConf) {
        List<String> logsGroups = (List<String>)topologyConf.get(Config.LOGS_GROUPS);
        List<String> topologyGroups = (List<String>)topologyConf.get(Config.TOPOLOGY_GROUPS);
        Set<String> mergedGroups = new HashSet<String>();
        if (logsGroups != null) {
            for (String group : logsGroups) {
                if (group != null) {
                    mergedGroups.add(group);
                }
            }
        }
        if (topologyGroups != null) {
            for (String group : topologyGroups) {
                if (group != null) {
                    mergedGroups.add(group);
                }
            }
        }
        List<String> ret = new ArrayList<String>(mergedGroups);
        Collections.sort(ret);
        return ret;
    }
}
