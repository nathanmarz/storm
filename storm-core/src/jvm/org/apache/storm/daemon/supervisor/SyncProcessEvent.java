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
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.container.cgroup.CgroupManager;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * 1. to kill are those in allocated that are dead or disallowed 2. kill the ones that should be dead - read pids, kill -9 and individually remove file - rmr
 * heartbeat dir, rmdir pid dir, rmdir id dir (catch exception and log) 3. of the rest, figure out what assignments aren't yet satisfied 4. generate new worker
 * ids, write new "approved workers" to LS 5. create local dir for worker id 5. launch new workers (give worker-id, port, and supervisor-id) 6. wait for workers
 * launch
 */
public class SyncProcessEvent implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(SyncProcessEvent.class);

    private  LocalState localState;
    private  SupervisorData supervisorData;
    public static final ExecutorInfo SYSTEM_EXECUTOR_INFO = new ExecutorInfo(-1, -1);

    private class ProcessExitCallback implements Utils.ExitCodeCallable {
        private final String logPrefix;
        private final String workerId;

        public ProcessExitCallback(String logPrefix, String workerId) {
            this.logPrefix = logPrefix;
            this.workerId = workerId;
        }

        @Override
        public Object call() throws Exception {
            return null;
        }

        @Override
        public Object call(int exitCode) {
            LOG.info("{} exited with code: {}", logPrefix, exitCode);
            supervisorData.getDeadWorkers().add(workerId);
            return null;
        }
    }

    public SyncProcessEvent(){

    }
    public SyncProcessEvent(SupervisorData supervisorData) {
        init(supervisorData);
    }

    //TODO: initData is intended to local supervisor, so we will remove them after porting worker.clj to java
    public void init(SupervisorData supervisorData){
        this.supervisorData = supervisorData;
        this.localState = supervisorData.getLocalState();
    }


    /**
     * 1. to kill are those in allocated that are dead or disallowed 2. kill the ones that should be dead - read pids, kill -9 and individually remove file -
     * rmr heartbeat dir, rmdir pid dir, rmdir id dir (catch exception and log) 3. of the rest, figure out what assignments aren't yet satisfied 4. generate new
     * worker ids, write new "approved workers" to LS 5. create local dir for worker id 5. launch new workers (give worker-id, port, and supervisor-id) 6. wait
     * for workers launch
     */
    @Override
    public void run() {
        LOG.debug("Syncing processes");
        try {
            Map conf = supervisorData.getConf();
            Map<Integer, LocalAssignment> assignedExecutors = localState.getLocalAssignmentsMap();

            if (assignedExecutors == null) {
                assignedExecutors = new HashMap<>();
            }
            int now = Time.currentTimeSecs();

            Map<String, StateHeartbeat> localWorkerStats = getLocalWorkerStats(supervisorData, assignedExecutors, now);

            Set<String> keeperWorkerIds = new HashSet<>();
            Set<Integer> keepPorts = new HashSet<>();
            for (Map.Entry<String, StateHeartbeat> entry : localWorkerStats.entrySet()) {
                StateHeartbeat stateHeartbeat = entry.getValue();
                if (stateHeartbeat.getState() == State.VALID) {
                    keeperWorkerIds.add(entry.getKey());
                    keepPorts.add(stateHeartbeat.getHeartbeat().get_port());
                }
            }
            Map<Integer, LocalAssignment> reassignExecutors = getReassignExecutors(assignedExecutors, keepPorts);
            Map<Integer, String> newWorkerIds = new HashMap<>();
            for (Integer port : reassignExecutors.keySet()) {
                newWorkerIds.put(port, Utils.uuid());
            }
            LOG.debug("Syncing processes");
            LOG.debug("Assigned executors: {}", assignedExecutors);
            LOG.debug("Allocated: {}", localWorkerStats);

            for (Map.Entry<String, StateHeartbeat> entry : localWorkerStats.entrySet()) {
                StateHeartbeat stateHeartbeat = entry.getValue();
                if (stateHeartbeat.getState() != State.VALID) {
                    LOG.info("Shutting down and clearing state for id {}, Current supervisor time: {}, State: {}, Heartbeat: {}", entry.getKey(), now,
                            stateHeartbeat.getState(), stateHeartbeat.getHeartbeat());
                    shutWorker(supervisorData, entry.getKey());
                }
            }
            // start new workers
            Map<String, Integer> newWorkerPortToIds = startNewWorkers(newWorkerIds, reassignExecutors);

            Map<String, Integer> allWorkerPortToIds = new HashMap<>();
            Map<String, Integer> approvedWorkers = localState.getApprovedWorkers();
            for (String keeper : keeperWorkerIds) {
                allWorkerPortToIds.put(keeper, approvedWorkers.get(keeper));
            }
            allWorkerPortToIds.putAll(newWorkerPortToIds);
            localState.setApprovedWorkers(allWorkerPortToIds);
            waitForWorkersLaunch(conf, newWorkerPortToIds.keySet());

        } catch (Exception e) {
            LOG.error("Failed Sync Process", e);
            throw Utils.wrapInRuntime(e);
        }

    }

    protected void waitForWorkersLaunch(Map conf, Set<String> workerIds) throws Exception {
        int startTime = Time.currentTimeSecs();
        int timeOut = (int) conf.get(Config.NIMBUS_SUPERVISOR_TIMEOUT_SECS);
        for (String workerId : workerIds) {
            LocalState localState = ConfigUtils.workerState(conf, workerId);
            while (true) {
                LSWorkerHeartbeat hb = localState.getWorkerHeartBeat();
                if (hb != null || (Time.currentTimeSecs() - startTime) > timeOut)
                    break;
                LOG.info("{} still hasn't started", workerId);
                Time.sleep(500);
            }
            if (localState.getWorkerHeartBeat() == null) {
                LOG.info("Worker {} failed to start", workerId);
            }
        }
    }

    protected Map<Integer, LocalAssignment> getReassignExecutors(Map<Integer, LocalAssignment> assignExecutors, Set<Integer> keepPorts) {
        Map<Integer, LocalAssignment> reassignExecutors = new HashMap<>();
        reassignExecutors.putAll(assignExecutors);
        for (Integer port : keepPorts) {
            reassignExecutors.remove(port);
        }
        return reassignExecutors;
    }
    
    /**
     * Returns map from worker id to worker heartbeat. if the heartbeat is nil, then the worker is dead
     * 
     * @param assignedExecutors
     * @return
     * @throws Exception
     */
    public Map<String, StateHeartbeat> getLocalWorkerStats(SupervisorData supervisorData, Map<Integer, LocalAssignment> assignedExecutors, int now) throws Exception {
        Map<String, StateHeartbeat> workerIdHbstate = new HashMap<>();
        Map conf = supervisorData.getConf();
        LocalState localState = supervisorData.getLocalState();
        Map<String, LSWorkerHeartbeat> idToHeartbeat = SupervisorUtils.readWorkerHeartbeats(conf);
        Map<String, Integer> approvedWorkers = localState.getApprovedWorkers();
        Set<String> approvedIds = new HashSet<>();
        if (approvedWorkers != null) {
            approvedIds.addAll(approvedWorkers.keySet());
        }
        for (Map.Entry<String, LSWorkerHeartbeat> entry : idToHeartbeat.entrySet()) {
            String workerId = entry.getKey();
            LSWorkerHeartbeat whb = entry.getValue();
            State state;
            if (whb == null) {
                state = State.NOT_STARTED;
            } else if (!approvedIds.contains(workerId) || !matchesAssignment(whb, assignedExecutors)) {
                state = State.DISALLOWED;
            } else if (supervisorData.getDeadWorkers().contains(workerId)) {
                LOG.info("Worker Process {} has died", workerId);
                state = State.TIMED_OUT;
            } else if (SupervisorUtils.isWorkerHbTimedOut(now, whb, conf)) {
                state = State.TIMED_OUT;
            } else {
                state = State.VALID;
            }
            LOG.debug("Worker:{} state:{} WorkerHeartbeat:{} at supervisor time-secs {}", workerId, state, whb, now);
            workerIdHbstate.put(workerId, new StateHeartbeat(state, whb));
        }
        return workerIdHbstate;
    }

    protected boolean matchesAssignment(LSWorkerHeartbeat whb, Map<Integer, LocalAssignment> assignedExecutors) {
        LocalAssignment localAssignment = assignedExecutors.get(whb.get_port());
        if (localAssignment == null || !localAssignment.get_topology_id().equals(whb.get_topology_id())) {
            return false;
        }
        List<ExecutorInfo> executorInfos = new ArrayList<>();
        executorInfos.addAll(whb.get_executors());
        // remove SYSTEM_EXECUTOR_ID
        executorInfos.remove(SYSTEM_EXECUTOR_INFO);
        List<ExecutorInfo> localExecuorInfos = localAssignment.get_executors();

        if (localExecuorInfos.size() != executorInfos.size())
            return false;

        for (ExecutorInfo executorInfo : localExecuorInfos){
            if (!localExecuorInfos.contains(executorInfo))
                return false;
        }
        return true;
    }

    /**
     * launch a worker in local mode.
     */
    protected void launchWorker(SupervisorData supervisorData, String stormId, Long port, String workerId, WorkerResources resources) throws IOException {
        // port this function after porting worker to java
    }

    protected String getWorkerClassPath(String stormJar, Map stormConf) {
        List<String> topoClasspath = new ArrayList<>();
        Object object = stormConf.get(Config.TOPOLOGY_CLASSPATH);

        if (object instanceof List) {
            topoClasspath.addAll((List<String>) object);
        } else if (object instanceof String){
            topoClasspath.add((String)object);
        }else {
            //ignore
        }
        String classPath = Utils.workerClasspath();
        String classAddPath = Utils.addToClasspath(classPath, Arrays.asList(stormJar));
        return Utils.addToClasspath(classAddPath, topoClasspath);
    }

    /**
     * "Generates runtime childopts by replacing keys with topology-id, worker-id, port, mem-onheap"
     * 
     * @param value
     * @param workerId
     * @param stormId
     * @param port
     * @param memOnheap
     */
    public List<String> substituteChildopts(Object value, String workerId, String stormId, Long port, int memOnheap) {
        List<String> rets = new ArrayList<>();
        if (value instanceof String) {
            String string = (String) value;
            string = string.replace("%ID%", String.valueOf(port));
            string = string.replace("%WORKER-ID%", workerId);
            string = string.replace("%TOPOLOGY-ID%", stormId);
            string = string.replace("%WORKER-PORT%", String.valueOf(port));
            string = string.replace("%HEAP-MEM%", String.valueOf(memOnheap));
            String[] strings = string.split("\\s+");
            rets.addAll(Arrays.asList(strings));
        } else if (value instanceof List) {
            List<Object> objects = (List<Object>) value;
            for (Object object : objects) {
                String str = (String)object;
                str = str.replace("%ID%", String.valueOf(port));
                str = str.replace("%WORKER-ID%", workerId);
                str = str.replace("%TOPOLOGY-ID%", stormId);
                str = str.replace("%WORKER-PORT%", String.valueOf(port));
                str = str.replace("%HEAP-MEM%", String.valueOf(memOnheap));
                rets.add(str);
            }
        }
        return rets;
    }



    /**
     * launch a worker in distributed mode
     * supervisorId for testing
     * @throws IOException
     */
    protected void launchWorker(Map conf, String supervisorId, String assignmentId, String stormId, Long port, String workerId,
            WorkerResources resources, CgroupManager cgroupManager, ConcurrentHashSet deadWorkers) throws IOException {

        Boolean runWorkerAsUser = Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false);
        String stormHome = ConfigUtils.concatIfNotNull(System.getProperty("storm.home"));
        String stormOptions = ConfigUtils.concatIfNotNull(System.getProperty("storm.options"));
        String stormConfFile = ConfigUtils.concatIfNotNull(System.getProperty("storm.conf.file"));
        String workerTmpDir = ConfigUtils.workerTmpRoot(conf, workerId);

        String stormLogDir = ConfigUtils.getLogDir();
        String stormLogConfDir = (String) (conf.get(Config.STORM_LOG4J2_CONF_DIR));

        String stormLog4j2ConfDir;
        if (StringUtils.isNotBlank(stormLogConfDir)) {
            if (Utils.isAbsolutePath(stormLogConfDir)) {
                stormLog4j2ConfDir = stormLogConfDir;
            } else {
                stormLog4j2ConfDir = stormHome + Utils.FILE_PATH_SEPARATOR + stormLogConfDir;
            }
        } else {
            stormLog4j2ConfDir = stormHome + Utils.FILE_PATH_SEPARATOR + "log4j2";
        }

        String stormRoot = ConfigUtils.supervisorStormDistRoot(conf, stormId);

        String jlp = jlp(stormRoot, conf);

        String stormJar = ConfigUtils.supervisorStormJarPath(stormRoot);

        Map stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);

        String workerClassPath = getWorkerClassPath(stormJar, stormConf);

        Object topGcOptsObject = stormConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS);
        List<String> topGcOpts = new ArrayList<>();
        if (topGcOptsObject instanceof String) {
            topGcOpts.add((String) topGcOptsObject);
        } else if (topGcOptsObject instanceof List) {
            topGcOpts.addAll((List<String>) topGcOptsObject);
        }

        int memOnheap = 0;
        if (resources.get_mem_on_heap() > 0) {
            memOnheap = (int) Math.ceil(resources.get_mem_on_heap());
        } else {
            //set the default heap memory size for supervisor-test
            memOnheap = Utils.getInt(stormConf.get(Config.WORKER_HEAP_MEMORY_MB), 768);
        }

        int memoffheap = (int) Math.ceil(resources.get_mem_off_heap());

        int cpu = (int) Math.ceil(resources.get_cpu());

        List<String> gcOpts = null;

        if (topGcOpts != null) {
            gcOpts = substituteChildopts(topGcOpts, workerId, stormId, port, memOnheap);
        } else {
            gcOpts = substituteChildopts(conf.get(Config.WORKER_GC_CHILDOPTS), workerId, stormId, port, memOnheap);
        }

        Object topoWorkerLogwriterObject = stormConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS);
        List<String> topoWorkerLogwriterChildopts = new ArrayList<>();
        if (topoWorkerLogwriterObject instanceof String) {
            topoWorkerLogwriterChildopts.add((String) topoWorkerLogwriterObject);
        } else if (topoWorkerLogwriterObject instanceof List) {
            topoWorkerLogwriterChildopts.addAll((List<String>) topoWorkerLogwriterObject);
        }

        String user = (String) stormConf.get(Config.TOPOLOGY_SUBMITTER_USER);

        String logfileName = "worker.log";

        String workersArtifacets = ConfigUtils.workerArtifactsRoot(conf);

        String loggingSensitivity = (String) stormConf.get(Config.TOPOLOGY_LOGGING_SENSITIVITY);
        if (loggingSensitivity == null) {
            loggingSensitivity = "S3";
        }

        List<String> workerChildopts = substituteChildopts(conf.get(Config.WORKER_CHILDOPTS), workerId, stormId, port, memOnheap);

        List<String> topWorkerChildopts = substituteChildopts(stormConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS), workerId, stormId, port, memOnheap);

        List<String> workerProfilerChildopts = null;
        if (Utils.getBoolean(conf.get(Config.WORKER_PROFILER_ENABLED), false)) {
            workerProfilerChildopts = substituteChildopts(conf.get(Config.WORKER_PROFILER_CHILDOPTS), workerId, stormId, port, memOnheap);
        }else {
            workerProfilerChildopts = new ArrayList<>();
        }

        Map<String, String> topEnvironment = new HashMap<String, String>();
        Map<String, String> environment = (Map<String, String>) stormConf.get(Config.TOPOLOGY_ENVIRONMENT);
        if (environment != null) {
            topEnvironment.putAll(environment);
        }
        topEnvironment.put("LD_LIBRARY_PATH", jlp);

        String log4jConfigurationFile = null;
        if (System.getProperty("os.name").startsWith("Windows") && !stormLog4j2ConfDir.startsWith("file:")) {
            log4jConfigurationFile = "file:///" + stormLog4j2ConfDir;
        } else {
            log4jConfigurationFile = stormLog4j2ConfDir;
        }
        log4jConfigurationFile = log4jConfigurationFile + Utils.FILE_PATH_SEPARATOR + "worker.xml";

        List<String> commandList = new ArrayList<>();
        commandList.add(SupervisorUtils.javaCmd("java"));
        commandList.add("-cp");
        commandList.add(workerClassPath);
        commandList.addAll(topoWorkerLogwriterChildopts);
        commandList.add("-Dlogfile.name=" + logfileName);
        commandList.add("-Dstorm.home=" + stormHome);
        commandList.add("-Dworkers.artifacts=" + workersArtifacets);
        commandList.add("-Dstorm.id=" + stormId);
        commandList.add("-Dworker.id=" + workerId);
        commandList.add("-Dworker.port=" + port);
        commandList.add("-Dstorm.log.dir=" + stormLogDir);
        commandList.add("-Dlog4j.configurationFile=" + log4jConfigurationFile);
        commandList.add("-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector");
        commandList.add("org.apache.storm.LogWriter");

        commandList.add(SupervisorUtils.javaCmd("java"));
        commandList.add("-server");
        commandList.addAll(workerChildopts);
        commandList.addAll(topWorkerChildopts);
        commandList.addAll(gcOpts);
        commandList.addAll(workerProfilerChildopts);
        commandList.add("-Djava.library.path=" + jlp);
        commandList.add("-Dlogfile.name=" + logfileName);
        commandList.add("-Dstorm.home=" + stormHome);
        commandList.add("-Dworkers.artifacts=" + workersArtifacets);
        commandList.add("-Dstorm.conf.file=" + stormConfFile);
        commandList.add("-Dstorm.options=" + stormOptions);
        commandList.add("-Dstorm.log.dir=" + stormLogDir);
        commandList.add("-Djava.io.tmpdir=" + workerTmpDir);
        commandList.add("-Dlogging.sensitivity=" + loggingSensitivity);
        commandList.add("-Dlog4j.configurationFile=" + log4jConfigurationFile);
        commandList.add("-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector");
        commandList.add("-Dstorm.id=" + stormId);
        commandList.add("-Dworker.id=" + workerId);
        commandList.add("-Dworker.port=" + port);
        commandList.add("-cp");
        commandList.add(workerClassPath);
        commandList.add("org.apache.storm.daemon.worker");
        commandList.add(stormId);
        commandList.add(assignmentId);
        commandList.add(String.valueOf(port));
        commandList.add(workerId);

        // {"cpu" cpu "memory" (+ mem-onheap mem-offheap (int (Math/ceil (conf STORM-CGROUP-MEMORY-LIMIT-TOLERANCE-MARGIN-MB))))
        if (Utils.getBoolean(conf.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE), false)) {
            int cgRoupMem = (int) (Math.ceil((double) conf.get(Config.STORM_CGROUP_MEMORY_LIMIT_TOLERANCE_MARGIN_MB)));
            int memoryValue = memoffheap + memOnheap + cgRoupMem;
            int cpuValue = cpu;
            Map<String, Number> map = new HashMap<>();
            map.put("cpu", cpuValue);
            map.put("memory", memoryValue);
            cgroupManager.reserveResourcesForWorker(workerId, map);
            commandList = cgroupManager.getLaunchCommand(workerId, commandList);
        }

        LOG.info("Launching worker with command: {}. ", Utils.shellCmd(commandList));
        writeLogMetadata(stormConf, user, workerId, stormId, port, conf);
        ConfigUtils.setWorkerUserWSE(conf, workerId, user);
        createArtifactsLink(conf, stormId, port, workerId);

        String logPrefix = "Worker Process " + workerId;
        String workerDir = ConfigUtils.workerRoot(conf, workerId);

        if (deadWorkers != null)
            deadWorkers.remove(workerId);
        createBlobstoreLinks(conf, stormId, workerId);

        ProcessExitCallback processExitCallback = new ProcessExitCallback(logPrefix, workerId);
        if (runWorkerAsUser) {
            List<String> args = new ArrayList<>();
            args.add("worker");
            args.add(workerDir);
            args.add(Utils.writeScript(workerDir, commandList, topEnvironment));
            SupervisorUtils.workerLauncher(conf, user, args, null, logPrefix, processExitCallback, new File(workerDir));
        } else {
            Utils.launchProcess(commandList, topEnvironment, logPrefix, processExitCallback, new File(workerDir));
        }
    }

    protected String jlp(String stormRoot, Map conf) {
        String resourceRoot = stormRoot + Utils.FILE_PATH_SEPARATOR + ConfigUtils.RESOURCES_SUBDIR;
        String os = System.getProperty("os.name").replaceAll("\\s+", "_");
        String arch = System.getProperty("os.arch");
        String archResourceRoot = resourceRoot + Utils.FILE_PATH_SEPARATOR + os + "-" + arch;
        String ret = archResourceRoot + Utils.FILE_PATH_SEPARATOR + resourceRoot + Utils.FILE_PATH_SEPARATOR + conf.get(Config.JAVA_LIBRARY_PATH);
        return ret;
    }

    protected Map<String, Integer> startNewWorkers(Map<Integer, String> newWorkerIds, Map<Integer, LocalAssignment> reassignExecutors) throws IOException {

        Map<String, Integer> newValidWorkerIds = new HashMap<>();
        Map conf = supervisorData.getConf();
        String supervisorId = supervisorData.getSupervisorId();
        String clusterMode = ConfigUtils.clusterMode(conf);

        for (Map.Entry<Integer, LocalAssignment> entry : reassignExecutors.entrySet()) {
            Integer port = entry.getKey();
            LocalAssignment assignment = entry.getValue();
            String workerId = newWorkerIds.get(port);
            String stormId = assignment.get_topology_id();
            WorkerResources resources = assignment.get_resources();

            // This condition checks for required files exist before launching the worker
            if (SupervisorUtils.doRequiredTopoFilesExist(conf, stormId)) {
                String pidsPath = ConfigUtils.workerPidsRoot(conf, workerId);
                String hbPath = ConfigUtils.workerHeartbeatsRoot(conf, workerId);

                LOG.info("Launching worker with assignment {} for this supervisor {} on port {} with id {}", assignment, supervisorData.getSupervisorId(), port,
                        workerId);

                FileUtils.forceMkdir(new File(pidsPath));
                FileUtils.forceMkdir(new File(ConfigUtils.workerTmpRoot(conf, workerId)));
                FileUtils.forceMkdir(new File(hbPath));

                if (clusterMode.endsWith("distributed")) {
                    launchWorker(conf, supervisorId, supervisorData.getAssignmentId(), stormId, port.longValue(), workerId, resources,
                            supervisorData.getResourceIsolationManager(), supervisorData.getDeadWorkers());
                } else if (clusterMode.endsWith("local")) {
                    launchWorker(supervisorData, stormId, port.longValue(), workerId, resources);
                }
                newValidWorkerIds.put(workerId, port);

            } else {
                LOG.info("Missing topology storm code, so can't launch worker with assignment {} for this supervisor {} on port {} with id {}", assignment,
                        supervisorData.getSupervisorId(), port, workerId);
            }

        }
        return newValidWorkerIds;
    }

    public void writeLogMetadata(Map stormconf, String user, String workerId, String stormId, Long port, Map conf) throws IOException {
        Map data = new HashMap();
        data.put(Config.TOPOLOGY_SUBMITTER_USER, user);
        data.put("worker-id", workerId);

        Set<String> logsGroups = new HashSet<>();
        //for supervisor-test
        if (stormconf.get(Config.LOGS_GROUPS) != null) {
            List<String> groups = (List<String>) stormconf.get(Config.LOGS_GROUPS);
            for (String group : groups){
                logsGroups.add(group);
            }
        }
        if (stormconf.get(Config.TOPOLOGY_GROUPS) != null) {
            List<String> topGroups = (List<String>) stormconf.get(Config.TOPOLOGY_GROUPS);
            for (String group : topGroups){
                logsGroups.add(group);
            }
        }
        data.put(Config.LOGS_GROUPS, logsGroups.toArray());

        Set<String> logsUsers = new HashSet<>();
        if (stormconf.get(Config.LOGS_USERS) != null) {
            List<String> logUsers = (List<String>) stormconf.get(Config.LOGS_USERS);
            for (String logUser : logUsers){
                logsUsers.add(logUser);
            }
        }
        if (stormconf.get(Config.TOPOLOGY_USERS) != null) {
            List<String> topUsers = (List<String>) stormconf.get(Config.TOPOLOGY_USERS);
            for (String logUser : topUsers){
                logsUsers.add(logUser);
            }
        }
        data.put(Config.LOGS_USERS, logsUsers.toArray());
        writeLogMetadataToYamlFile(stormId, port, data, conf);
    }

    /**
     * run worker as user needs the directory to have special permissions or it is insecure
     * 
     * @param stormId
     * @param port
     * @param data
     * @param conf
     * @throws IOException
     */
    public void writeLogMetadataToYamlFile(String stormId, Long port, Map data, Map conf) throws IOException {
        File file = ConfigUtils.getLogMetaDataFile(conf, stormId, port.intValue());

        if (!Utils.checkFileExists(file.getParent())) {
            if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
                FileUtils.forceMkdir(file.getParentFile());
                SupervisorUtils.setupStormCodeDir(conf, ConfigUtils.readSupervisorStormConf(conf, stormId), file.getParentFile().getCanonicalPath());
            } else {
                file.getParentFile().mkdirs();
            }
        }
        FileWriter writer = new FileWriter(file);
        Yaml yaml = new Yaml();
        try {
            yaml.dump(data, writer);
        }finally {
            writer.close();
        }

    }

    /**
     * Create a symlink from workder directory to its port artifacts directory
     * 
     * @param conf
     * @param stormId
     * @param port
     * @param workerId
     */
    protected void createArtifactsLink(Map conf, String stormId, Long port, String workerId) throws IOException {
        String workerDir = ConfigUtils.workerRoot(conf, workerId);
        String topoDir = ConfigUtils.workerArtifactsRoot(conf, stormId);
        if (Utils.checkFileExists(workerDir)) {
            Utils.createSymlink(workerDir, topoDir, "artifacts", String.valueOf(port));
        }
    }

    /**
     * Create symlinks in worker launch directory for all blobs
     * 
     * @param conf
     * @param stormId
     * @param workerId
     * @throws IOException
     */
    protected void createBlobstoreLinks(Map conf, String stormId, String workerId) throws IOException {
        String stormRoot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
        Map stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);
        String workerRoot = ConfigUtils.workerRoot(conf, workerId);
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
        List<String> resourceFileNames = new ArrayList<>();
        resourceFileNames.add(ConfigUtils.RESOURCES_SUBDIR);
        resourceFileNames.addAll(blobFileNames);
        LOG.info("Creating symlinks for worker-id: {} storm-id: {} for files({}): {}", workerId, stormId, resourceFileNames.size(), resourceFileNames);
        Utils.createSymlink(workerRoot, stormRoot, ConfigUtils.RESOURCES_SUBDIR);
        for (String fileName : blobFileNames) {
            Utils.createSymlink(workerRoot, stormRoot, fileName, fileName);
        }
    }

    //for supervisor-test
    public void shutWorker(SupervisorData supervisorData, String workerId) throws IOException, InterruptedException{
        SupervisorUtils.shutWorker(supervisorData, workerId);
    }
}
