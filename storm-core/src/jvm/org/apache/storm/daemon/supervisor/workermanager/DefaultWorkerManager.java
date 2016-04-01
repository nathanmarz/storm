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
package org.apache.storm.daemon.supervisor.workermanager;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.ProcessSimulator;
import org.apache.storm.container.cgroup.CgroupManager;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DefaultWorkerManager implements IWorkerManager {

    private static Logger LOG = LoggerFactory.getLogger(DefaultWorkerManager.class);

    private Map conf;
    private CgroupManager resourceIsolationManager;
    private boolean runWorkerAsUser;

    @Override
    public void prepareWorker(Map conf, Localizer localizer) {
        this.conf = conf;
        if (Utils.getBoolean(conf.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE), false)) {
            try {
                this.resourceIsolationManager = Utils.newInstance((String) conf.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN));
                this.resourceIsolationManager.prepare(conf);
                LOG.info("Using resource isolation plugin {} {}", conf.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN), resourceIsolationManager);
            } catch (IOException e) {
                throw Utils.wrapInRuntime(e);
            }
        } else {
            this.resourceIsolationManager = null;
        }
        this.runWorkerAsUser = Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false);
    }

    @Override
    public void launchWorker(String supervisorId, String assignmentId, String stormId, Long port, String workerId, WorkerResources resources,
            Utils.ExitCodeCallable workerExitCallback) {
        try {

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
                // set the default heap memory size for supervisor-test
                memOnheap = Utils.getInt(stormConf.get(Config.WORKER_HEAP_MEMORY_MB), 768);
            }

            int memoffheap = (int) Math.ceil(resources.get_mem_off_heap());

            int cpu = (int) Math.ceil(resources.get_cpu());

            List<String> gcOpts = null;

            if (topGcOpts.size() > 0) {
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
            } else {
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
            if (resourceIsolationManager != null) {
                int cGroupMem = (int) (Math.ceil((double) conf.get(Config.STORM_CGROUP_MEMORY_LIMIT_TOLERANCE_MARGIN_MB)));
                int memoryValue = memoffheap + memOnheap + cGroupMem;
                int cpuValue = cpu;
                Map<String, Number> map = new HashMap<>();
                map.put("cpu", cpuValue);
                map.put("memory", memoryValue);
                resourceIsolationManager.reserveResourcesForWorker(workerId, map);
                commandList = resourceIsolationManager.getLaunchCommand(workerId, commandList);
            }

            LOG.info("Launching worker with command: {}. ", Utils.shellCmd(commandList));

            String logPrefix = "Worker Process " + workerId;
            String workerDir = ConfigUtils.workerRoot(conf, workerId);

            if (runWorkerAsUser) {
                List<String> args = new ArrayList<>();
                args.add("worker");
                args.add(workerDir);
                args.add(Utils.writeScript(workerDir, commandList, topEnvironment));
                List<String> commandPrefix = null;
                if (resourceIsolationManager != null)
                    commandPrefix = resourceIsolationManager.getLaunchCommandPrefix(workerId);
                SupervisorUtils.processLauncher(conf, user, commandPrefix, args, null, logPrefix, workerExitCallback, new File(workerDir));
            } else {
                Utils.launchProcess(commandList, topEnvironment, logPrefix, workerExitCallback, new File(workerDir));
            }
        } catch (IOException e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    @Override
    public void shutdownWorker(String supervisorId, String workerId, Map<String, String> workerThreadPids) {
        try {
            LOG.info("Shutting down {}:{}", supervisorId, workerId);
            Collection<String> pids = Utils.readDirContents(ConfigUtils.workerPidsRoot(conf, workerId));
            Integer shutdownSleepSecs = Utils.getInt(conf.get(Config.SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS));
            String user = ConfigUtils.getWorkerUser(conf, workerId);
            String threadPid = workerThreadPids.get(workerId);
            if (StringUtils.isNotBlank(threadPid)) {
                ProcessSimulator.killProcess(threadPid);
            }

            for (String pid : pids) {
                if (runWorkerAsUser) {
                    List<String> commands = new ArrayList<>();
                    commands.add("signal");
                    commands.add(pid);
                    commands.add("15");
                    String logPrefix = "kill -15 " + pid;
                    SupervisorUtils.processLauncherAndWait(conf, user, commands, null, logPrefix);
                } else {
                    Utils.killProcessWithSigTerm(pid);
                }
            }

            if (pids.size() > 0) {
                LOG.info("Sleep {} seconds for execution of cleanup threads on worker.", shutdownSleepSecs);
                Time.sleepSecs(shutdownSleepSecs);
            }

            for (String pid : pids) {
                if (runWorkerAsUser) {
                    List<String> commands = new ArrayList<>();
                    commands.add("signal");
                    commands.add(pid);
                    commands.add("9");
                    String logPrefix = "kill -9 " + pid;
                    SupervisorUtils.processLauncherAndWait(conf, user, commands, null, logPrefix);
                } else {
                    Utils.forceKillProcess(pid);
                }
                String path = ConfigUtils.workerPidPath(conf, workerId, pid);
                if (runWorkerAsUser) {
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
            LOG.info("Shut down {}:{}", supervisorId, workerId);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    @Override
    public boolean cleanupWorker(String workerId) {
        try {
            //clean up for resource isolation if enabled
            if (resourceIsolationManager != null) {
                resourceIsolationManager.releaseResourcesForWorker(workerId);
            }
            //Always make sure to clean up everything else before worker directory
            //is removed since that is what is going to trigger the retry for cleanup
            String workerRoot = ConfigUtils.workerRoot(conf, workerId);
            if (Utils.checkFileExists(workerRoot)) {
                if (runWorkerAsUser) {
                    SupervisorUtils.rmrAsUser(conf, workerId, workerRoot);
                } else {
                    Utils.forceDelete(ConfigUtils.workerHeartbeatsRoot(conf, workerId));
                    Utils.forceDelete(ConfigUtils.workerPidsRoot(conf, workerId));
                    Utils.forceDelete(ConfigUtils.workerTmpRoot(conf, workerId));
                    Utils.forceDelete(ConfigUtils.workerRoot(conf, workerId));
                }
                ConfigUtils.removeWorkerUserWSE(conf, workerId);
            }
            return true;
        } catch (IOException e) {
            LOG.warn("Failed to cleanup worker {}. Will retry later", workerId, e);
        } catch (RuntimeException e) {
            LOG.warn("Failed to cleanup worker {}. Will retry later", workerId, e);
        }
        return false;
    }

    protected String jlp(String stormRoot, Map conf) {
        String resourceRoot = stormRoot + Utils.FILE_PATH_SEPARATOR + ConfigUtils.RESOURCES_SUBDIR;
        String os = System.getProperty("os.name").replaceAll("\\s+", "_");
        String arch = System.getProperty("os.arch");
        String archResourceRoot = resourceRoot + Utils.FILE_PATH_SEPARATOR + os + "-" + arch;
        String ret = archResourceRoot + Utils.CLASS_PATH_SEPARATOR + resourceRoot + Utils.CLASS_PATH_SEPARATOR + conf.get(Config.JAVA_LIBRARY_PATH);
        return ret;
    }

    protected String getWorkerClassPath(String stormJar, Map stormConf) {
        List<String> topoClasspath = new ArrayList<>();
        Object object = stormConf.get(Config.TOPOLOGY_CLASSPATH);

        if (object instanceof List) {
            topoClasspath.addAll((List<String>) object);
        } else if (object instanceof String) {
            topoClasspath.add((String) object);
        }
        LOG.debug("topology specific classpath is {}", object);

        String classPath = Utils.workerClasspath();
        String classAddPath = Utils.addToClasspath(classPath, Arrays.asList(stormJar));
        return Utils.addToClasspath(classAddPath, topoClasspath);
    }

    private static String substituteChildOptsInternal(String string,  String workerId, String stormId, Long port, int memOnheap) {
        if (StringUtils.isNotBlank(string)){
            string = string.replace("%ID%", String.valueOf(port));
            string = string.replace("%WORKER-ID%", workerId);
            string = string.replace("%TOPOLOGY-ID%", stormId);
            string = string.replace("%WORKER-PORT%", String.valueOf(port));
            string = string.replace("%HEAP-MEM%", String.valueOf(memOnheap));
        }
        return string;
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
            String string = substituteChildOptsInternal((String) value,  workerId, stormId, port, memOnheap);
            if (StringUtils.isNotBlank(string)){
                String[] strings = string.split("\\s+");
                rets.addAll(Arrays.asList(strings));
            }
        } else if (value instanceof List) {
            List<Object> objects = (List<Object>) value;
            for (Object object : objects) {
                String str = substituteChildOptsInternal((String) object,  workerId, stormId, port, memOnheap);
                if (StringUtils.isNotBlank(str)){
                    rets.add(str);
                }
            }
        }
        return rets;
    }
}
