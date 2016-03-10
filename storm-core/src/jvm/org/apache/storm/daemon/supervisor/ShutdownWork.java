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
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.*;

public  class ShutdownWork implements Shutdownable {

    private static Logger LOG = LoggerFactory.getLogger(ShutdownWork.class);

    public void shutWorker(SupervisorData supervisorData, String workerId) throws IOException, InterruptedException {
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

    protected void tryCleanupWorker(Map conf, SupervisorData supervisorData, String workerId) {
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

    @Override
    public void shutdown() {
    }
}