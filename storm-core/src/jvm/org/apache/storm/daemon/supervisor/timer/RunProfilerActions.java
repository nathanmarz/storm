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

package org.apache.storm.daemon.supervisor.timer;

import com.google.common.collect.Lists;
import org.apache.storm.Config;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.supervisor.SupervisorData;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class RunProfilerActions implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(RunProfilerActions.class);

    private Map conf;
    private IStormClusterState stormClusterState;
    private String hostName;

    private String profileCmd;

    private SupervisorData supervisorData;

    private class ActionExitCallback implements Utils.ExitCodeCallable {
        private String stormId;
        private ProfileRequest profileRequest;
        private String logPrefix;
        private boolean stop;

        public ActionExitCallback(String stormId, ProfileRequest profileRequest, String logPrefix, boolean stop) {
            this.stormId = stormId;
            this.profileRequest = profileRequest;
            this.logPrefix = logPrefix;
            this.stop = stop;
        }

        @Override
        public Object call() throws Exception {
            return null;
        }

        @Override
        public Object call(int exitCode) {
            LOG.info("{} profile-action exited for {}", logPrefix, exitCode);
            try {
                if (stop)
                    stormClusterState.deleteTopologyProfileRequests(stormId, profileRequest);
            } catch (Exception e) {
                LOG.warn("failed delete profileRequest: " + profileRequest);
            }
            return null;
        }
    }

    public RunProfilerActions(SupervisorData supervisorData) {
        this.conf = supervisorData.getConf();
        this.stormClusterState = supervisorData.getStormClusterState();
        this.hostName = supervisorData.getHostName();
        String stormHome = System.getProperty("storm.home");
        this.profileCmd = stormHome + Utils.FILE_PATH_SEPARATOR + "bin" + Utils.FILE_PATH_SEPARATOR + conf.get(Config.WORKER_PROFILER_COMMAND);
        this.supervisorData = supervisorData;
    }

    @Override
    public void run() {
        Map<String, List<ProfileRequest>> stormIdToActions = supervisorData.getStormIdToProfilerActions().get();
        try {
            for (Map.Entry<String, List<ProfileRequest>> entry : stormIdToActions.entrySet()) {
                String stormId = entry.getKey();
                List<ProfileRequest> requests = entry.getValue();
                if (requests != null) {
                    for (ProfileRequest profileRequest : requests) {
                        if (profileRequest.get_nodeInfo().get_node().equals(hostName)) {
                            boolean stop = System.currentTimeMillis() > profileRequest.get_time_stamp();
                            Long port = profileRequest.get_nodeInfo().get_port().iterator().next();
                            String targetDir = ConfigUtils.workerArtifactsRoot(conf, stormId, port.intValue());
                            Map stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);

                            String user = null;
                            if (stormConf.get(Config.TOPOLOGY_SUBMITTER_USER) != null) {
                                user = (String) (stormConf.get(Config.TOPOLOGY_SUBMITTER_USER));
                            }
                            Map<String, String> env = null;
                            if (stormConf.get(Config.TOPOLOGY_ENVIRONMENT) != null) {
                                env = (Map<String, String>) stormConf.get(Config.TOPOLOGY_ENVIRONMENT);
                            } else {
                                env = new HashMap<String, String>();
                            }

                            String str = ConfigUtils.workerArtifactsPidPath(conf, stormId, port.intValue());
                            StringBuilder stringBuilder = new StringBuilder();

                            String workerPid = null;
                            try (FileReader reader = new FileReader(str); BufferedReader br = new BufferedReader(reader)) {
                                workerPid = br.readLine().trim();
                            }
                            ProfileAction profileAction = profileRequest.get_action();
                            String logPrefix = "ProfilerAction process " + stormId + ":" + port + " PROFILER_ACTION: " + profileAction + " ";

                            // Until PROFILER_STOP action is invalid, keep launching profiler start in case worker restarted
                            // The profiler plugin script validates if JVM is recording before starting another recording.
                            List<String> command = mkCommand(profileAction, stop, workerPid, targetDir);
                            try {
                                ActionExitCallback actionExitCallback = new ActionExitCallback(stormId, profileRequest, logPrefix, stop);
                                launchProfilerActionForWorker(user, targetDir, command, env, actionExitCallback, logPrefix);
                            } catch (IOException e) {
                                LOG.error("Error in processing ProfilerAction '{}' for {}:{}, will retry later", profileAction, stormId, port);
                            } catch (RuntimeException e) {
                                LOG.error("Error in processing ProfilerAction '{}' for {}:{}, will retry later", profileAction, stormId, port);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error running profiler actions, will retry again later");
        }
    }

    private void launchProfilerActionForWorker(String user, String targetDir, List<String> commands, Map<String, String> environment,
            final Utils.ExitCodeCallable exitCodeCallable, String logPrefix) throws IOException {
        File targetFile = new File(targetDir);
        if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            LOG.info("Running as user:{} command:{}", user, commands);
            String containerFile = Utils.containerFilePath(targetDir);
            if (Utils.checkFileExists(containerFile)) {
                SupervisorUtils.rmrAsUser(conf, containerFile, containerFile);
            }
            String scriptFile = Utils.scriptFilePath(targetDir);
            if (Utils.checkFileExists(scriptFile)) {
                SupervisorUtils.rmrAsUser(conf, scriptFile, scriptFile);
            }
            String script = Utils.writeScript(targetDir, commands, environment);
            List<String> args = new ArrayList<>();
            args.add("profiler");
            args.add(targetDir);
            args.add(script);
            SupervisorUtils.processLauncher(conf, user, null, args, environment, logPrefix, exitCodeCallable, targetFile);
        } else {
            Utils.launchProcess(commands, environment, logPrefix, exitCodeCallable, targetFile);
        }
    }

    private List<String> mkCommand(ProfileAction action, boolean stop, String workerPid, String targetDir) {
        if (action == ProfileAction.JMAP_DUMP) {
            return jmapDumpCmd(workerPid, targetDir);
        } else if (action == ProfileAction.JSTACK_DUMP) {
            return jstackDumpCmd(workerPid, targetDir);
        } else if (action == ProfileAction.JPROFILE_DUMP) {
            return jprofileDump(workerPid, targetDir);
        } else if (action == ProfileAction.JVM_RESTART) {
            return jprofileJvmRestart(workerPid);
        } else if (!stop && action == ProfileAction.JPROFILE_STOP) {
            return jprofileStart(workerPid);
        } else if (stop && action == ProfileAction.JPROFILE_STOP) {
            return jprofileStop(workerPid, targetDir);
        }
        return Lists.newArrayList();
    }

    private List<String> jmapDumpCmd(String pid, String targetDir) {
        return Lists.newArrayList(profileCmd, pid, "jmap", targetDir);
    }

    private List<String> jstackDumpCmd(String pid, String targetDir) {
        return Lists.newArrayList(profileCmd, pid, "jstack", targetDir);
    }

    private List<String> jprofileStart(String pid) {
        return Lists.newArrayList(profileCmd, pid, "start");
    }

    private List<String> jprofileStop(String pid, String targetDir) {
        return Lists.newArrayList(profileCmd, pid, "stop", targetDir);
    }

    private List<String> jprofileDump(String pid, String targetDir) {
        return Lists.newArrayList(profileCmd, pid, "dump", targetDir);
    }

    private List<String> jprofileJvmRestart(String pid) {
        return Lists.newArrayList(profileCmd, pid, "kill");
    }

}
