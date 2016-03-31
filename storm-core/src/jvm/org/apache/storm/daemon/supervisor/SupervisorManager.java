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

import org.apache.storm.daemon.DaemonCommon;
import org.apache.storm.daemon.supervisor.workermanager.IWorkerManager;
import org.apache.storm.event.EventManager;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class SupervisorManager implements SupervisorDaemon, DaemonCommon, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorManager.class);
    private final EventManager eventManager;
    private final EventManager processesEventManager;
    private final SupervisorData supervisorData;

    public SupervisorManager(SupervisorData supervisorData, EventManager eventManager, EventManager processesEventManager) {
        this.eventManager = eventManager;
        this.supervisorData = supervisorData;
        this.processesEventManager = processesEventManager;
    }

    public void shutdown() {
        LOG.info("Shutting down supervisor {}", supervisorData.getSupervisorId());
        supervisorData.setActive(false);
        try {
            supervisorData.getHeartbeatTimer().close();
            supervisorData.getEventTimer().close();
            supervisorData.getBlobUpdateTimer().close();
            eventManager.close();
            processesEventManager.close();
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        supervisorData.getStormClusterState().disconnect();
    }

    @Override
    public void shutdownAllWorkers() {
        IWorkerManager workerManager = supervisorData.getWorkerManager();
        SupervisorUtils.shutdownAllWorkers(supervisorData.getConf(), supervisorData.getSupervisorId(), supervisorData.getWorkerThreadPids(),
                supervisorData.getDeadWorkers(), workerManager);
    }

    @Override
    public Map getConf() {
        return supervisorData.getConf();
    }

    @Override
    public String getId() {
        return supervisorData.getSupervisorId();
    }

    @Override
    public boolean isWaiting() {
        if (!supervisorData.isActive()) {
            return true;
        }

        if (supervisorData.getHeartbeatTimer().isTimerWaiting() && supervisorData.getEventTimer().isTimerWaiting() && eventManager.waiting()
                && processesEventManager.waiting()) {
            return true;
        }
        return false;
    }

    public void run() {
        shutdown();
    }

}
