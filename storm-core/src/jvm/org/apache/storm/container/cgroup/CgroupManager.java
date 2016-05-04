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

package org.apache.storm.container.cgroup;

import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.Config;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class that implements ResourceIsolationInterface that manages cgroups
 */
public class CgroupManager implements ResourceIsolationInterface {

    private static final Logger LOG = LoggerFactory.getLogger(CgroupManager.class);

    private CgroupCenter center;

    private Hierarchy hierarchy;

    private CgroupCommon rootCgroup;

    private static String rootDir;

    private Map conf;

    /**
     * initialize intial data structures
     * @param conf storm confs
     */
    public void prepare(Map conf) throws IOException {
        this.conf = conf;
        this.rootDir = Config.getCgroupRootDir(this.conf);
        if (this.rootDir == null) {
            throw new RuntimeException("Check configuration file. The storm.supervisor.cgroup.rootdir is missing.");
        }

        File file = new File(Config.getCgroupStormHierarchyDir(conf) + "/" + this.rootDir);
        if (!file.exists()) {
            LOG.error("{} is not existing.", file.getPath());
            throw new RuntimeException("Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
        }
        this.center = CgroupCenter.getInstance();
        if (this.center == null) {
            throw new RuntimeException("Cgroup error, please check /proc/cgroups");
        }
        this.prepareSubSystem(this.conf);
    }

    /**
     * initalize subsystems
     */
    private void prepareSubSystem(Map conf) throws IOException {
        List<SubSystemType> subSystemTypes = new LinkedList<>();
        for (String resource : Config.getCgroupStormResources(conf)) {
            subSystemTypes.add(SubSystemType.getSubSystem(resource));
        }

        this.hierarchy = center.getHierarchyWithSubSystems(subSystemTypes);

        if (this.hierarchy == null) {
            Set<SubSystemType> types = new HashSet<SubSystemType>();
            types.add(SubSystemType.cpu);
            this.hierarchy = new Hierarchy(Config.getCgroupStormHierarchyName(conf), types, Config.getCgroupStormHierarchyDir(conf));
        }
        this.rootCgroup = new CgroupCommon(this.rootDir, this.hierarchy, this.hierarchy.getRootCgroups());

        // set upper limit to how much cpu can be used by all workers running on supervisor node.
        // This is done so that some cpu cycles will remain free to run the daemons and other miscellaneous OS operations.
        CpuCore supervisorRootCPU = (CpuCore) this.rootCgroup.getCores().get(SubSystemType.cpu);
        setCpuUsageUpperLimit(supervisorRootCPU, ((Number) this.conf.get(Config.SUPERVISOR_CPU_CAPACITY)).intValue());
    }

    /**
     * Use cfs_period & cfs_quota to control the upper limit use of cpu core e.g.
     * If making a process to fully use two cpu cores, set cfs_period_us to
     * 100000 and set cfs_quota_us to 200000
     */
    private void setCpuUsageUpperLimit(CpuCore cpuCore, int cpuCoreUpperLimit) throws IOException {

        if (cpuCoreUpperLimit == -1) {
            // No control of cpu usage
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit);
        } else {
            cpuCore.setCpuCfsPeriodUs(100000);
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit * 1000);
        }
    }

    public void reserveResourcesForWorker(String workerId, Map resourcesMap) throws SecurityException {
        Number cpuNum = null;
        // The manually set STORM_WORKER_CGROUP_CPU_LIMIT config on supervisor will overwrite resources assigned by RAS (Resource Aware Scheduler)
        if (this.conf.get(Config.STORM_WORKER_CGROUP_CPU_LIMIT) != null) {
            cpuNum = (Number) this.conf.get(Config.STORM_WORKER_CGROUP_CPU_LIMIT);
        } else if(resourcesMap.get("cpu") != null) {
            cpuNum = (Number) resourcesMap.get("cpu");
        }

        Number totalMem = null;
        // The manually set STORM_WORKER_CGROUP_MEMORY_MB_LIMIT config on supervisor will overwrite resources assigned by RAS (Resource Aware Scheduler)
        if (this.conf.get(Config.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT) != null) {
            totalMem = (Number) this.conf.get(Config.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT);
        } else if (resourcesMap.get("memory") != null) {
            totalMem = (Number) resourcesMap.get("memory");
        }

        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);
        try {
            this.center.createCgroup(workerGroup);
        } catch (Exception e) {
            LOG.error("Error when creating Cgroup: {}", e);
        }

        if (cpuNum != null) {
            CpuCore cpuCore = (CpuCore) workerGroup.getCores().get(SubSystemType.cpu);
            try {
                cpuCore.setCpuShares(cpuNum.intValue());
            } catch (IOException e) {
                throw new RuntimeException("Cannot set cpu.shares! Exception: ", e);
            }
        }

        if (totalMem != null) {
            MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
            try {
                memCore.setPhysicalUsageLimit(Long.valueOf(totalMem.longValue() * 1024 * 1024));
            } catch (IOException e) {
                throw new RuntimeException("Cannot set memory.limit_in_bytes! Exception: ", e);
            }
        }
    }

    public void releaseResourcesForWorker(String workerId) {
        CgroupCommon workerGroup = new CgroupCommon(workerId, hierarchy, this.rootCgroup);
        try {
            Set<Integer> tasks = workerGroup.getTasks();
            if (!tasks.isEmpty()) {
                throw new Exception("Cannot correctly showdown worker CGroup " + workerId + "tasks " + tasks.toString() + " still running!");
            }
            this.center.deleteCgroup(workerGroup);
        } catch (Exception e) {
            LOG.error("Exception thrown when shutting worker {} Exception: {}", workerId, e);
        }
    }

    @Override
    public List<String> getLaunchCommand(String workerId, List<String> existingCommand) {
        List<String> newCommand = getLaunchCommandPrefix(workerId);
        newCommand.addAll(existingCommand);
        return newCommand;
    }

    @Override
    public List<String> getLaunchCommandPrefix(String workerId) {
        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);

        if (!this.rootCgroup.getChildren().contains(workerGroup)) {
            throw new RuntimeException("cgroup " + workerGroup + " doesn't exist! Need to reserve resources for worker first!");
        }

        StringBuilder sb = new StringBuilder();

        sb.append(this.conf.get(Config.STORM_CGROUP_CGEXEC_CMD)).append(" -g ");

        Iterator<SubSystemType> it = this.hierarchy.getSubSystems().iterator();
        while (it.hasNext()) {
            sb.append(it.next().toString());
            if (it.hasNext()) {
                sb.append(",");
            } else {
                sb.append(":");
            }
        }
        sb.append(workerGroup.getName());
        List<String> newCommand = new ArrayList<String>();
        newCommand.addAll(Arrays.asList(sb.toString().split(" ")));
        return newCommand;
    }

    public void close() throws IOException {
        this.center.deleteCgroup(this.rootCgroup);
    }
}
