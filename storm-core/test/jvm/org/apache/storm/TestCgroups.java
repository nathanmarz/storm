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

package org.apache.storm;

import org.junit.Assert;
import org.junit.Assume;
import org.apache.storm.container.cgroup.CgroupManager;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Unit tests for CGroups
 */
public class TestCgroups {

    private static final Logger LOG = LoggerFactory.getLogger(TestCgroups.class);

    /**
     * Test whether cgroups are setup up correctly for use.  Also tests whether Cgroups produces the right command to
     * start a worker and cleans up correctly after the worker is shutdown
     */
    @Test
    public void testSetupAndTearDown() throws IOException {
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        //We don't want to run the test is CGroups are not setup
        Assume.assumeTrue("Check if CGroups are setup", ((boolean) config.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE)) == true);

        Assert.assertTrue("Check if STORM_CGROUP_HIERARCHY_DIR exists", stormCgroupHierarchyExists(config));
        Assert.assertTrue("Check if STORM_SUPERVISOR_CGROUP_ROOTDIR exists", stormCgroupSupervisorRootDirExists(config));

        CgroupManager manager = new CgroupManager();
        manager.prepare(config);

        Map<String, Object> resourcesMap = new HashMap<String, Object>();
        resourcesMap.put("cpu", 200);
        resourcesMap.put("memory", 1024);
        String workerId = UUID.randomUUID().toString();
        manager.reserveResourcesForWorker(workerId, resourcesMap);

        List<String> commandList = manager.getLaunchCommand(workerId, new ArrayList<String>());
        StringBuilder command = new StringBuilder();
        for (String entry : commandList) {
            command.append(entry).append(" ");
        }
        String correctCommand1 = config.get(Config.STORM_CGROUP_CGEXEC_CMD) + " -g memory,cpu:/"
                + config.get(Config.STORM_SUPERVISOR_CGROUP_ROOTDIR) + "/" + workerId + " ";
        String correctCommand2 = config.get(Config.STORM_CGROUP_CGEXEC_CMD) + " -g cpu,memory:/"
                + config.get(Config.STORM_SUPERVISOR_CGROUP_ROOTDIR) + "/" + workerId + " ";
        Assert.assertTrue("Check if cgroup launch command is correct", command.toString().equals(correctCommand1) || command.toString().equals(correctCommand2));

        String pathToWorkerCgroupDir = ((String) config.get(Config.STORM_CGROUP_HIERARCHY_DIR))
                + "/" + ((String) config.get(Config.STORM_SUPERVISOR_CGROUP_ROOTDIR)) + "/" + workerId;

        Assert.assertTrue("Check if cgroup directory exists for worker", dirExists(pathToWorkerCgroupDir));

        /* validate cpu settings */

        String pathToCpuShares = pathToWorkerCgroupDir + "/cpu.shares";
        Assert.assertTrue("Check if cpu.shares file exists", fileExists(pathToCpuShares));
        Assert.assertEquals("Check if the correct value is written into cpu.shares", "200", readFileAll(pathToCpuShares));

        /* validate memory settings */

        String pathTomemoryLimitInBytes = pathToWorkerCgroupDir + "/memory.limit_in_bytes";

        Assert.assertTrue("Check if memory.limit_in_bytes file exists", fileExists(pathTomemoryLimitInBytes));
        Assert.assertEquals("Check if the correct value is written into memory.limit_in_bytes", String.valueOf(1024 * 1024 * 1024), readFileAll(pathTomemoryLimitInBytes));

        manager.releaseResourcesForWorker(workerId);

        Assert.assertFalse("Make sure cgroup was removed properly", dirExists(pathToWorkerCgroupDir));
    }

    private boolean stormCgroupHierarchyExists(Map config) {
        String pathToStormCgroupHierarchy = (String) config.get(Config.STORM_CGROUP_HIERARCHY_DIR);
        return dirExists(pathToStormCgroupHierarchy);
    }

    private boolean stormCgroupSupervisorRootDirExists(Map config) {
        String pathTostormCgroupSupervisorRootDir = ((String) config.get(Config.STORM_CGROUP_HIERARCHY_DIR))
                + "/" + ((String) config.get(Config.STORM_SUPERVISOR_CGROUP_ROOTDIR));

        return dirExists(pathTostormCgroupSupervisorRootDir);
    }

    private boolean dirExists(String rawPath) {
        File path = new File(rawPath);
        return path.exists() && path.isDirectory();
    }

    private boolean fileExists(String rawPath) {
        File path = new File(rawPath);
        return path.exists() && !path.isDirectory();
    }

    private String readFileAll(String filePath) throws IOException {
        byte[] data = Files.readAllBytes(Paths.get(filePath));
        return new String(data).trim();
    }
}
