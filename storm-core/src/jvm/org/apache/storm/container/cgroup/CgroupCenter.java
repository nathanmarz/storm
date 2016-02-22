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

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CgroupCenter implements CgroupOperation {

    private static Logger LOG = LoggerFactory.getLogger(CgroupCenter.class);

    private static CgroupCenter instance;

    private CgroupCenter() {

    }

    public synchronized static CgroupCenter getInstance() {
        if (CgroupUtils.enabled()) {
            instance = new CgroupCenter();
            return instance;
        }
        return null;
    }

    @Override
    public List<Hierarchy> getHierarchies() {
        Map<String, Hierarchy> hierarchies = new HashMap<String, Hierarchy>();
        try (FileReader reader = new FileReader(CgroupUtils.MOUNT_STATUS_FILE);
             BufferedReader br = new BufferedReader(reader)) {
            String str = null;
            while ((str = br.readLine()) != null) {
                String[] strSplit = str.split(" ");
                if (!strSplit[2].equals("cgroup")) {
                    continue;
                }
                String name = strSplit[0];
                String type = strSplit[3];
                String dir = strSplit[1];
                //Some mount options (i.e. rw and relatime) in type are not cgroups related
                Hierarchy h = new Hierarchy(name, CgroupUtils.getSubSystemsFromString(type), dir);
                hierarchies.put(type, h);
            }
            return new ArrayList<Hierarchy>(hierarchies.values());
        } catch (Exception e) {
            LOG.error("Get hierarchies error {}", e);
        }
        return null;
    }

    @Override
    public Set<SubSystem> getSubSystems() {
        Set<SubSystem> subSystems = new HashSet<SubSystem>();
        try (FileReader reader = new FileReader(CgroupUtils.CGROUP_STATUS_FILE);
             BufferedReader br = new BufferedReader(reader)){
            String str = null;
            while ((str = br.readLine()) != null) {
                String[] split = str.split("\t");
                SubSystemType type = SubSystemType.getSubSystem(split[0]);
                if (type == null) {
                    continue;
                }
                int hierarchyID = Integer.valueOf(split[1]);
                int cgroupNum = Integer.valueOf(split[2]);
                boolean enable =  Integer.valueOf(split[3]).intValue() == 1 ? true : false;
                subSystems.add(new SubSystem(type, hierarchyID, cgroupNum, enable));
            }
            return subSystems;
        } catch (Exception e) {
            LOG.error("Get subSystems error {}", e);
        }
        return null;
    }

    @Override
    public boolean isSubSystemEnabled(SubSystemType subSystemType) {
        Set<SubSystem> subSystems = this.getSubSystems();
        for (SubSystem subSystem : subSystems) {
            if (subSystem.getType() == subSystemType) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Hierarchy getHierarchyWithSubSystem(SubSystemType subSystem) {
        return getHierarchyWithSubSystems(Arrays.asList(subSystem));
    }

    @Override
    public Hierarchy getHierarchyWithSubSystems(List<SubSystemType> subSystems) {
        List<Hierarchy> hierarchies = this.getHierarchies();
        for (Hierarchy hierarchy : hierarchies) {
            Hierarchy ret = hierarchy;
            for (SubSystemType subSystem : subSystems) {
                if (!hierarchy.getSubSystems().contains(subSystem)) {
                    ret = null;
                    break;
                }
            }
            if (ret != null) {
                return ret;
            }
        }
        return null;
    }

    @Override
    public boolean isMounted(Hierarchy hierarchy) {
        if (Utils.CheckDirExists(hierarchy.getDir())) {
            List<Hierarchy> hierarchies = this.getHierarchies();
            for (Hierarchy h : hierarchies) {
                if (h.equals(hierarchy)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void mount(Hierarchy hierarchy) throws IOException {
        if (this.isMounted(hierarchy)) {
            LOG.error("{} is already mounted", hierarchy.getDir());
            return;
        }
        Set<SubSystemType> subSystems = hierarchy.getSubSystems();
        for (SubSystemType type : subSystems) {
            Hierarchy hierarchyWithSubSystem = this.getHierarchyWithSubSystem(type);
            if (hierarchyWithSubSystem != null) {
                LOG.error("subSystem: {} is already mounted on hierarchy: {}", type.name(), hierarchyWithSubSystem);
                subSystems.remove(type);
            }
        }
        if (subSystems.size() == 0) {
            return;
        }
        if (!Utils.CheckDirExists(hierarchy.getDir())) {
            new File(hierarchy.getDir()).mkdirs();
        }
        String subSystemsName = CgroupUtils.subSystemsToString(subSystems);
        SystemOperation.mount(subSystemsName, hierarchy.getDir(), "cgroup", subSystemsName);

    }

    @Override
    public void umount(Hierarchy hierarchy) throws IOException {
        if (this.isMounted(hierarchy)) {
            hierarchy.getRootCgroups().delete();
            SystemOperation.umount(hierarchy.getDir());
            CgroupUtils.deleteDir(hierarchy.getDir());
        } else {
            LOG.error("{} is not mounted", hierarchy.getDir());
        }
    }

    @Override
    public void createCgroup(CgroupCommon cgroup) throws SecurityException {
        if (cgroup.isRoot()) {
            LOG.error("You can't create rootCgroup in this function");
            throw new RuntimeException("You can't create rootCgroup in this function");
        }
        CgroupCommon parent = cgroup.getParent();
        while (parent != null) {
            if (!Utils.CheckDirExists(parent.getDir())) {
                throw new RuntimeException("Parent " + parent.getDir() + "does not exist");
            }
            parent = parent.getParent();
        }
        Hierarchy h = cgroup.getHierarchy();
        if (!isMounted(h)) {
            throw new RuntimeException("hierarchy " + h.getDir() + " is not mounted");
        }
        if (Utils.CheckDirExists(cgroup.getDir())) {
            throw new RuntimeException("cgroup {} already exists " + cgroup.getDir());
        }

        if (!(new File(cgroup.getDir())).mkdir()) {
            throw new RuntimeException("Could not create cgroup dir at " + cgroup.getDir());
        }
    }

    @Override
    public void deleteCgroup(CgroupCommon cgroup) throws IOException {
        cgroup.delete();
    }
}
