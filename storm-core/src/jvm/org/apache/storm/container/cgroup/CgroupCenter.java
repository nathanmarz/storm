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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * Thread unsafe
     * 
     * @return
     */
    public synchronized static CgroupCenter getInstance() {
        if (instance == null) {
            instance = new CgroupCenter();
        }
        return CgroupUtils.enabled() ? instance : null;
    }

    @Override
    public List<Hierarchy> getHierarchies() {

        Map<String, Hierarchy> hierarchies = new HashMap<String, Hierarchy>();

        try (FileReader reader = new FileReader(Constants.MOUNT_STATUS_FILE);
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
                Hierarchy h = hierarchies.get(type);
                h = new Hierarchy(name, CgroupUtils.analyse(type), dir);
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

        try (FileReader reader = new FileReader(Constants.CGROUP_STATUS_FILE);
             BufferedReader br = new BufferedReader(reader)){
            String str = null;
            while ((str = br.readLine()) != null) {
                String[] split = str.split("\t");
                SubSystemType type = SubSystemType.getSubSystem(split[0]);
                if (type == null) {
                    continue;
                }
                subSystems.add(new SubSystem(type, Integer.valueOf(split[1]), Integer.valueOf(split[2])
                        , Integer.valueOf(split[3]).intValue() == 1 ? true : false));
            }
            return subSystems;
        } catch (Exception e) {
            LOG.error("Get subSystems error {}", e);
        }
        return null;
    }

    @Override
    public boolean enabled(SubSystemType subsystem) {

        Set<SubSystem> subSystems = this.getSubSystems();
        for (SubSystem subSystem : subSystems) {
            if (subSystem.getType() == subsystem) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Hierarchy busy(SubSystemType subsystem) {
        List<Hierarchy> hierarchies = this.getHierarchies();
        for (Hierarchy hierarchy : hierarchies) {
            for (SubSystemType type : hierarchy.getSubSystems()) {
                if (type == subsystem) {
                    return hierarchy;
                }
            }
        }
        return null;
    }

    @Override
    public Hierarchy busy(List<SubSystemType> subSystems) {
        List<Hierarchy> hierarchies = this.getHierarchies();
        for (Hierarchy hierarchy : hierarchies) {
            Hierarchy ret = hierarchy;
            for (SubSystemType subsystem : subSystems) {
                if (!hierarchy.getSubSystems().contains(subsystem)) {
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
    public Hierarchy mounted(Hierarchy hierarchy) {

        List<Hierarchy> hierarchies = this.getHierarchies();
        if (CgroupUtils.dirExists(hierarchy.getDir())) {
            for (Hierarchy h : hierarchies) {
                if (h.equals(hierarchy)) {
                    return h;
                }
            }
        }
        return null;
    }

    @Override
    public void mount(Hierarchy hierarchy) throws IOException {

        if (this.mounted(hierarchy) != null) {
            LOG.error("{} is mounted", hierarchy.getDir());
            return;
        }
        Set<SubSystemType> subsystems = hierarchy.getSubSystems();
        for (SubSystemType type : subsystems) {
            if (this.busy(type) != null) {
                LOG.error("subsystem: {} is busy", type.name());
                subsystems.remove(type);
            }
        }
        if (subsystems.size() == 0) {
            return;
        }
        if (!CgroupUtils.dirExists(hierarchy.getDir())) {
            new File(hierarchy.getDir()).mkdirs();
        }
        String subSystems = CgroupUtils.reAnalyse(subsystems);
        SystemOperation.mount(subSystems, hierarchy.getDir(), "cgroup", subSystems);

    }

    @Override
    public void umount(Hierarchy hierarchy) throws IOException {
        if (this.mounted(hierarchy) != null) {
            hierarchy.getRootCgroups().delete();
            SystemOperation.umount(hierarchy.getDir());
            CgroupUtils.deleteDir(hierarchy.getDir());
        }
    }

    @Override
    public void create(CgroupCommon cgroup) throws SecurityException {
        if (cgroup.isRoot()) {
            LOG.error("You can't create rootCgroup in this function");
            return;
        }
        CgroupCommon parent = cgroup.getParent();
        while (parent != null) {
            if (!CgroupUtils.dirExists(parent.getDir())) {
                LOG.error(" {} is not existed", parent.getDir());
                return;
            }
            parent = parent.getParent();
        }
        Hierarchy h = cgroup.getHierarchy();
        if (mounted(h) == null) {
            LOG.error("{} is not mounted", h.getDir());
            return;
        }
        if (CgroupUtils.dirExists(cgroup.getDir())) {
            LOG.error("{} is existed", cgroup.getDir());
            return;
        }

        //Todo perhaps thrown exception or print out error message is dir is not created successfully
        if (!(new File(cgroup.getDir())).mkdir()) {
            LOG.error("Could not create cgroup dir at {}", cgroup.getDir());
        }
    }

    @Override
    public void delete(CgroupCommon cgroup) throws IOException {
        cgroup.delete();
    }
}
