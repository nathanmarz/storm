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

import org.apache.storm.container.cgroup.core.CgroupCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CgroupCommon implements CgroupCommonOperation {

    public static final String TASKS = "/tasks";
    public static final String NOTIFY_ON_RELEASE = "/notify_on_release";
    public static final String RELEASE_AGENT = "/release_agent";
    public static final String CGROUP_CLONE_CHILDREN = "/cgroup.clone_children";
    public static final String CGROUP_EVENT_CONTROL = "/cgroup.event_control";
    public static final String CGROUP_PROCS = "/cgroup.procs";

    private final Hierarchy hierarchy;

    private final String name;

    private final String dir;

    private final CgroupCommon parent;

    private final boolean isRoot;

    private static final Logger LOG = LoggerFactory.getLogger(CgroupCommon.class);

    public CgroupCommon(String name, Hierarchy hierarchy, CgroupCommon parent) {
        this.name = parent.getName() + "/" + name;
        this.hierarchy = hierarchy;
        this.parent = parent;
        this.dir = parent.getDir() + "/" + name;
        this.isRoot = false;
    }

    /**
     * rootCgroup
     */
    public CgroupCommon(Hierarchy hierarchy, String dir) {
        this.name = "";
        this.hierarchy = hierarchy;
        this.parent = null;
        this.dir = dir;
        this.isRoot = true;
    }

    @Override
    public void addTask(int taskId) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, TASKS), String.valueOf(taskId));
    }

    @Override
    public Set<Integer> getTasks() throws IOException {
        List<String> stringTasks = CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, TASKS));
        Set<Integer> tasks = new HashSet<Integer>();
        for (String task : stringTasks) {
            tasks.add(Integer.valueOf(task));
        }
        return tasks;
    }

    @Override
    public void addProcs(int pid) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, CGROUP_PROCS), String.valueOf(pid));
    }

    @Override
    public Set<Integer> getPids() throws IOException {
        List<String> stringPids = CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CGROUP_PROCS));
        Set<Integer> pids = new HashSet<Integer>();
        for (String task : stringPids) {
            pids.add(Integer.valueOf(task));
        }
        return pids;
    }

    @Override
    public void setNotifyOnRelease(boolean flag) throws IOException {

        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, NOTIFY_ON_RELEASE), flag ? "1" : "0");
    }

    @Override
    public boolean getNotifyOnRelease() throws IOException {
        return CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, NOTIFY_ON_RELEASE)).get(0).equals("1") ? true : false;
    }

    @Override
    public void setReleaseAgent(String command) throws IOException {
        if (!this.isRoot) {
            LOG.warn("Cannot set {} in {} since its not the root group", RELEASE_AGENT, this.isRoot);
            return;
        }
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, RELEASE_AGENT), command);
    }

    @Override
    public String getReleaseAgent() throws IOException {
        if (!this.isRoot) {
            LOG.warn("Cannot get {} in {} since its not the root group", RELEASE_AGENT, this.isRoot);
            return null;
        }
        return CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, RELEASE_AGENT)).get(0);
    }

    @Override
    public void setCgroupCloneChildren(boolean flag) throws IOException {
        if (!getCores().keySet().contains(SubSystemType.cpuset)) {
            return;
        }
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, CGROUP_CLONE_CHILDREN), flag ? "1" : "0");
    }

    @Override
    public boolean getCgroupCloneChildren() throws IOException {
        return CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CGROUP_CLONE_CHILDREN)).get(0).equals("1") ? true : false;
    }

    @Override
    public void setEventControl(String eventFd, String controlFd, String... args) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(eventFd);
        sb.append(' ');
        sb.append(controlFd);
        for (String arg : args) {
            sb.append(' ');
            sb.append(arg);
        }
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, CGROUP_EVENT_CONTROL), sb.toString());
    }

    public Hierarchy getHierarchy() {
        return hierarchy;
    }

    public String getName() {
        return name;
    }

    public String getDir() {
        return dir;
    }

    public CgroupCommon getParent() {
        return parent;
    }

    public Set<CgroupCommon> getChildren() {

        File file = new File(this.dir);
        File[] files = file.listFiles();
        if (files == null) {
            LOG.info("{} is not a directory", this.dir);
            return null;
        }
        Set<CgroupCommon> children = new HashSet<CgroupCommon>();
        for (File child : files) {
            if (child.isDirectory()) {
                children.add(new CgroupCommon(child.getName(), this.hierarchy, this));
            }
        }
        return children;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public Map<SubSystemType, CgroupCore> getCores() {
        return CgroupCoreFactory.getInstance(this.hierarchy.getSubSystems(), this.dir);
    }

    public void delete() throws IOException {
        this.free();
        if (!this.isRoot) {
            this.parent.getChildren().remove(this);
        }
    }

    private void free() throws IOException {
        for (CgroupCommon child : getChildren()) {
            child.free();
        }
        if (this.isRoot) {
            return;
        }
        Set<Integer> tasks = this.getTasks();
        if (tasks != null) {
            for (Integer task : tasks) {
                this.parent.addTask(task);
            }
        }
        CgroupUtils.deleteDir(this.dir);
    }

    @Override
    public boolean equals(Object o) {
        boolean ret = false;
        if (o != null && (o instanceof CgroupCommon)) {

            boolean hierarchyFlag =false;
            if (((CgroupCommon)o).hierarchy != null && this.hierarchy != null) {
                hierarchyFlag = ((CgroupCommon)o).hierarchy.equals(this.hierarchy);
            } else if (((CgroupCommon)o).hierarchy == null && this.hierarchy == null) {
                hierarchyFlag = true;
            } else {
                hierarchyFlag = false;
            }

            boolean nameFlag = false;
            if (((CgroupCommon)o).name != null && this.name != null) {
                nameFlag = ((CgroupCommon)o).name.equals(this.name);
            } else if (((CgroupCommon)o).name == null && this.name == null) {
                nameFlag = true;
            } else {
                nameFlag = false;
            }

            boolean dirFlag = false;
            if (((CgroupCommon)o).dir != null && this.dir != null) {
                dirFlag = ((CgroupCommon)o).dir.equals(this.dir);
            } else if (((CgroupCommon)o).dir == null && this.dir == null) {
                dirFlag = true;
            } else {
                dirFlag = false;
            }
            ret = hierarchyFlag && nameFlag && dirFlag;
        }
        return ret;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.name != null ? this.name.hashCode() : 0);
        result = prime * result + (this.hierarchy != null ? this.hierarchy.hashCode() : 0);
        result = prime * result + (this.dir != null ? this.dir.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
