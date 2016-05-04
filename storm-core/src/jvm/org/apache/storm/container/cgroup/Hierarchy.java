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

import java.util.Set;

/**
 * A class that describes a cgroup hiearchy
 */
public class Hierarchy {

    private final String name;

    private final Set<SubSystemType> subSystems;

    private final String type;

    private final String dir;

    private final CgroupCommon rootCgroups;

    public Hierarchy(String name, Set<SubSystemType> subSystems, String dir) {
        this.name = name;
        this.subSystems = subSystems;
        this.dir = dir;
        this.rootCgroups = new CgroupCommon(this, dir);
        this.type = CgroupUtils.subSystemsToString(subSystems);
    }

    /**
     * get subsystems
     */
    public Set<SubSystemType> getSubSystems() {
        return subSystems;
    }

    /**
     * get all subsystems in hierarchy as a comma delimited list
     */
    public String getType() {
        return type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dir == null) ? 0 : dir.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Hierarchy other = (Hierarchy) obj;
        if (dir == null) {
            if (other.dir != null) {
                return false;
            }
        } else if (!dir.equals(other.dir)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!type.equals(other.type)) {
            return false;
        }
        return true;
    }

    public String getDir() {
        return dir;
    }

    public CgroupCommon getRootCgroups() {
        return rootCgroups;
    }

    public String getName() {
        return name;
    }

    public boolean isSubSystemMounted(SubSystemType subsystem) {
        for (SubSystemType type : this.subSystems) {
            if (type == subsystem) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return this.dir;
    }
}
