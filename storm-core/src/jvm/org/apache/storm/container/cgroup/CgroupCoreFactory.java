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

import org.apache.storm.container.cgroup.core.BlkioCore;
import org.apache.storm.container.cgroup.core.CgroupCore;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.CpuacctCore;
import org.apache.storm.container.cgroup.core.CpusetCore;
import org.apache.storm.container.cgroup.core.DevicesCore;
import org.apache.storm.container.cgroup.core.FreezerCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.container.cgroup.core.NetClsCore;
import org.apache.storm.container.cgroup.core.NetPrioCore;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CgroupCoreFactory {

    public static Map<SubSystemType, CgroupCore> getInstance(Set<SubSystemType> types, String dir) {
        Map<SubSystemType, CgroupCore> result = new HashMap<SubSystemType, CgroupCore>();
        for (SubSystemType type : types) {
            switch (type) {
            case blkio:
                result.put(SubSystemType.blkio, new BlkioCore(dir));
                break;
            case cpuacct:
                result.put(SubSystemType.cpuacct, new CpuacctCore(dir));
                break;
            case cpuset:
                result.put(SubSystemType.cpuset, new CpusetCore(dir));
                break;
            case cpu:
                result.put(SubSystemType.cpu, new CpuCore(dir));
                break;
            case devices:
                result.put(SubSystemType.devices, new DevicesCore(dir));
                break;
            case freezer:
                result.put(SubSystemType.freezer, new FreezerCore(dir));
                break;
            case memory:
                result.put(SubSystemType.memory, new MemoryCore(dir));
                break;
            case net_cls:
                result.put(SubSystemType.net_cls, new NetClsCore(dir));
                break;
            case net_prio:
                result.put(SubSystemType.net_prio, new NetPrioCore(dir));
                break;
            default:
                break;
            }
        }
        return result;
    }
}
