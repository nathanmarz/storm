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

public enum SubSystemType {

    // net_cls,ns is not supposted in ubuntu
    blkio, cpu, cpuacct, cpuset, devices, freezer, memory, perf_event, net_cls, net_prio;

    public static SubSystemType getSubSystem(String str) {
        if (str.equals("blkio")) {
            return blkio;
        }
        else if (str.equals("cpu")) {
            return cpu;
        }
        else if (str.equals("cpuacct")) {
            return cpuacct;
        }
        else if (str.equals("cpuset")) {
            return cpuset;
        }
        else if (str.equals("devices")) {
            return devices;
        }
        else if (str.equals("freezer")) {
            return freezer;
        }
        else if (str.equals("memory")) {
            return memory;
        }
        else if (str.equals("perf_event")) {
            return perf_event;
        }
        else if (str.equals("net_cls")) {
            return net_cls;
        }
        else if (str.equals("net_prio")) {
            return net_prio;
        }
        return null;
    }
}
