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
package org.apache.storm.container.cgroup.core;

import org.apache.storm.container.cgroup.CgroupUtils;
import org.apache.storm.container.cgroup.SubSystemType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CpuacctCore implements CgroupCore {

    public static final String CPUACCT_USAGE = "/cpuacct.usage";
    public static final String CPUACCT_STAT = "/cpuacct.stat";
    public static final String CPUACCT_USAGE_PERCPU = "/cpuacct.usage_percpu";

    private final String dir;

    public CpuacctCore(String dir) {
        this.dir = dir;
    }

    @Override
    public SubSystemType getType() {
        return SubSystemType.cpuacct;
    }

    public Long getCpuUsage() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPUACCT_USAGE)).get(0));
    }

    public Map<StatType, Long> getCpuStat() throws IOException {
        List<String> strs = CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPUACCT_STAT));
        Map<StatType, Long> result = new HashMap<StatType, Long>();
        result.put(StatType.user, Long.parseLong(strs.get(0).split(" ")[1]));
        result.put(StatType.system, Long.parseLong(strs.get(1).split(" ")[1]));
        return result;
    }

    public Long[] getPerCpuUsage() throws IOException {
        String str = CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPUACCT_USAGE_PERCPU)).get(0);
        String[] strArgs = str.split(" ");
        Long[] result = new Long[strArgs.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = Long.parseLong(strArgs[i]);
        }
        return result;
    }

    public static enum StatType {
        user, system;
    }

}
