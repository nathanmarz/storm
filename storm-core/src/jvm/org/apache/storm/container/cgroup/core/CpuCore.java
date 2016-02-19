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
import java.util.List;

public class CpuCore implements CgroupCore {

    public static final String CPU_SHARES = "/cpu.shares";
    public static final String CPU_RT_RUNTIME_US = "/cpu.rt_runtime_us";
    public static final String CPU_RT_PERIOD_US = "/cpu.rt_period_us";
    public static final String CPU_CFS_PERIOD_US = "/cpu.cfs_period_us";
    public static final String CPU_CFS_QUOTA_US = "/cpu.cfs_quota_us";
    public static final String CPU_STAT = "/cpu.stat";

    private final String dir;

    public CpuCore(String dir) {
        this.dir = dir;
    }

    @Override
    public SubSystemType getType() {
        return SubSystemType.cpu;
    }

    public void setCpuShares(int weight) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, CPU_SHARES), String.valueOf(weight));
    }

    public int getCpuShares() throws IOException {
        return Integer.parseInt(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPU_SHARES)).get(0));
    }

    public void setCpuRtRuntimeUs(long us) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, CPU_RT_RUNTIME_US), String.valueOf(us));
    }

    public long getCpuRtRuntimeUs() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPU_RT_RUNTIME_US)).get(0));
    }

    public void setCpuRtPeriodUs(long us) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, CPU_RT_PERIOD_US), String.valueOf(us));
    }

    public Long getCpuRtPeriodUs() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPU_RT_PERIOD_US)).get(0));
    }

    public void setCpuCfsPeriodUs(long us) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, CPU_CFS_PERIOD_US), String.valueOf(us));
    }

    public Long getCpuCfsPeriodUs() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPU_CFS_PERIOD_US)).get(0));
    }

    public void setCpuCfsQuotaUs(long us) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, CPU_CFS_QUOTA_US), String.valueOf(us));
    }

    public Long getCpuCfsQuotaUs() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPU_CFS_QUOTA_US)).get(0));
    }

    public Stat getCpuStat() throws IOException {
        return new Stat(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, CPU_STAT)));
    }

    public static class Stat {
        public final int nrPeriods;
        public final int nrThrottled;
        public final int throttledTime;

        public Stat(List<String> statStr) {
            this.nrPeriods = Integer.parseInt(statStr.get(0).split(" ")[1]);
            this.nrThrottled = Integer.parseInt(statStr.get(1).split(" ")[1]);
            this.throttledTime = Integer.parseInt(statStr.get(2).split(" ")[1]);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + nrPeriods;
            result = prime * result + nrThrottled;
            result = prime * result + throttledTime;
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
            Stat other = (Stat) obj;
            if (nrPeriods != other.nrPeriods) {
                return false;
            }
            if (nrThrottled != other.nrThrottled) {
                return false;
            }
            if (throttledTime != other.throttledTime) {
                return false;
            }
            return true;
        }
    }
}
