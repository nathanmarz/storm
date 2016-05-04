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

public class MemoryCore implements CgroupCore {

    public static final String MEMORY_STAT = "/memory.stat";
    public static final String MEMORY_USAGE_IN_BYTES = "/memory.usage_in_bytes";
    public static final String MEMORY_MEMSW_USAGE_IN_BYTES = "/memory.memsw.usage_in_bytes";
    public static final String MEMORY_MAX_USAGE_IN_BYTES = "/memory.max_usage_in_bytes";
    public static final String MEMORY_MEMSW_MAX_USAGE_IN_BYTES = "/memory.memsw.max_usage_in_bytes";
    public static final String MEMORY_LIMIT_IN_BYTES = "/memory.limit_in_bytes";
    public static final String MEMORY_MEMSW_LIMIT_IN_BYTES = "/memory.memsw.limit_in_bytes";
    public static final String MEMORY_FAILCNT = "/memory.failcnt";
    public static final String MEMORY_MEMSW_FAILCNT = "/memory.memsw.failcnt";
    public static final String MEMORY_FORCE_EMPTY = "/memory.force_empty";
    public static final String MEMORY_SWAPPINESS = "/memory.swappiness";
    public static final String MEMORY_USE_HIERARCHY = "/memory.use_hierarchy";
    public static final String MEMORY_OOM_CONTROL = "/memory.oom_control";

    private final String dir;

    public MemoryCore(String dir) {
        this.dir = dir;
    }

    @Override
    public SubSystemType getType() {
        return SubSystemType.memory;
    }

    public static class Stat {
        public final long cacheSize;
        public final long rssSize;
        public final long mappedFileSize;
        public final long pgpginNum;
        public final long pgpgoutNum;
        public final long swapSize;
        public final long activeAnonSize;
        public final long inactiveAnonSize;
        public final long activeFileSize;
        public final long inactiveFileSize;
        public final long unevictableSize;
        public final long hierarchicalMemoryLimitSize;
        public final long hierarchicalMemSwapLimitSize;
        public final long totalCacheSize;
        public final long totalRssSize;
        public final long totalMappedFileSize;
        public final long totalPgpginNum;
        public final long totalPgpgoutNum;
        public final long totalSwapSize;
        public final long totalActiveAnonSize;
        public final long totalInactiveAnonSize;
        public final long totalActiveFileSize;
        public final long totalInactiveFileSize;
        public final long totalUnevictableSize;
        public final long totalHierarchicalMemoryLimitSize;
        public final long totalHierarchicalMemSwapLimitSize;

        public Stat(String output) {
            String[] splits = output.split("\n");
            this.cacheSize = Long.parseLong(splits[0]);
            this.rssSize = Long.parseLong(splits[1]);
            this.mappedFileSize = Long.parseLong(splits[2]);
            this.pgpginNum = Long.parseLong(splits[3]);
            this.pgpgoutNum = Long.parseLong(splits[4]);
            this.swapSize = Long.parseLong(splits[5]);
            this.inactiveAnonSize = Long.parseLong(splits[6]);
            this.activeAnonSize = Long.parseLong(splits[7]);
            this.inactiveFileSize = Long.parseLong(splits[8]);
            this.activeFileSize = Long.parseLong(splits[9]);
            this.unevictableSize = Long.parseLong(splits[10]);
            this.hierarchicalMemoryLimitSize = Long.parseLong(splits[11]);
            this.hierarchicalMemSwapLimitSize = Long.parseLong(splits[12]);
            this.totalCacheSize = Long.parseLong(splits[13]);
            this.totalRssSize = Long.parseLong(splits[14]);
            this.totalMappedFileSize = Long.parseLong(splits[15]);
            this.totalPgpginNum = Long.parseLong(splits[16]);
            this.totalPgpgoutNum = Long.parseLong(splits[17]);
            this.totalSwapSize = Long.parseLong(splits[18]);
            this.totalInactiveAnonSize = Long.parseLong(splits[19]);
            this.totalActiveAnonSize = Long.parseLong(splits[20]);
            this.totalInactiveFileSize = Long.parseLong(splits[21]);
            this.totalActiveFileSize = Long.parseLong(splits[22]);
            this.totalUnevictableSize = Long.parseLong(splits[23]);
            this.totalHierarchicalMemoryLimitSize = Long.parseLong(splits[24]);
            this.totalHierarchicalMemSwapLimitSize = Long.parseLong(splits[25]);
        }
    }

    public Stat getStat() throws IOException {
        String output = CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_STAT)).get(0);
        Stat stat = new Stat(output);
        return stat;
    }

    public long getPhysicalUsage() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_USAGE_IN_BYTES)).get(0));
    }

    public long getWithSwapUsage() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_MEMSW_USAGE_IN_BYTES)).get(0));
    }

    public long getMaxPhysicalUsage() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_MAX_USAGE_IN_BYTES)).get(0));
    }

    public long getMaxWithSwapUsage() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_MEMSW_MAX_USAGE_IN_BYTES)).get(0));
    }

    public void setPhysicalUsageLimit(long value) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, MEMORY_LIMIT_IN_BYTES), String.valueOf(value));
    }

    public long getPhysicalUsageLimit() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_LIMIT_IN_BYTES)).get(0));
    }

    public void setWithSwapUsageLimit(long value) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, MEMORY_MEMSW_LIMIT_IN_BYTES), String.valueOf(value));
    }

    public long getWithSwapUsageLimit() throws IOException {
        return Long.parseLong(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_MEMSW_LIMIT_IN_BYTES)).get(0));
    }

    public int getPhysicalFailCount() throws IOException {
        return Integer.parseInt(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_FAILCNT)).get(0));
    }

    public int getWithSwapFailCount() throws IOException {
        return Integer.parseInt(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_MEMSW_FAILCNT)).get(0));
    }

    public void clearForceEmpty() throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, MEMORY_FORCE_EMPTY), String.valueOf(0));
    }

    public void setSwappiness(int value) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, MEMORY_SWAPPINESS), String.valueOf(value));
    }

    public int getSwappiness() throws IOException {
        return Integer.parseInt(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_SWAPPINESS)).get(0));
    }

    public void setUseHierarchy(boolean flag) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, MEMORY_USE_HIERARCHY), String.valueOf(flag ? 1 : 0));
    }

    public boolean isUseHierarchy() throws IOException {
        int output = Integer.parseInt(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_USE_HIERARCHY)).get(0));
        return output > 0;
    }

    public void setOomControl(boolean flag) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, MEMORY_OOM_CONTROL), String.valueOf(flag ? 1 : 0));
    }

    public boolean isOomControl() throws IOException {
        String output = CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, MEMORY_OOM_CONTROL)).get(0);
        output = output.split("\n")[0].split("[\\s]")[1];
        int value = Integer.parseInt(output);
        return value > 0;
    }
}
