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

public class NetPrioCore implements CgroupCore {

    public static final String NET_PRIO_PRIOIDX = "/net_prio.prioidx";
    public static final String NET_PRIO_IFPRIOMAP = "/net_prio.ifpriomap";

    private final String dir;

    public NetPrioCore(String dir) {
        this.dir = dir;
    }

    @Override
    public SubSystemType getType() {
        return SubSystemType.net_prio;
    }

    public int getPrioId() throws IOException {
        return Integer.parseInt(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, NET_PRIO_PRIOIDX)).get(0));
    }

    public void setIfPrioMap(String iface, int priority) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(iface);
        sb.append(' ');
        sb.append(priority);
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, NET_PRIO_IFPRIOMAP), sb.toString());
    }

    public Map<String, Integer> getIfPrioMap() throws IOException {
        Map<String, Integer> result = new HashMap<String, Integer>();
        List<String> strs = CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, NET_PRIO_IFPRIOMAP));
        for (String str : strs) {
            String[] strArgs = str.split(" ");
            result.put(strArgs[0], Integer.valueOf(strArgs[1]));
        }
        return result;
    }
}
