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
package org.apache.storm.metrics.sigar;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.ProcCpu;

import org.apache.storm.metric.api.IMetric;

import java.util.HashMap;

/**
 * A metric using Sigar to get User and System CPU utilization for a worker.
 */
public class CPUMetric implements IMetric {
    private long _prevUser = 0;
    private long _prevSys = 0;
    private final Sigar _sigar;
    private final long _pid;

    public CPUMetric() {
        _sigar = new Sigar();
        _pid = _sigar.getPid();
    }

    @Override
    public Object getValueAndReset() {
        try {
          ProcCpu cpu = _sigar.getProcCpu(_pid);
          long userTotal = cpu.getUser();
          long sysTotal = cpu.getSys();
          long user = userTotal - _prevUser;
          long sys = sysTotal - _prevSys;
          _prevUser = userTotal;
          _prevSys = sysTotal;

          HashMap<String, Long> ret = new HashMap<String, Long>();
          ret.put("user-ms", user);
          ret.put("sys-ms", sys);
          return ret;
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
    }
}
