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
package backtype.storm.metric;

import backtype.storm.Config;
import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.metric.api.IMetric;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


// There is one task inside one executor for each worker of the topology.
// TaskID is always -1, therefore you can only send-unanchored tuples to co-located SystemBolt.
// This bolt was conceived to export worker stats via metrics api.
public class SystemBolt implements IBolt {
    private static Logger LOG = LoggerFactory.getLogger(SystemBolt.class);
    private static boolean _prepareWasCalled = false;

    private static class MemoryUsageMetric implements IMetric {
        IFn _getUsage;
        public MemoryUsageMetric(IFn getUsage) {
            _getUsage = getUsage;
        }
        @Override
        public Object getValueAndReset() {
            MemoryUsage memUsage = (MemoryUsage)_getUsage.invoke();
            HashMap m = new HashMap();
            m.put("maxBytes", memUsage.getMax());
            m.put("committedBytes", memUsage.getCommitted());
            m.put("initBytes", memUsage.getInit());
            m.put("usedBytes", memUsage.getUsed());
            m.put("virtualFreeBytes", memUsage.getMax() - memUsage.getUsed());
            m.put("unusedBytes", memUsage.getCommitted() - memUsage.getUsed());
            return m;
        }
    }

    // canonically the metrics data exported is time bucketed when doing counts.
    // convert the absolute values here into time buckets.
    private static class GarbageCollectorMetric implements IMetric {
        GarbageCollectorMXBean _gcBean;
        Long _collectionCount;
        Long _collectionTime;
        public GarbageCollectorMetric(GarbageCollectorMXBean gcBean) {
            _gcBean = gcBean;
        }
        @Override
        public Object getValueAndReset() {
            Long collectionCountP = _gcBean.getCollectionCount();
            Long collectionTimeP = _gcBean.getCollectionTime();

            Map ret = null;
            if(_collectionCount!=null && _collectionTime!=null) {
                ret = new HashMap();
                ret.put("count", collectionCountP - _collectionCount);
                ret.put("timeMs", collectionTimeP - _collectionTime);
            }

            _collectionCount = collectionCountP;
            _collectionTime = collectionTimeP;
            return ret;
        }
    }

    @Override
    public void prepare(final Map stormConf, TopologyContext context, OutputCollector collector) {
        if(_prepareWasCalled && !"local".equals(stormConf.get(Config.STORM_CLUSTER_MODE))) {
            throw new RuntimeException("A single worker should have 1 SystemBolt instance.");
        }
        _prepareWasCalled = true;

        int bucketSize = RT.intCast(stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));

        final RuntimeMXBean jvmRT = ManagementFactory.getRuntimeMXBean();

        context.registerMetric("uptimeSecs", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return jvmRT.getUptime()/1000.0;
            }
        }, bucketSize);

        context.registerMetric("startTimeSecs", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return jvmRT.getStartTime()/1000.0;
            }
        }, bucketSize);

        context.registerMetric("newWorkerEvent", new IMetric() {
            boolean doEvent = true;

            @Override
            public Object getValueAndReset() {
                if (doEvent) {
                    doEvent = false;
                    return 1;
                } else return 0;
            }
        }, bucketSize);

        final MemoryMXBean jvmMemRT = ManagementFactory.getMemoryMXBean();

        context.registerMetric("memory/heap", new MemoryUsageMetric(new AFn() {
            public Object invoke() {
                return jvmMemRT.getHeapMemoryUsage();
            }
        }), bucketSize);
        context.registerMetric("memory/nonHeap", new MemoryUsageMetric(new AFn() {
            public Object invoke() {
                return jvmMemRT.getNonHeapMemoryUsage();
            }
        }), bucketSize);

        for(GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            context.registerMetric("GC/" + b.getName().replaceAll("\\W", ""), new GarbageCollectorMetric(b), bucketSize);
        }
    }

    @Override
    public void execute(Tuple input) {
        throw new RuntimeException("Non-system tuples should never be sent to __system bolt.");
    }

    @Override
    public void cleanup() {
    }
}
