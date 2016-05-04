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
package org.apache.storm.daemon.metrics;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.metric.api.StateMetric;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.task.TopologyContext;

import java.util.HashMap;
import java.util.Map;

public class BuiltinMetricsUtil {
    public static BuiltinMetrics mkData(String type, CommonStats stats) {
        if (StatsUtil.SPOUT.equals(type)) {
            return new BuiltinSpoutMetrics((SpoutExecutorStats) stats);
        } else if (StatsUtil.BOLT.equals(type)) {
            return new BuiltinBoltMetrics((BoltExecutorStats) stats);
        }
        throw new RuntimeException("Invalid component type!");
    }

    public static void registerIconnectionServerMetric(Object server, Map stormConf, TopologyContext context) {
        if (server instanceof IStatefulObject) {
            registerMetric("__recv-iconnection", new StateMetric((IStatefulObject) server), stormConf, context);
        }
    }

    public static void registerIconnectionClientMetrics(final Map nodePortToSocket, Map stormConf, TopologyContext context) {
        IMetric metric = new IMetric() {
            @Override
            public Object getValueAndReset() {
                Map<Object, Object> ret = new HashMap<>();
                for (Object o : nodePortToSocket.entrySet()) {
                    Map.Entry entry = (Map.Entry) o;
                    Object nodePort = entry.getKey();
                    Object connection = entry.getValue();
                    if (connection instanceof IStatefulObject) {
                        ret.put(nodePort, ((IStatefulObject) connection).getState());
                    }
                }
                return ret;
            }
        };
        registerMetric("__send-iconnection", metric, stormConf, context);
    }

    public static void registerQueueMetrics(Map queues, Map stormConf, TopologyContext context) {
        for (Object o : queues.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String name = "__" + entry.getKey();
            IMetric metric = new StateMetric((IStatefulObject) entry.getValue());
            registerMetric(name, metric, stormConf, context);
        }
    }

    public static void registerMetric(String name, IMetric metric, Map stormConf, TopologyContext context) {
        int bucketSize = ((Number) stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)).intValue();
        context.registerMetric(name, metric, bucketSize);
    }
}
