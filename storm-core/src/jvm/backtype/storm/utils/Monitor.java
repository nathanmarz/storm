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
package backtype.storm.utils;

import backtype.storm.generated.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class Monitor {
    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";

    private int _interval = 4;
    private String _topology;
    private String _component;
    private String _stream;
    private String _watch;

    private static class MetricsState {
        private long lastTime = 0;
        private long lastStatted = 0;

        private MetricsState(long lastTime, long lastStatted) {
            this.lastTime = lastTime;
            this.lastStatted = lastStatted;
        }

        public long getLastStatted() {
            return lastStatted;
        }

        public void setLastStatted(long lastStatted) {
            this.lastStatted = lastStatted;
        }

        public long getLastTime() {
            return lastTime;
        }

        public void setLastTime(long lastTime) {
            this.lastTime = lastTime;
        }
    }

    private static class Poller {
        private long startTime = 0;
        private long pollMs = 0;

        private Poller(long startTime, long pollMs) {
            this.startTime = startTime;
            this.pollMs = pollMs;
        }

        public long nextPoll() throws InterruptedException {
            long now = System.currentTimeMillis();
            long cycle = (now - startTime) / pollMs;
            long wakeupTime = startTime + (pollMs * (cycle + 1));
            long sleepTime = wakeupTime - now;
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
            now = System.currentTimeMillis();
            return now;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getPollMs() {
            return pollMs;
        }

        public void setPollMs(long pollMs) {
            this.pollMs = pollMs;
        }
    }

    private HashSet<String> getComponents(Nimbus.Client client, String topology) throws Exception{
        HashSet<String> components = new HashSet<String>();
        ClusterSummary clusterSummary = client.getClusterInfo();
        TopologySummary topologySummary = null;
        for (TopologySummary ts: clusterSummary.get_topologies()) {
            if (topology.equals(ts.get_name())) {
                topologySummary = ts;
                break;
            }
        }
        if (topologySummary == null) {
            throw new IllegalArgumentException("topology: " + topology + " not found");
        } else {
            String id = topologySummary.get_id();
            TopologyInfo info = client.getTopologyInfo(id);
            for (ExecutorSummary es: info.get_executors()) {
                components.add(es.get_component_id());
            }
        }
        return components;
    }

    public void metrics(Nimbus.Client client) throws Exception {
        if (_interval <= 0) {
            throw new IllegalArgumentException("poll interval must be positive");
        }

        if (_topology == null || _topology.isEmpty()) {
            throw new IllegalArgumentException("topology name must be something");
        }

        if (_component == null || _component.isEmpty()) {
            HashSet<String> components = getComponents(client, _topology);
            System.out.println("Available components for " + _topology + " :");
            System.out.println("------------------");
            for (String comp : components) {
                System.out.println(comp);
            }
            System.out.println("------------------");
            System.out.println("Please use -m to specify one component");
            return;
        }

        if (_stream == null || _stream.isEmpty()) {
            throw new IllegalArgumentException("stream name must be something");
        }

        if ( !WATCH_TRANSFERRED.equals(_watch) && !WATCH_EMITTED.equals(_watch)) {
            throw new IllegalArgumentException("watch item must either be transferred or emitted");
        }
        System.out.println("topology\tcomponent\tparallelism\tstream\ttime-diff ms\t" + _watch + "\tthroughput (Kt/s)");

        long pollMs = _interval * 1000;
        long now = System.currentTimeMillis();
        MetricsState state = new MetricsState(now, 0);
        Poller poller = new Poller(now, pollMs);

        do {
            metrics(client, now, state);
            try {
                now = poller.nextPoll();
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        } while (true);
    }

    public void metrics(Nimbus.Client client, long now, MetricsState state) throws Exception {
        long totalStatted = 0;

        int componentParallelism = 0;
        boolean streamFound = false;
        ClusterSummary clusterSummary = client.getClusterInfo();
        TopologySummary topologySummary = null;
        for (TopologySummary ts: clusterSummary.get_topologies()) {
            if (_topology.equals(ts.get_name())) {
                topologySummary = ts;
                break;
            }
        }
        if (topologySummary == null) {
            throw new IllegalArgumentException("topology: " + _topology + " not found");
        } else {
            String id = topologySummary.get_id();
            TopologyInfo info = client.getTopologyInfo(id);
            for (ExecutorSummary es: info.get_executors()) {
                if (_component.equals(es.get_component_id())) {
                    componentParallelism ++;
                    ExecutorStats stats = es.get_stats();
                    if (stats != null) {
                        Map<String,Map<String,Long>> statted =
                                WATCH_EMITTED.equals(_watch) ? stats.get_emitted() : stats.get_transferred();
                        if ( statted != null) {
                            Map<String, Long> e2 = statted.get(":all-time");
                            if (e2 != null) {
                                Long stream = e2.get(_stream);
                                if (stream != null){
                                    streamFound = true;
                                    totalStatted += stream;
                                }
                            }
                        }
                    }
                }
            }
        }

        if (componentParallelism <= 0) {
            HashSet<String> components = getComponents(client, _topology);
            System.out.println("Available components for " + _topology + " :");
            System.out.println("------------------");
            for (String comp : components) {
                System.out.println(comp);
            }
            System.out.println("------------------");
            throw new IllegalArgumentException("component: " + _component + " not found");
        }

        if (!streamFound) {
            throw new IllegalArgumentException("stream: " + _stream + " not found");
        }
        long timeDelta = now - state.getLastTime();
        long stattedDelta = totalStatted - state.getLastStatted();
        state.setLastTime(now);
        state.setLastStatted(totalStatted);
        double throughput = (stattedDelta == 0 || timeDelta == 0) ? 0.0 : ((double)stattedDelta/(double)timeDelta);
        System.out.println(_topology+"\t"+_component+"\t"+componentParallelism+"\t"+_stream+"\t"+timeDelta+"\t"+stattedDelta+"\t"+throughput);
    }

    public void set_interval(int _interval) {
        this._interval = _interval;
    }

    public void set_topology(String _topology) {
        this._topology = _topology;
    }

    public void set_component(String _component) {
        this._component = _component;
    }

    public void set_stream(String _stream) {
        this._stream = _stream;
    }

    public void set_watch(String _watch) {
        this._watch = _watch;
    }
}
