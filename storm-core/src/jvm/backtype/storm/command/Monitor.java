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
package backtype.storm.command;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.Map;

public class Monitor {
    @Option(name="--help", aliases={"-h"}, usage="print help message")
    private boolean _help = false;

    @Option(name="--interval", aliases={"-i"}, usage="poll frequency in seconds")
    private int _interval = 4;

    @Option(name="--name", aliases={"--topologyName"}, metaVar="NAME",
            usage="base name of the topology (numbers may be appended to the end)")
    private String _name;

    @Option(name="--component", aliases={"--componentName"}, metaVar="NAME",
            usage="component name of the topology")
    private String _component;

    @Option(name="--stat", aliases={"--statItem"}, metaVar="ITEM",
            usage="stat item [emitted | transferred]")
    private String _stat = "emitted";

    private static class MetricsState {
        long lastStatted = 0;
        long lastTime = 0;
    }

    public void metrics(Nimbus.Client client, int poll, String name, String component, String stat) throws Exception {
        System.out.println("status\ttopologie\tslots\tcomponent\texecutors\texecutorsWithMetrics\ttime-diff ms\t" + stat + "\tthroughput (Kt/s)");
        MetricsState state = new MetricsState();
        long pollMs = poll * 1000;
        long now = System.currentTimeMillis();
        state.lastTime = now;
        long startTime = now;
        long cycle, sleepTime, wakeupTime;

        while (metrics(client, name, component, stat, now, state, "WAITING")) {
            now = System.currentTimeMillis();
            cycle = (now - startTime)/pollMs;
            wakeupTime = startTime + (pollMs * (cycle + 1));
            sleepTime = wakeupTime - now;
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
            now = System.currentTimeMillis();
        }

        now = System.currentTimeMillis();
        cycle = (now - startTime)/pollMs;
        wakeupTime = startTime + (pollMs * (cycle + 1));
        sleepTime = wakeupTime - now;
        if (sleepTime > 0) {
            Thread.sleep(sleepTime);
        }
        now = System.currentTimeMillis();
        do {
            metrics(client, name, component, stat, now, state, "RUNNING");
            now = System.currentTimeMillis();
            cycle = (now - startTime)/pollMs;
            wakeupTime = startTime + (pollMs * (cycle + 1));
            sleepTime = wakeupTime - now;
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
            now = System.currentTimeMillis();
        } while (true);
    }

    public boolean metrics(Nimbus.Client client, String name, String component, String stat, long now, MetricsState state, String message) throws Exception {
        long totalStatted = 0;

        boolean topologyFound = false;
        boolean componentFound = false;
        int slotsUsed = 0;
        int executors = 0;
        int executorsWithMetrics = 0;
        ClusterSummary summary = client.getClusterInfo();
        for (TopologySummary ts: summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                topologyFound = true;
                slotsUsed = ts.get_num_workers();
                String id = ts.get_id();
                TopologyInfo info = client.getTopologyInfo(id);
                for (ExecutorSummary es: info.get_executors()) {
                    if (component.equals(es.get_component_id())) {
                        componentFound = true;
                        executors ++;
                        ExecutorStats stats = es.get_stats();
                        if (stats != null) {
                            Map<String,Map<String,Long>> statted =
                                    "emitted".equals(stat) ? stats.get_emitted() : stats.get_transferred();
                            if ( statted != null) {
                                Map<String, Long> e2 = statted.get(":all-time");
                                if (e2 != null) {
                                    executorsWithMetrics++;
                                    //topology messages are always on the default stream, so just count those
                                    Long dflt = e2.get("default");
                                    if (dflt != null){
                                        totalStatted += dflt;
                                    }
                                }
                            }
                        }
                    }


                }
            }
        }

        if (!topologyFound) {
            throw new IllegalArgumentException("topology: " + name + " not found");
        }
        if (!componentFound) {
            throw new IllegalArgumentException("component: " + component + " not fouond");
        }
        long timeDelta = now - state.lastTime;
        long stattedDelta = totalStatted - state.lastStatted;
        state.lastTime = now;
        state.lastStatted = totalStatted;
        double throughput = (stattedDelta == 0 || timeDelta == 0) ? 0.0 : ((double)stattedDelta/(double)timeDelta);
        System.out.println(message+"\t"+name+"\t"+slotsUsed+"\t"+component+"\t"+executors+"\t"+executorsWithMetrics+"\t"+timeDelta+"\t"+stattedDelta+"\t"+throughput);

        return !(executors > 0 && executorsWithMetrics >= executors);
    }

    public void realMain(String[] args) {
        /*** parse command line ***/
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(80);
        try {
            parser.parseArgument(args);
        } catch( CmdLineException e ) {
            System.err.println(e.getMessage());
            _help = true;
        }
        if(_help) {
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        if (_name == null || _name.isEmpty()) {
            throw new IllegalArgumentException("topology name must be something");
        }

        if (_component == null || _component.isEmpty()) {
            throw new IllegalArgumentException("component name must be something");
        }

        if (_interval <= 0) {
            throw new IllegalArgumentException("poll interval must be positive");
        }

        if ( !"transferred".equals(_stat) && !"emitted".equals(_stat)) {
            throw new IllegalArgumentException("stat item must either be transferred or emitted");
        }

        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        try {
            metrics(client, _interval, _name, _component, _stat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Monitor().realMain(args);
    }
}
