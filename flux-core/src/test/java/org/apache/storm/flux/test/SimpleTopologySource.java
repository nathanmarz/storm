package org.apache.storm.flux.test;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.apache.storm.flux.api.TopologySource;
import org.apache.storm.flux.wrappers.bolts.LogInfoBolt;
import org.apache.storm.flux.wrappers.spouts.FluxShellSpout;

import java.util.Map;

public class SimpleTopologySource implements TopologySource {
    @Override
    public StormTopology getTopology(Map<String, Object> config) {
        TopologyBuilder builder = new TopologyBuilder();

        // spouts
        FluxShellSpout spout = new FluxShellSpout(
                new String[]{"node", "randomsentence.js"},
                new String[]{"word"});
        builder.setSpout("sentence-spout", spout, 1);

        // bolts
        builder.setBolt("log-bolt", new LogInfoBolt(), 1)
                .shuffleGrouping("sentence-spout");

        return builder.createTopology();
    }
}
