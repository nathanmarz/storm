package org.apache.storm.flux.test;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.apache.storm.flux.wrappers.bolts.LogInfoBolt;
import org.apache.storm.flux.wrappers.spouts.FluxShellSpout;

import java.util.Map;

/**
 * Test topology source that does not implement TopologySource, but has the same
 * `getTopology()` method.
 */
public class SimpleTopologyWithConfigParam {


    public SimpleTopologyWithConfigParam(){}

    public SimpleTopologyWithConfigParam(String foo, String bar){}


    public StormTopology getTopology(Config config) {
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
