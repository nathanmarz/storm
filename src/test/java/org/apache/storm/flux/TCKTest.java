package org.apache.storm.flux;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import org.apache.storm.flux.model.ExecutionContext;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.junit.Test;
import static org.junit.Assert.*;

public class TCKTest {
    @Test
    public void testTCK() throws Exception {
        TopologyDef topologyDef = FluxParser.parseFile("src/test/resources/configs/tck.yaml", false, true);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testShellComponents() throws Exception {
        TopologyDef topologyDef = FluxParser.parseFile("src/test/resources/configs/shell_test.yaml", false, true);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testKafkaSpoutConfig() throws Exception {
        TopologyDef topologyDef = FluxParser.parseFile("src/test/resources/configs/kafka_test.yaml", false, true);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testLoadFromResource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/kafka_test.yaml", false, true);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }


    @Test
    public void testIncludes() throws Exception {
        TopologyDef topologyDef = FluxParser.parseFile("src/test/resources/configs/include_test.yaml", false, true);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        assertTrue(topologyDef.getName().equals("include-topology"));
        assertTrue(topologyDef.getBolts().size() > 0);
        assertTrue(topologyDef.getSpouts().size() > 0);
        topology.validate();
    }
}
