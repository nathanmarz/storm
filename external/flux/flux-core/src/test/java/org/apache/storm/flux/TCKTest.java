/*
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
package org.apache.storm.flux;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.flux.model.ExecutionContext;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.apache.storm.flux.test.TestBolt;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TCKTest {
    @Test
    public void testTCK() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/tck.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testShellComponents() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/shell_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testKafkaSpoutConfig() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/kafka_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testLoadFromResource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/kafka_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }


    @Test
    public void testHdfs() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/hdfs_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testDiamondTopology() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/diamond-topology.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }


    @Test
    public void testHbase() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/simple_hbase.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadHbase() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/bad_hbase.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testIncludes() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/include_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        assertTrue(topologyDef.getName().equals("include-topology"));
        assertTrue(topologyDef.getBolts().size() > 0);
        assertTrue(topologyDef.getSpouts().size() > 0);
        topology.validate();
    }

    @Test
    public void testTopologySource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testTopologySourceWithReflection() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-reflection.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testTopologySourceWithConfigParam() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-reflection-config.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testTopologySourceWithMethodName() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-method-override.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }


    @Test
    public void testTridentTopologySource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-trident.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTopologySource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/invalid-existing-topology.yaml", false, true, null, false);
        assertFalse("Topology config is invalid.", topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
    }


    @Test
    public void testTopologySourceWithGetMethodName() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-reflection.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testTopologySourceWithConfigMethods() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/config-methods-test.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();

        // make sure the property was actually set
        TestBolt bolt = (TestBolt)context.getBolt("bolt-1");
        assertTrue(bolt.getFoo().equals("foo"));
        assertTrue(bolt.getBar().equals("bar"));
        assertTrue(bolt.getFooBar().equals("foobar"));
    }

    @Test
    public void testVariableSubstitution() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/substitution-test.yaml", false, true, "src/test/resources/configs/test.properties", true);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();

        // test basic substitution
        assertEquals("Property not replaced.",
                "substitution-topology",
                context.getTopologyDef().getName());

        // test environment variable substitution
        // $PATH should be defined on most systems
        String envPath = System.getenv().get("PATH");
        assertEquals("ENV variable not replaced.",
                envPath,
                context.getTopologyDef().getConfig().get("test.env.value"));

    }
}
