package backtype.storm;

import java.util.Map;

import junit.framework.TestCase;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestAggregatesCounter;
import backtype.storm.testing.TestGlobalCount;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestingTest extends TestCase {
    
    public void testBasicTopology() {
        Testing.withSimulatedTimeLocalCluster(new TestJob() {
            @Override
            public Object run(Map clusterMap) {
                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("1", new TestWordSpout(true), 3);
                builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping("1", new Fields("word"));
                builder.setBolt("3", new TestGlobalCount()).globalGrouping("1");
                builder.setBolt("4", new TestAggregatesCounter()).globalGrouping("2");
                StormTopology topology = builder.createTopology();
                
                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockedData("1", new Values("nathan"), new Values("bob"), new Values("joey"), new Values("nathan"));
                
                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);
                
                // complete the topology
                Map result = Testing.completeTopology(clusterMap, topology, mockedSources, conf);
                
                // check whether the result is right
                assertTrue(Testing.eq(Utils.list(new Values("nathan"), new Values("bob"), new Values("joey"), new Values("nathan")), 
                                               Testing.readTuples(result, "1")));
                assertTrue(Testing.eq(Utils.list(new Values("nathan", 1), new Values("nathan", 2), new Values("bob", 1), new Values("joey", 1)), 
                		Testing.readTuples(result, "2")));
                assertTrue(Testing.eq(Utils.list(new Values(1), new Values(2), new Values(3), new Values(4)), 
                                               Testing.readTuples(result, "3")));
                assertTrue(Testing.eq(Utils.list(new Values(1), new Values(2), new Values(3), new Values(4)), 
                                               Testing.readTuples(result, "4")));
                return null;
            }
            
        });
    }
}
