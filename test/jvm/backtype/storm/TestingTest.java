package backtype.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.AckTracker;
import backtype.storm.testing.Cluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestAggregatesCounter;
import backtype.storm.testing.TestGlobalCount;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.testing.TrackedTopology;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestingTest extends TestCase {
    
    public void testBasicTopology() {
        Testing.withSimulatedTimeLocalCluster(new TestJob() {
            @Override
            public void run(Cluster cluster) {
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
                Map result = Testing.completeTopology(cluster, topology, mockedSources, conf);
                
                // check whether the result is right
                assertTrue(Testing.eq(Utils.list(new Values("nathan"), new Values("bob"), new Values("joey"), new Values("nathan")), 
                                               Testing.readTuples(result, "1")));
                assertTrue(Testing.eq(Utils.list(new Values("nathan", 1), new Values("nathan", 2), new Values("bob", 1), new Values("joey", 1)), 
                		Testing.readTuples(result, "2")));
                assertTrue(Testing.eq(Utils.list(new Values(1), new Values(2), new Values(3), new Values(4)), 
                                               Testing.readTuples(result, "3")));
                assertTrue(Testing.eq(Utils.list(new Values(1), new Values(2), new Values(3), new Values(4)), 
                                               Testing.readTuples(result, "4")));
            }
            
        });
    }
    
    public void testAckBranching() {
    	Testing.withTrackedCluster(new TestJob() {
			@Override
			public void run(Cluster cluster) {
				AckTracker tracker = new AckTracker();
				FeederSpout feederSpout =  ackTrackingFeeder(tracker, "num");
								
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", feederSpout);
				builder.setBolt("2", new IdentityBolt()).shuffleGrouping("1");
				builder.setBolt("3", new IdentityBolt()).shuffleGrouping("1");
				builder.setBolt("4", new AggBolt(4)).shuffleGrouping("2").shuffleGrouping("3");
				StormTopology topology = builder.createTopology();
				
				TrackedTopology tracked = Testing.mkTrackedTopology(cluster, topology);
				
				Testing.submitLocalTopology(cluster.getNimbus(), "test-acking2", new Config(), tracked.getTopology());
				feederSpout.feed(Utils.list(1));
				Testing.trackedWait(tracked, 1);
				checker(tracker, 0);
				feederSpout.feed(Utils.list(1));
				Testing.trackedWait(tracked, 1);
				checker(tracker, 2);
			}
    	});
    }
    
    public static FeederSpout ackTrackingFeeder(AckTracker tracker, String... fields) {
    	FeederSpout feeder = createFeederSpout(fields);
    	feeder.setAckFailDelegate(tracker);
    	
    	return feeder;
    }
    
    public static FeederSpout createFeederSpout(String... fields) {
    	return new FeederSpout(new Fields(fields));
    }
    
    public static void checker(AckTracker tracker, int val) {
    	assertEquals(val, tracker.getNumAcks());
    	tracker.resetNumAcks();
    }

    static class IdentityBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }
        
        public void execute(Tuple input) {
        	_collector.emit(input, input.getValues());
        	_collector.ack(input);
        }

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("num"));
		}
    }
    
    static class AggBolt extends BaseRichBolt {
        OutputCollector _collector;
        List<Tuple> seen = new ArrayList<Tuple>();
        int amt;

        public AggBolt(int amt) {
        	this.amt = amt;
        }
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }
        
        public void execute(Tuple input) {
        	seen.add(input);
        	
        	if (seen.size() == this.amt) {
        		_collector.emit(seen, new Values(1));
        		
            	for (Tuple tuple : seen) {
            		_collector.ack(tuple);
            	}
            	
            	seen.clear();
        	}
        }

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("num"));
		}
    }
}
