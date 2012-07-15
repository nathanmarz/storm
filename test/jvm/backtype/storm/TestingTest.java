package backtype.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.AckFailMapTracker;
import backtype.storm.testing.AckTracker;
import backtype.storm.testing.Cluster;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.IdentityBolt;
import backtype.storm.testing.MkClusterParam;
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
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;

public class TestingTest extends TestCase {

	public void testWithSimulatedTime() {
		assertFalse(Time.isSimulating());
		Testing.withSimulatedTime(new Runnable() {
			@Override
			public void run() {
				assertTrue(Time.isSimulating());
			}
		});
		assertFalse(Time.isSimulating());
	}

	public void testWithLocalCluster() {
		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(2);
		mkClusterParam.setPortsPerSupervisor(5);
		Config daemonConf = new Config();
		daemonConf.put(Config.SUPERVISOR_ENABLE, false);
		daemonConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
		Testing.withLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(Cluster cluster) {
				assertNotNull(cluster.getNimbus());
				assertNotNull(cluster.getClusterState());
			}
		});
	}

	public void testBasicTopology() {
		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		mkClusterParam.setDaemonConf(daemonConf);

		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {

			@Override
			public void run(Cluster cluster) {
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", new TestWordSpout(true), 3);
				builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping(
						"1", new Fields("word"));
				builder.setBolt("3", new TestGlobalCount()).globalGrouping("1");
				builder.setBolt("4", new TestAggregatesCounter())
						.globalGrouping("2");
				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockedData("1", new Values("nathan"),
						new Values("bob"), new Values("joey"), new Values(
								"nathan"));

				// prepare the config
				Config conf = new Config();
				conf.setNumWorkers(2);

				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				// check whether the result is right
				assertTrue(Testing.eq(Utils.list(new Values("nathan"),
						new Values("bob"), new Values("joey"), new Values(
								"nathan")), Testing.readTuples(result, "1")));
				assertTrue(Testing.eq(Utils.list(new Values("nathan", 1),
						new Values("nathan", 2), new Values("bob", 1),
						new Values("joey", 1)), Testing.readTuples(result, "2")));
				assertTrue(Testing.eq(Utils.list(new Values(1), new Values(2),
						new Values(3), new Values(4)), Testing.readTuples(
						result, "3")));
				assertTrue(Testing.eq(Utils.list(new Values(1), new Values(2),
						new Values(3), new Values(4)), Testing.readTuples(
						result, "4")));
			}

		});
	}

	public void testAckBranching() {
		Testing.withTrackedCluster(new TestJob() {
			@Override
			public void run(Cluster cluster) {
				AckTracker tracker = new AckTracker();
				FeederSpout feederSpout = ackTrackingFeeder(tracker, "num");

				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", feederSpout);
				builder.setBolt("2", new IdentityBolt(new Fields("num")))
						.shuffleGrouping("1");
				builder.setBolt("3", new IdentityBolt(new Fields("num")))
						.shuffleGrouping("1");
				builder.setBolt("4", new AggBolt(4)).shuffleGrouping("2")
						.shuffleGrouping("3");
				StormTopology topology = builder.createTopology();

				TrackedTopology tracked = Testing.mkTrackedTopology(cluster,
						topology);

				Testing.submitLocalTopology(cluster.getNimbus(),
						"test-acking2", new Config(), tracked.getTopology());
				feederSpout.feed(Utils.list(1));
				Testing.trackedWait(tracked, 1);
				checker(tracker, 0);
				feederSpout.feed(Utils.list(1));
				Testing.trackedWait(tracked, 1);
				checker(tracker, 2);
			}
		});
	}

	public void testTimeout() {
		Config daemonConfig = new Config();
		daemonConfig.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);

		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setDaemonConf(daemonConfig);
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(Cluster cluster) {
				AckFailMapTracker tracker = new AckFailMapTracker();
				FeederSpout feeder = createFeederSpout("field1");
				feeder.setAckFailDelegate(tracker);

				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", feeder);
				builder.setBolt("2", new AckEveryOtherBolt()).globalGrouping(
						"1");
				StormTopology topology = builder.createTopology();

				Config topologyConfig = new Config();
				topologyConfig.setMessageTimeoutSecs(10);

				Testing.submitLocalTopology(cluster.getNimbus(),
						"timeout-tester", topologyConfig, topology);

				feeder.feed(new Values("a"), 1);
				feeder.feed(new Values("b"), 2);
				feeder.feed(new Values("c"), 3);

				Testing.advanceClusterTime(cluster, 9);
				assertAcked(tracker, 1, 3);
				assertFalse(tracker.isFailed(2));
				Testing.advanceClusterTime(cluster, 12);
				assertFailed(tracker, 2);
			}
		});
	}

	public static void assertAcked(AckFailMapTracker tracker, Object... ids) {
		boolean notAllAcked = true;

		while (notAllAcked) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			int notAckedCnt = 0;
			for (int i = 0; i < ids.length; i++) {
				if (!tracker.isAcked(ids[i])) {
					notAckedCnt += 1;
					break;
				}
			}

			if (notAckedCnt == 0) {
				notAllAcked = false;
			}
		}
	}

	public static void assertFailed(AckFailMapTracker tracker, Object... ids) {
		boolean notAllFailed = true;

		while (notAllFailed) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			int notFailedCnt = 0;
			for (int i = 0; i < ids.length; i++) {
				if (!tracker.isFailed(ids[i])) {
					notFailedCnt += 1;
					break;
				}
			}

			if (notFailedCnt == 0) {
				notAllFailed = false;
			}
		}
	}

	public static FeederSpout ackTrackingFeeder(AckTracker tracker,
			String... fields) {
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

	static class AggBolt extends BaseRichBolt {
		OutputCollector _collector;
		List<Tuple> seen = new ArrayList<Tuple>();
		int amt;

		public AggBolt(int amt) {
			this.amt = amt;
		}

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector collector) {
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

	static class AckEveryOtherBolt extends BaseRichBolt {
		boolean flag = false;
		OutputCollector _collector;

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector collector) {
			_collector = collector;
		}

		public void execute(Tuple input) {
			flag = !flag;

			if (flag) {
				_collector.ack(input);
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {

		}
	}
}
