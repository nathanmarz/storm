package backtype.storm.testing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class TestClassLoaderTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TestClassLoaderSpout());
		builder.setBolt("bolt", new TestClassLoaderBolt()).allGrouping("spout");
		StormTopology topology = builder.createTopology();
		
		ILocalCluster cluster = new LocalCluster();
		cluster.submitTopology("classloader-test", Collections.EMPTY_MAP, topology);
		Thread.sleep(3000);
		cluster.shutdown();
	}
	
	@SuppressWarnings("serial")
	public static class TestClassLoaderSpout extends BaseRichSpout {
		private SpoutOutputCollector _collector;
		private boolean sent = false;
		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void nextTuple() {
			if (sent == false) {
				_collector.emit(new Values("msg"));
				sent = true;
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("msg"));
		}
	}

	public static class TestClassLoaderBolt extends BaseRichBolt {
		private static final long serialVersionUID = 8886285645997459434L;

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// empty
		}

		@Override
		public void execute(Tuple input) {
			// Write the output to a file, so we can check it later(by the run.sh)
			File file = new File("/tmp/storm-classloader-test.txt");
			FileOutputStream stream = null;
			try {
				stream = new FileOutputStream(file);
				stream.write("\n\n=======================================================================\n".getBytes());
				// Bar.foo will load the backtype.storm.testing.Foo from Storm core
				stream.write((Bar.foo() + "\n").getBytes());
				// The following loads the backtype.storm.testing.Foo from classloader_test.
				stream.write(("classloader_test::backtype.storm.testing.Foo loaded by " + Foo.class.getClassLoader() + "\n").getBytes());
				stream.write("=======================================================================\n".getBytes());
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (stream != null) {
					try {
						stream.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// empty
		}
	}
}
