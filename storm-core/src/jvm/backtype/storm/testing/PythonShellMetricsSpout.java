package backtype.storm.testing;

import java.util.Map;

import backtype.storm.metric.api.rpc.CountShellMetric;
import backtype.storm.spout.ShellSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class PythonShellMetricsSpout extends ShellSpout implements IRichSpout {
	private static final long serialVersionUID = 1999209252187463355L;

	public PythonShellMetricsSpout(String[] command) {
		super(command);
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
	
		CountShellMetric cMetric = new CountShellMetric();
		context.registerMetric("my-custom-shellspout-metric", cMetric, 5);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("field1"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
