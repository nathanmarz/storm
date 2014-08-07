package backtype.storm.testing;

import java.util.Map;

import backtype.storm.metric.api.rpc.CountShellMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;

public class PythonShellMetricsBolt extends ShellBolt implements IRichBolt {
	private static final long serialVersionUID = 1999209252187463355L;
	
	public PythonShellMetricsBolt(String[] command) {
		super(command);
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		
		CountShellMetric cMetric = new CountShellMetric();
		context.registerMetric("my-custom-shell-metric", cMetric, 5);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
