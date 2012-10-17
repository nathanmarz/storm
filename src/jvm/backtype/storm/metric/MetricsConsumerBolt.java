package backtype.storm.metric;

import backtype.storm.Config;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.Map;

public class MetricsConsumerBolt implements IBolt {
    IMetricsConsumer _metricsConsumer;
    String _consumerClassName;
    OutputCollector _collector;
    Object _registrationArgument;

    public MetricsConsumerBolt(String consumerClassName, Object registrationArgument) {
        _consumerClassName = consumerClassName;
        _registrationArgument = registrationArgument;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            _metricsConsumer = (IMetricsConsumer)Class.forName(_consumerClassName).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate a class listed in config under section " +
                Config.TOPOLOGY_METRICS_CONSUMER_REGISTER + " with fully qualified name " + _consumerClassName, e);
        }
        _metricsConsumer.prepare(stormConf, _registrationArgument, context);
        _collector = collector;
    }
    
    @Override
    public void execute(Tuple input) {
        IMetricsConsumer.DataPoint d = new IMetricsConsumer.DataPoint();
        d.srcComponentId = input.getSourceComponent(); 
        d.srcTaskId = input.getSourceTask(); 
        d.srcWorkerHost = input.getString(0);
        d.srcWorkerPort = input.getInteger(1);
        d.updateIntervalSecs = input.getInteger(2);
        d.timestamp = input.getLong(3);
        d.name = input.getString(4);
        d.value = input.getValue(5);

        _metricsConsumer.handleDataPoint(d);
        _collector.ack(input);
    }

    @Override
    public void cleanup() {
        _metricsConsumer.cleanup();
    }
    
}
