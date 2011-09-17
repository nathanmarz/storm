package backtype.storm.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.Map;

public interface IBasicBolt extends IComponent {
    void prepare(Map stormConf, TopologyContext context);
    void execute(Tuple input, BasicOutputCollector collector);
    void cleanup();
}