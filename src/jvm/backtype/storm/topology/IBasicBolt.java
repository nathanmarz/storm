package backtype.storm.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.Map;

public interface IBasicBolt extends IComponent {
    void prepare(Map stormConf, TopologyContext context);
    /**
     * Process the input tuple and optionally emit new tuples based on the input tuple.
     * 
     * All acking is managed for you. Throw a FailedException if you want to fail the tuple.
     */
    void execute(Tuple input, BasicOutputCollector collector);
    void cleanup();
}