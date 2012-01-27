package backtype.storm.coordination;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;
import java.io.Serializable;
import java.util.Map;

public interface IBatchBolt<T> extends Serializable, IComponent {
    void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, T id);
    void execute(Tuple tuple);
    void finishBatch();
}
