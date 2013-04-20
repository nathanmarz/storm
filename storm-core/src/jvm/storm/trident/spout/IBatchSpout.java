package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.Map;
import storm.trident.operation.TridentCollector;

public interface IBatchSpout extends Serializable {
    void open(Map conf, TopologyContext context);
    void emitBatch(long batchId, TridentCollector collector);
    void ack(long batchId);
    void close();
    Map getComponentConfiguration();
    Fields getOutputFields();
}
