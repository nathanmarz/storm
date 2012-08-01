package storm.trident.topology;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;
import java.util.Map;

public interface ITridentBatchBolt extends IComponent {
    void prepare(Map conf, TopologyContext context, BatchOutputCollector collector);
    void execute(BatchInfo batchInfo, Tuple tuple);
    void finishBatch(BatchInfo batchInfo);
    Object initBatchState(String batchGroup, Object batchId);
    void cleanup();    
}
