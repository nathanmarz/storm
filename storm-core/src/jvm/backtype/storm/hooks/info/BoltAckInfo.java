package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltAckInfo {
    public Tuple tuple;
    public int ackingTaskId;
    public Long processLatencyMs; // null if it wasn't sampled
    
    public BoltAckInfo(Tuple tuple, int ackingTaskId, Long processLatencyMs) {
        this.tuple = tuple;
        this.ackingTaskId = ackingTaskId;
        this.processLatencyMs = processLatencyMs;
    }
}
