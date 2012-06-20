package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltAckInfo {
    public Tuple tuple;
    public Long processLatencyMs; // null if it wasn't sampled
    
    public BoltAckInfo(Tuple tuple, Long processLatencyMs) {
        this.tuple = tuple;
        this.processLatencyMs = processLatencyMs;
    }
}
