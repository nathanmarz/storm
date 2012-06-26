package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class bolthAckInfo {
    public Tuple tuple;
    public Long processLatencyMs; // null if it wasn't sampled
    
    public bolthAckInfo(Tuple tuple, Long processLatencyMs) {
        this.tuple = tuple;
        this.processLatencyMs = processLatencyMs;
    }
}
