package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltFailInfo {
    public Tuple tuple;
    public Long failLatencyMs; // null if it wasn't sampled
    
    public BoltFailInfo(Tuple tuple, Long failLatencyMs) {
        this.tuple = tuple;
        this.failLatencyMs = failLatencyMs;
    }
}
