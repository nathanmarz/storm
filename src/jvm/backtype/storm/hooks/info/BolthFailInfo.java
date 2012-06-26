package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class bolthFailInfo {
    public Tuple tuple;
    public Long failLatencyMs; // null if it wasn't sampled
    
    public bolthFailInfo(Tuple tuple, Long failLatencyMs) {
        this.tuple = tuple;
        this.failLatencyMs = failLatencyMs;
    }
}
