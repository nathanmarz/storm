package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltFailInfo {
    public Tuple tuple;
    public int failingTaskId;
    public Long failLatencyMs; // null if it wasn't sampled
    
    public BoltFailInfo(Tuple tuple, int failingTaskId, Long failLatencyMs) {
        this.tuple = tuple;
        this.failingTaskId = failingTaskId;
        this.failLatencyMs = failLatencyMs;
    }
}
