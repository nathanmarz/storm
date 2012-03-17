package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltFailInfo {
    public Tuple tuple;
    public long failLatencyMs;
    
    public BoltFailInfo(Tuple tuple, long failLatencyMs) {
        this.tuple = tuple;
        this.failLatencyMs = failLatencyMs;
    }
}
