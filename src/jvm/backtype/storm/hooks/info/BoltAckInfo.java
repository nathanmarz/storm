package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltAckInfo {
    public Tuple tuple;
    public long completeLatencyMs;
    
    public BoltAckInfo(Tuple tuple, long completeLatencyMs) {
        this.tuple = tuple;
        this.completeLatencyMs = completeLatencyMs;
    }
}
