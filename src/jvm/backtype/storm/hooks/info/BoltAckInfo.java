package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltAckInfo {
    public Tuple tuple;
    public long processLatencyMs;
    
    public BoltAckInfo(Tuple tuple, long processLatencyMs) {
        this.tuple = tuple;
        this.processLatencyMs = processLatencyMs;
    }
}
