package backtype.storm.hooks.info;

import backtype.storm.tuple.Tuple;

public class BoltExecuteInfo {
    public Tuple tuple;
    public int executingTaskId;
    public Long executeLatencyMs; // null if it wasn't sampled
    
    public BoltExecuteInfo(Tuple tuple, int executingTaskId, Long executeLatencyMs) {
        this.tuple = tuple;
        this.executingTaskId = executingTaskId;
        this.executeLatencyMs = executeLatencyMs;
    }
}
