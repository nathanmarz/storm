package backtype.storm.spout;

import java.util.Map;

/**
 * The strategy a spout needs to use when its waiting. Waiting is
 * triggered in one of two conditions:
 * 
 * 1. nextTuple emits no tuples
 * 2. The spout has hit maxSpoutPending and can't emit any more tuples
 * 
 * The default strategy sleeps for one millisecond.
 */
public interface ISpoutWaitStrategy {
    void prepare(Map conf);
    void emptyEmit(long streak);
}
