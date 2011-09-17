package backtype.storm.task;

import java.util.List;
import backtype.storm.tuple.Tuple;

public interface IOutputCollector {
    /**
        Returns the task ids that received the tuples.
    */
    List<Integer> emit(int streamId, List<Tuple> anchors, List<Object> tuple);
    void emitDirect(int taskId, int streamId, List<Tuple> anchors, List<Object> tuple);
    void ack(Tuple input);
    void fail(Tuple input);
    void reportError(Throwable error);
}
