package backtype.storm.task;

import backtype.storm.tuple.IAnchorable;
import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.List;

public interface IOutputCollector {
    /**
        Returns the task ids that received the tuples.
    */
    List<Integer> emit(String streamId, Collection<IAnchorable> anchors, List<Object> tuple);
    void emitDirect(int taskId, String streamId, Collection<IAnchorable> anchors, List<Object> tuple);
    void ack(Tuple input);
    void fail(Tuple input);
    void reportError(Throwable error);
}
