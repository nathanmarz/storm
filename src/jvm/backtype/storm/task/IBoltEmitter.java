package backtype.storm.task;

import backtype.storm.tuple.IAnchorable;
import java.util.Collection;
import java.util.List;

public interface IBoltEmitter {
    /**
        Returns the task ids that received the tuples.
    */
    List<Integer> emit(String streamId, Collection<IAnchorable> anchors, List<Object> tuple);
    void emitDirect(int taskId, String streamId, Collection<IAnchorable> anchors, List<Object> tuple);    
}
