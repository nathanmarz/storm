package backtype.storm.transactional;

import java.util.List;

public interface ITransactionalSpoutOutputCollector {
    /**
        Returns the task ids that received the tuples.
    */
    List<Integer> emit(String streamId, List<Object> tuple);
    void emitDirect(int taskId, String streamId, List<Object> tuple);    
}
