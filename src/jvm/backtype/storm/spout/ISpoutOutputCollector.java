package backtype.storm.spout;

import java.util.List;

public interface ISpoutOutputCollector {
    /**
        Returns the task ids that received the tuples.
    */
    List<Integer> emit(String streamId, List<Object> tuple, Object messageId);
    void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId);
    void reportError(Throwable error);
}

