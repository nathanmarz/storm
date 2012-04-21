package backtype.storm.coordination;

import backtype.storm.utils.Utils;
import java.util.List;

public abstract class BatchOutputCollector {

    /**
     * Emits a tuple to the default output stream.
     */
    public List<Integer> emit(List<Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple);
    }

    public abstract List<Integer> emit(String streamId, List<Object> tuple);
    
    /**
     * Emits a tuple to the specified task on the default output stream. This output
     * stream must have been declared as a direct stream, and the specified task must
     * use a direct grouping on this stream to receive the message.
     */
    public void emitDirect(int taskId, List<Object> tuple) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
    }
    
    public abstract void emitDirect(int taskId, String streamId, List<Object> tuple); 
    
    public abstract void reportError(Throwable error);
}
