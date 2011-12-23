package backtype.storm.transactional;

import backtype.storm.utils.Utils;
import java.util.List;

public class TransactionalSpoutOutputCollector implements ITransactionalSpoutOutputCollector {
    ITransactionalSpoutOutputCollector _delegate;

    public TransactionalSpoutOutputCollector(ITransactionalSpoutOutputCollector delegate) {
        _delegate = delegate;
    }

    /**
     * Emits a tuple to the default output stream.
     */
    public List<Integer> emit(List<Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple);
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {
        return _delegate.emit(Utils.DEFAULT_STREAM_ID, tuple);
    }
    
    /**
     * Emits a tuple to the specified task on the default output stream. This output
     * stream must have been declared as a direct stream, and the specified task must
     * use a direct grouping on this stream to receive the message.
     */
    public void emitDirect(int taskId, List<Object> tuple) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
    }
    
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        _delegate.emitDirect(taskId, streamId, tuple);
    }
}
