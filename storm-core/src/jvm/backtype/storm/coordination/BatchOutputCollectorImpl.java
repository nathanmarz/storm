package backtype.storm.coordination;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.util.List;

public class BatchOutputCollectorImpl extends BatchOutputCollector {
    OutputCollector _collector;
    
    public BatchOutputCollectorImpl(OutputCollector collector) {
        _collector = collector;
    }
    
    @Override
    public List<Integer> emit(String streamId, List<Object> tuple) {
        return _collector.emit(streamId, tuple);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        _collector.emitDirect(taskId, streamId, tuple);
    }

    @Override
    public void reportError(Throwable error) {
        _collector.reportError(error);
    }
    
    public void ack(Tuple tup) {
        _collector.ack(tup);
    }
    
    public void fail(Tuple tup) {
        _collector.fail(tup);
    }
}
