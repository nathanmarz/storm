package backtype.storm.transactional;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.util.List;

public class TransactionalOutputCollectorImpl extends TransactionalOutputCollector {
    OutputCollector _collector;
    
    public TransactionalOutputCollectorImpl(OutputCollector collector) {
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
    
    public void ack(Tuple tup) {
        _collector.ack(tup);
    }
    
    public void fail(Tuple tup) {
        _collector.fail(tup);
    }
}
