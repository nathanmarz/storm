package backtype.storm.transactional;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.IAnchorable;
import backtype.storm.tuple.Tuple;
import java.util.List;

public class TransactionalOutputCollectorImpl extends TransactionalOutputCollector {
    OutputCollector _collector;
    IAnchorable _anchor;
    
    public TransactionalOutputCollectorImpl(OutputCollector collector) {
        _collector = collector;
    }
    
    public void setAnchor(IAnchorable anchor) {
        _anchor = anchor;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple) {
        return _collector.emit(streamId, _anchor, tuple);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        _collector.emitDirect(taskId, streamId, _anchor, tuple);
    }
    
    public void ack(Tuple tup) {
        _collector.ack(tup);
    }
    
    public void fail(Tuple tup) {
        _collector.fail(tup);
    }
}
