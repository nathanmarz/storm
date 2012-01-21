package backtype.storm.testing;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.IAnchorable;
import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.List;

public class DelegateOutputCollector extends OutputCollector {
    IOutputCollector _delegate;
    
    public DelegateOutputCollector(IOutputCollector delegate) {
        _delegate = delegate;
    }
    
    @Override
    public List<Integer> emit(String streamId, Collection<IAnchorable> anchors, List<Object> tuple) {
        return _delegate.emit(streamId, anchors, tuple);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<IAnchorable> anchors, List<Object> tuple) {
        _delegate.emitDirect(taskId, streamId, anchors, tuple);
    }

    @Override
    public void ack(Tuple input) {
        _delegate.ack(input);
    }

    @Override
    public void fail(Tuple input) {
        _delegate.fail(input);
    }

    @Override
    public void reportError(Throwable error) {
        _delegate.reportError(error);
    }
}
