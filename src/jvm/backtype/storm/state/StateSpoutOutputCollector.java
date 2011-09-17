package backtype.storm.state;

import backtype.storm.utils.Utils;
import java.util.List;


public class StateSpoutOutputCollector extends SynchronizeOutputCollector implements IStateSpoutOutputCollector {
    private IStateSpoutOutputCollector _delegate;
    
    public StateSpoutOutputCollector(IStateSpoutOutputCollector delegate) {
        super(delegate);
        _delegate = delegate;
    }
    
    
    public void remove(List<Object> tuple) {
        remove(Utils.DEFAULT_STREAM_ID, tuple);
    }
    
    @Override
    public void remove(int streamId, List<Object> tuple) {
        _delegate.remove(streamId, tuple);
    }

}
