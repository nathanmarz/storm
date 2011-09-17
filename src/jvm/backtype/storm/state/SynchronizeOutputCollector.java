package backtype.storm.state;

import backtype.storm.utils.Utils;
import java.util.List;


public class SynchronizeOutputCollector implements ISynchronizeOutputCollector {
    private ISynchronizeOutputCollector _delegate;
    
    public SynchronizeOutputCollector(ISynchronizeOutputCollector delegate) {
        _delegate = delegate;
    }
    
    
    public void add(List<Object> tuple) {
        add(Utils.DEFAULT_STREAM_ID, tuple);
    }
    
    @Override
    public void add(int streamId, List<Object> tuple) {
        _delegate.add(streamId, tuple);
    }

    @Override
    public void resynchronize() {
        _delegate.resynchronize();
    }    
}
