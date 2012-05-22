package backtype.storm.tuple;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.TopologyContext;
import java.util.List;

public class TupleImpl extends Tuple {
    public TupleImpl(GeneralTopologyContext context, List<Object> values, int taskId, String streamId, MessageId id) {
        super(context, values, taskId, streamId, id);
    }

    public TupleImpl(GeneralTopologyContext context, List<Object> values, int taskId, String streamId) {
        super(context, values, taskId, streamId);
    }    
    
    Long _sampleStartTime = null;
    
    public void setSampleStartTime(long ms) {
        _sampleStartTime = ms;
    }

    public Long getSampleStartTime() {
        return _sampleStartTime;
    }
    
    long _outAckVal = 0;
    
    public void updateAckVal(long val) {
        _outAckVal = _outAckVal ^ val;
    }
    
    public long getAckVal() {
        return _outAckVal;
    }
    
    
}
