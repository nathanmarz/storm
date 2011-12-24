package backtype.storm.coordination;

import backtype.storm.tuple.IAnchorableImpl;
import backtype.storm.tuple.Tuple;

public class FinishedTupleImpl implements FinishedTuple, IAnchorableImpl {
    Tuple _delegate;
    
    public FinishedTupleImpl(Tuple delegate) {
        _delegate = delegate;
    }

    public Object getId() {
        return _delegate.getValue(0);
    }

    @Override
    public Tuple getUnderlyingTuple() {
        return _delegate;
    }    
}
