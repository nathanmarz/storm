package backtype.storm.transactional;

import backtype.storm.tuple.IAnchorable;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;


public class TransactionTuple implements IAnchorable {
    Tuple _delegate;
    
    public TransactionTuple(Tuple delegate) {
        _delegate = delegate;
    }
    
    public int getTransactionId() {
        return ((TransactionAttempt) _delegate.getValue(0)).getTransactionId();
    }
    
    @Override
    public MessageId getMessageId() {
        return _delegate.getMessageId();
    }

    @Override
    public boolean equals(Object other) {
        // for OutputCollector
        return this == other;
    }
    
    @Override
    public int hashCode() {
        // for OutputCollector
        return System.identityHashCode(this);
    }
}
