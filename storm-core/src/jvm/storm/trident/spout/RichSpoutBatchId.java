package storm.trident.spout;

public class RichSpoutBatchId implements IBatchID {
    long _id;    
    
    public RichSpoutBatchId(long id) {
        _id = id;
    }
    
    @Override
    public Object getId() {
        // this is to distinguish from TransactionAttempt
        return this;
    }

    @Override
    public int getAttemptId() {
        return 0; // each drpc request is always a single attempt
    }

    @Override
    public int hashCode() {
        return ((Long) _id).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof RichSpoutBatchId)) return false;
        RichSpoutBatchId other = (RichSpoutBatchId) o;
        return _id == other._id;
    }
}
