package storm.trident.topology;

import storm.trident.spout.IBatchID;


public class TransactionAttempt implements IBatchID {
    Long _txid;
    int _attemptId;
    
    
    // for kryo compatibility
    public TransactionAttempt() {
        
    }
    
    public TransactionAttempt(Long txid, int attemptId) {
        _txid = txid;
        _attemptId = attemptId;
    }
    
    public Long getTransactionId() {
        return _txid;
    }
    
    public Object getId() {
        return _txid;
    }
    
    public int getAttemptId() {
        return _attemptId;
    }

    @Override
    public int hashCode() {
        return _txid.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof TransactionAttempt)) return false;
        TransactionAttempt other = (TransactionAttempt) o;
        return _txid.equals(other._txid) && _attemptId == other._attemptId;
    }

    @Override
    public String toString() {
        return "" + _txid + ":" + _attemptId;
    }    
}