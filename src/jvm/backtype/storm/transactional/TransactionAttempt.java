package backtype.storm.transactional;

import java.math.BigInteger;

public class TransactionAttempt {
    BigInteger _txid;
    long _attemptId;
    
    
    // for kryo compatibility
    public TransactionAttempt() {
        
    }
    
    public TransactionAttempt(BigInteger txid, long attemptId) {
        _txid = txid;
        _attemptId = attemptId;
    }
    
    public BigInteger getTransactionId() {
        return _txid;
    }
    
    public long getAttemptId() {
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
