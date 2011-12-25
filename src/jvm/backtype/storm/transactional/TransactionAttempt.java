package backtype.storm.transactional;

public class TransactionAttempt implements Comparable<TransactionAttempt> {
    int _txid;
    long _attemptId;
    
    public TransactionAttempt(int txid, long attemptId) {
        _txid = txid;
        _attemptId = attemptId;
    }
    
    public int getTransactionId() {
        return _txid;
    }
    
    public long getAttemptId() {
        return _attemptId;
    }

    @Override
    public int hashCode() {
        return 13 * _txid + (int) _attemptId;
    }

    @Override
    public boolean equals(Object o) {
        TransactionAttempt other = (TransactionAttempt) o;
        return _txid == other._txid && _attemptId == other._attemptId;
    }

    @Override
    public int compareTo(TransactionAttempt t2) {
        int t = ((Integer)getTransactionId()).compareTo(t2.getTransactionId());
        if(t!=0) return t;
        return ((Long)getAttemptId()).compareTo(t2.getAttemptId());
    }
}
