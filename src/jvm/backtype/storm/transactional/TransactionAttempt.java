package backtype.storm.transactional;

public class TransactionAttempt {
    int _txid;
    int _attemptId;
    
    public TransactionAttempt(int txid, int attemptId) {
        _txid = txid;
        _attemptId = attemptId;
    }
    
    public int getTransactionId() {
        return _txid;
    }
    
    public int getAttemptId() {
        return _attemptId;
    }

    @Override
    public int hashCode() {
        return 13 * _txid + _attemptId;
    }

    @Override
    public boolean equals(Object o) {
        TransactionAttempt other = (TransactionAttempt) o;
        return _txid == other._txid && _attemptId == other._attemptId;
    }
}
