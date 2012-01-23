package backtype.storm.transactional;

public class FailedTransactionException extends RuntimeException {
    public FailedTransactionException() {
        super();
    }
    
    public FailedTransactionException(String msg) {
        super(msg);
    }
    
    public FailedTransactionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public FailedTransactionException(Throwable cause) {
        super(cause);
    }
}
