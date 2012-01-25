package backtype.storm.transactional;

public class FailedBatchException extends RuntimeException {
    public FailedBatchException() {
        super();
    }
    
    public FailedBatchException(String msg) {
        super(msg);
    }
    
    public FailedBatchException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public FailedBatchException(Throwable cause) {
        super(cause);
    }
}
