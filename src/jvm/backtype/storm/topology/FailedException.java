package backtype.storm.topology;

public class FailedException extends RuntimeException {
    public FailedException() {
        super();
    }
    
    public FailedException(String msg) {
        super(msg);
    }
    
    public FailedException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public FailedException(Throwable cause) {
        super(cause);
    }
}
