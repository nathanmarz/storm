package backtype.storm.topology;

public class ReportedFailedException extends FailedException {
    public ReportedFailedException() {
        super();
    }
    
    public ReportedFailedException(String msg) {
        super(msg);
    }
    
    public ReportedFailedException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ReportedFailedException(Throwable cause) {
        super(cause);
    }
}
