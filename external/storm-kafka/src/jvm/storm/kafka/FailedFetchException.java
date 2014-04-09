package storm.kafka;

public class FailedFetchException extends RuntimeException {

    public FailedFetchException(String message) {
        super(message);
    }

    public FailedFetchException(Exception e) {
        super(e);
    }
}
