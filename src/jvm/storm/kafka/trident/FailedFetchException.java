package storm.kafka.trident;

public class FailedFetchException extends RuntimeException {
    public FailedFetchException(Exception e) {
        super(e);
    }
}
