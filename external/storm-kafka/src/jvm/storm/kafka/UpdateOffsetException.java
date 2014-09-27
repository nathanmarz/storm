package storm.kafka;

public class UpdateOffsetException extends RuntimeException {

    public final Long startOffset;

    public UpdateOffsetException(Long _offset) {
        this.startOffset = _offset;
    }
}
