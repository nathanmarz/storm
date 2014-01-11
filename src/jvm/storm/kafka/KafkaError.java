package storm.kafka;

/**
 * Date: 11/01/2014
 * Time: 14:21
 */
public enum KafkaError {
    NO_ERROR,
    OFFSET_OUT_OF_RANGE,
    INVALID_MESSAGE,
    UNKNOWN_TOPIC_OR_PARTITION,
    INVALID_FETCH_SIZE,
    LEADER_NOT_AVAILABLE,
    NOT_LEADER_FOR_PARTITION,
    REQUEST_TIMED_OUT,
    BROKER_NOT_AVAILABLE,
    REPLICA_NOT_AVAILABLE,
    MESSAGE_SIZE_TOO_LARGE,
    STALE_CONTROLLER_EPOCH,
    UNKNOWN;

    public static KafkaError getError(short errorCode) {
        if (errorCode < 0) {
            return UNKNOWN;
        } else {
            return values()[errorCode];
        }
    }
}
