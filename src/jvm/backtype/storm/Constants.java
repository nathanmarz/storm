package backtype.storm;

import backtype.storm.drpc.CoordinatedBolt;


public class Constants {
    public static final String COORDINATED_STREAM_ID = CoordinatedBolt.class.getName() + "/coord-stream";
    public static final String TRANSACTION_BATCH_STREAM_ID = "__txbatch";
    public static final String TRANSACTION_COMMIT_STREAM_ID = "__txcommit";
}
