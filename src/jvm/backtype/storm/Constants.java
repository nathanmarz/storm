package backtype.storm;

import backtype.storm.coordination.CoordinatedBolt;


public class Constants {
    public static final String COORDINATED_STREAM_ID = CoordinatedBolt.class.getName() + "/coord-stream"; 

    public static final String SYSTEM_COMPONENT_ID = "__system";
    public static final String SYSTEM_TICK_STREAM_ID = "__tick";

    // LocalState constants
    public static final String LS_WORKER_HEARTBEAT = "worker-heartbeat";
    public static final String LS_ID = "supervisor-id";
    public static final String LS_LOCAL_ASSIGNMENTS = "local-assignments";
    public static final String LS_APPROVED_WORKERS = "approved-workers";
}
