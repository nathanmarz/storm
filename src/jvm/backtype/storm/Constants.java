package backtype.storm;

import backtype.storm.coordination.CoordinatedBolt;


public class Constants {
    public static final String COORDINATED_STREAM_ID = CoordinatedBolt.class.getName() + "/coord-stream"; 

    public static final String SYSTEM_COMPONENT_ID = "__system";
    public static final String SYSTEM_TICK_STREAM_ID = "__tick";
    public static final String METRICS_COMPONENT_ID_PREFIX = "__metrics";
    public static final String METRICS_STREAM_ID = "__metrics";
    public static final String METRICS_TICK_STREAM_ID = "__metrics_tick";
}
    