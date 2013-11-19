package backtype.storm;

import backtype.storm.coordination.CoordinatedBolt;

import java.util.ArrayList;
import java.util.Arrays;


public class Constants {
    public static final String COORDINATED_STREAM_ID = CoordinatedBolt.class.getName() + "/coord-stream";

    public static final long SYSTEM_TASK_ID = -1;
    public static final ArrayList<Integer> SYSTEM_EXECUTOR_ID = new ArrayList<Integer>(Arrays.asList(-1,-1));
    public static final String SYSTEM_COMPONENT_ID = "__system";
    public static final String SYSTEM_TICK_STREAM_ID = "__tick";
    public static final String METRICS_COMPONENT_ID_PREFIX = "__metrics";
    public static final String METRICS_STREAM_ID = "__metrics";
    public static final String METRICS_TICK_STREAM_ID = "__metrics_tick";
}
    