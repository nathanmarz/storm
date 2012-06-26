package backtype.storm;

import backtype.storm.coordination.Coordinatedbolth;


public class Constants {
    public static final String COORDINATED_STREAM_ID = Coordinatedbolth.class.getName() + "/coord-stream"; 

    public static final String SYSTEM_COMPONENT_ID = "__system";
    public static final String SYSTEM_TICK_STREAM_ID = "__tick";
}
