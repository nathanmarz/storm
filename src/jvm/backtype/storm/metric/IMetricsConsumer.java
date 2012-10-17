package backtype.storm.metric;

import backtype.storm.task.TopologyContext;
import java.util.Map;

public interface IMetricsConsumer {
    public static class DataPoint {
        public String srcWorkerHost;
        public int srcWorkerPort;
        public String srcComponentId; 
        public int srcTaskId; 
        public long timestamp;
        public int updateIntervalSecs; 
        public String name; 
        public Object value;
    }

    void prepare(Map stormConf, Object registrationOptions, TopologyContext context);
    void handleDataPoint(DataPoint dataPoint);
    void cleanup();
}