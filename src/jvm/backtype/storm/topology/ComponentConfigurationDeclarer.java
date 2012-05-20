package backtype.storm.topology;

import java.util.Map;

public interface ComponentConfigurationDeclarer<T extends ComponentConfigurationDeclarer> {
    T addConfigurations(Map conf);
    T addConfiguration(String config, Object value);
    T setDebug(boolean debug);
    T setMaxTaskParallelism(Number val);
    T setMaxSpoutPending(Number val);
    T setNumTasks(Number val);
}
