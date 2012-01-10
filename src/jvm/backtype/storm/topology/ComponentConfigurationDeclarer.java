package backtype.storm.topology;

import java.util.Map;

public interface ComponentConfigurationDeclarer<T extends ComponentConfigurationDeclarer> {
    T addConfigurations(Map conf);
    T addConfiguration(String config, Object value);
    T setDebug(boolean debug);
    T setMaxTaskParallelism(Integer val);
    T setMaxSpoutPending(Integer val);    
}
