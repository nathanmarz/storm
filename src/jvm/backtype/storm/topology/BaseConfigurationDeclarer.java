package backtype.storm.topology;

import backtype.storm.Config;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseConfigurationDeclarer<T extends ComponentConfigurationDeclarer> implements ComponentConfigurationDeclarer<T> {
    @Override
    public T addConfiguration(String config, Object value) {
        Map configMap = new HashMap();
        configMap.put(config, value);
        return addConfigurations(configMap);
    }

    @Override
    public T setDebug(boolean debug) {
        return addConfiguration(Config.TOPOLOGY_DEBUG, debug);
    }

    @Override
    public T setMaxTaskParallelism(Number val) {
        if(val!=null) val = val.intValue();
        return addConfiguration(Config.TOPOLOGY_MAX_TASK_PARALLELISM, val);
    }

    @Override
    public T setMaxSpoutPending(Number val) {
        if(val!=null) val = val.intValue();
        return addConfiguration(Config.TOPOLOGY_MAX_SPOUT_PENDING, val);
    }
    
    @Override
    public T setNumTasks(Number val) {
        if(val!=null) val = val.intValue();
        return addConfiguration(Config.TOPOLOGY_TASKS, val);
    }
}
