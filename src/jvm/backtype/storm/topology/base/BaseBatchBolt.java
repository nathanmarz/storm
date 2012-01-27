package backtype.storm.topology.base;

import backtype.storm.coordination.IBatchBolt;
import java.util.Map;

public abstract class BaseBatchBolt<T> implements IBatchBolt<T> {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }    
}
