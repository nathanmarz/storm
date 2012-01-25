package backtype.storm.topology.base;

import backtype.storm.transactional.ICommitterBolt;
import java.util.Map;

public abstract class BaseCommitterBolt implements ICommitterBolt {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }    
}
