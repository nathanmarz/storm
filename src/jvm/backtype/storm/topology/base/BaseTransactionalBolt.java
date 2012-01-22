package backtype.storm.topology.base;

import backtype.storm.transactional.ITransactionalBolt;
import java.util.Map;

public abstract class BaseTransactionalBolt implements ITransactionalBolt {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }    
}
