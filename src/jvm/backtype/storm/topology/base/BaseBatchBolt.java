package backtype.storm.topology.base;

import backtype.storm.transactional.IBatchBolt;
import java.util.Map;

public abstract class BaseBatchBolt implements IBatchBolt {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }    
}
