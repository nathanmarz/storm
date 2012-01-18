package backtype.storm.topology.base;

import backtype.storm.transactional.ITransactionalSpout;
import java.util.Map;

public abstract class BaseTransactionalSpout implements ITransactionalSpout {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
