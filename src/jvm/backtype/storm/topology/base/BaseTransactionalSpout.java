package backtype.storm.topology.base;

import backtype.storm.transactional.ITransactionalSpout;
import java.util.Map;

public abstract class BaseTransactionalSpout implements ITransactionalSpout {
    String _id;
    
    public BaseTransactionalSpout(String id) {
        _id = id;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public String getId() {
        return _id;
    }    
}
