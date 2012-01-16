package backtype.storm.topology.base;

import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import java.util.Map;

public abstract class BasePartitionedTransactionalSpout implements IPartitionedTransactionalSpout {
    String _id;
    
    public BasePartitionedTransactionalSpout(String id) {
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
