package backtype.storm.topology.base;

import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import java.util.Map;

public abstract class BasePartitionedTransactionalSpout<T> implements IPartitionedTransactionalSpout<T> {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }  
}
