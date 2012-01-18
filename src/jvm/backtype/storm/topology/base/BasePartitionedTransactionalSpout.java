package backtype.storm.topology.base;

import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import java.util.Map;

public abstract class BasePartitionedTransactionalSpout implements IPartitionedTransactionalSpout {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }  
}
