package backtype.storm.topology.base;

import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import java.util.Map;

public abstract class BasePartitionedTransactionalSpout<T> extends BaseComponent implements IPartitionedTransactionalSpout<T> {

}
