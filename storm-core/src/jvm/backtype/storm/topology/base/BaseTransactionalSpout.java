package backtype.storm.topology.base;

import backtype.storm.transactional.ITransactionalSpout;
import java.util.Map;

public abstract class BaseTransactionalSpout<T> extends BaseComponent implements ITransactionalSpout<T> {

}
