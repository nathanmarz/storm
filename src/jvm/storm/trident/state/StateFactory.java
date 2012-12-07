package storm.trident.state;

import backtype.storm.task.IMetricsContext;
import java.io.Serializable;
import java.util.Map;

public interface StateFactory extends Serializable {
    State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions);
}
