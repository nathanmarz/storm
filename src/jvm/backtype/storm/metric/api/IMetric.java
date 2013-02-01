package backtype.storm.metric.api;

import java.io.Serializable;

public interface IMetric extends Serializable {
    public Object getValueAndReset();
}
