package backtype.storm.metric.api;

import backtype.storm.metric.api.IMetric;

public class CountMetric implements IMetric, java.io.Serializable {
    long _value = 0;

    public CountMetric() {
    }
    
    public void incr() {
        _value++;
    }

    public void incrBy(long incrementBy) {
        _value += incrementBy;
    }

    public Object getValueAndReset() {
        long ret = _value;
        _value = 0;
        return ret;
    }
}
