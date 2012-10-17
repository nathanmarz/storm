package backtype.storm.metric;

public class IncrementedMetric implements IMetric {
    long _value = 0;

    public IncrementedMetric() {
    }
    
    public void inc() {
        _value++;
    }

    public void inc(long incrementBy) {
        _value += incrementBy;
    }

    public Object getValueAndReset() {
        long ret = _value;
        _value = 0;
        return ret;
    }
}
