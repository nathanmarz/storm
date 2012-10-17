package backtype.storm.metric;

public class FixedValueMetric implements IMetric {
    Object _value;

    public FixedValueMetric(Object value) {
        _value = value;
    }

    public void setValue(Object value) {
        _value = value;
    }

    public Object getValueAndReset() {
        return _value;
    }
}
