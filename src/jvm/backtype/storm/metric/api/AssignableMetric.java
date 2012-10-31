package backtype.storm.metric.api;

public class AssignableMetric implements IMetric {
    Object _value;

    public AssignableMetric(Object value) {
        _value = value;
    }

    public void setValue(Object value) {
        _value = value;
    }

    public Object getValueAndReset() {
        return _value;
    }
}
