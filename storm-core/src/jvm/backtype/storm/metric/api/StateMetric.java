package backtype.storm.metric.api;

public class StateMetric implements IMetric {
    private IStatefulObject _obj;

    public StateMetric(IStatefulObject obj) {
        _obj = obj;
    }

    @Override
    public Object getValueAndReset() {
        return _obj.getState();
    }
}
