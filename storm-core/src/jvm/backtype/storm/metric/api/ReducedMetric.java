package backtype.storm.metric.api;

public class ReducedMetric implements IMetric {
    private final IReducer _reducer;
    private Object _accumulator;

    public ReducedMetric(IReducer reducer) {
        _reducer = reducer;
        _accumulator = _reducer.init();
    }

    public void update(Object value) {
        _accumulator = _reducer.reduce(_accumulator, value);
    }

    public Object getValueAndReset() {
        Object ret = _reducer.extractResult(_accumulator);
        _accumulator = _reducer.init();
        return ret;
    }
}
