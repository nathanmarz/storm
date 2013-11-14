package backtype.storm.metric.api;

public interface IMetric {
    public Object getValueAndReset();
}
