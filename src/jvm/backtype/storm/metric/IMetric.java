package backtype.storm.metric;

public interface IMetric {
    public Object getValueAndReset();
}
