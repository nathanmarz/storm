package backtype.storm.metric;

public class MetricHolder {
    private String _name;
    private int _timeBucketIntervalInSecs;
    private IMetric _metric;

    public MetricHolder(String name, IMetric metric, int timeBucketIntervalInSecs) {
        _name = name;
        _timeBucketIntervalInSecs = timeBucketIntervalInSecs;
        _metric = metric;
    }

    public String getName() { return _name; }
    public int getTimeBucketIntervalInSecs() { return _timeBucketIntervalInSecs; }
    public IMetric getMetric() { return _metric; }
}
