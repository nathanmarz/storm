package backtype.storm.metric;

import backtype.storm.metric.api.IMetric;

public class MetricHolder {
    public String name;
    public IMetric metric;

    public MetricHolder(String name, IMetric metric) {
        this.name = name;
        this.metric = metric;
    }
}
