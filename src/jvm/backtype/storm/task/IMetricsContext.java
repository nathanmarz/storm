package backtype.storm.task;

import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.ICombiner;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.IReducer;
import backtype.storm.metric.api.ReducedMetric;


public interface IMetricsContext {
    <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs);
    ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs);
    CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs);  
}
