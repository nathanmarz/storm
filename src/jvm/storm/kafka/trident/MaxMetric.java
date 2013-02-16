package storm.kafka.trident;


import backtype.storm.metric.api.ICombiner;
import clojure.lang.Numbers;

public class MaxMetric implements ICombiner<Long> {
    @Override
    public Long identity() {
        return null;
    }

    @Override
    public Long combine(Long l1, Long l2) {
        return Math.max(l1, l2);
    }

}
