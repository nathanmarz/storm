package backtype.storm.metric.api.rpc;

import backtype.storm.metric.api.AssignableMetric;

public class AssignableShellMetric extends AssignableMetric implements IShellMetric {
    public AssignableShellMetric(Object value) {
        super(value);
    }

    public void updateMetricFromRPC(Object value) {
        setValue(value);
    }
}
