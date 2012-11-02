package backtype.storm.metric.api;

import backtype.storm.metric.api.IReducer;

class MeanReducerState {
    public int count = 0;
    public double sum = 0.0;
}

public class MeanReducer implements IReducer<MeanReducerState> {
    public MeanReducerState init() {
        return new MeanReducerState();
    }

    public MeanReducerState reduce(MeanReducerState acc, Object input) {
        acc.count++;
        acc.sum += (Double)input;
        return acc;
    }

    public Object extractResult(MeanReducerState acc) {
        return new Double(acc.sum / (double)acc.count);
    }
}
