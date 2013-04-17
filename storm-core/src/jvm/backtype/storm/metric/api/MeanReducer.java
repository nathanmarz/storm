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
        if(input instanceof Double) {
            acc.sum += (Double)input;
        } else if(input instanceof Long) {
            acc.sum += ((Long)input).doubleValue();
        } else if(input instanceof Integer) {
            acc.sum += ((Integer)input).doubleValue();
        } else {
            throw new RuntimeException(
                "MeanReducer::reduce called with unsupported input type `" + input.getClass()
                + "`. Supported types are Double, Long, Integer.");
        }
        return acc;
    }

    public Object extractResult(MeanReducerState acc) {
        if(acc.count > 0) {
            return new Double(acc.sum / (double)acc.count);
        } else {
            return null;
        }
    }
}
