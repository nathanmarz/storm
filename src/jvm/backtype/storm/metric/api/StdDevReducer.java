package backtype.storm.metric.api;

import backtype.storm.metric.api.IReducer;

class StdDevReducerState {
  public int count = 0;
  public double sum = 0.0;
  public double sumOfSquares = 0.0;
}

public class StdDevReducer implements IReducer<StdDevReducerState> {
  public StdDevReducerState init() {
    return new StdDevReducerState();
  }

  public StdDevReducerState reduce(StdDevReducerState acc, Object input) {
    acc.count++;
    double value;
    if(input instanceof Double) {
      value = (Double)input;
    } else if(input instanceof Long) {
      value = ((Long)input).doubleValue();
    } else if(input instanceof Integer) {
      value = ((Integer)input).doubleValue();
    } else {
      throw new RuntimeException(
          "StdDevReducer::reduce called with unsupported input type `" + input.getClass()
              + "`. Supported types are Double, Long, Integer.");
    }
    acc.sum += value;
    acc.sumOfSquares += Math.pow(value, 2);
    return acc;
  }

  public Object extractResult(StdDevReducerState acc) {
    if(acc.count > 0) {
      return (acc.sumOfSquares / acc.count) - Math.pow(acc.sum / (double)acc.count, 2);
    } else {
      return null;
    }
  }
}
