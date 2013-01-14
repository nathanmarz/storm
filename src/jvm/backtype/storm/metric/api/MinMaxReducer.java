package backtype.storm.metric.api;

import backtype.storm.metric.api.IReducer;

class MinMaxReducerState {
  private final double value;
  private boolean isValueSet;

  public MinMaxReducerState() {
    this.isValueSet = false;
    this.value = Double.NaN;
  }
  public MinMaxReducerState(double value) {
    this.isValueSet = true;
    this.value = value;
  }

  public Double getValue() {
    if (isValueSet) {
      return value;
    }
    return null;
  }

  public boolean hasValue() {
    return isValueSet;
  }
}

public class MinMaxReducer implements IReducer<MinMaxReducerState> {

  public enum MinMaxReducerType {
    MIN,
    MAX,
  }

  private final MinMaxReducerType type;

  public MinMaxReducer(MinMaxReducerType type) {
    this.type = type;
  }

  @Override
  public MinMaxReducerState init() {
    return new MinMaxReducerState();
  }

  @Override
  public MinMaxReducerState reduce(MinMaxReducerState acc, Object input) {
    double value;
    if(input instanceof Double) {
      value = (Double)input;
    } else if(input instanceof Long) {
      value = ((Long)input).doubleValue();
    } else if(input instanceof Integer) {
      value = ((Integer)input).doubleValue();
    } else {
      throw new RuntimeException(
          "MinMaxReducer::reduce called with unsupported input type `" + input.getClass()
              + "`. Supported types are Double, Long, Integer.");
    }
    if (acc.hasValue()
        && ((type == MinMaxReducerType.MAX && acc.getValue() >= value)
        || (type == MinMaxReducerType.MIN && acc.getValue() <= value))) {
      return acc;
    } else {
      return new MinMaxReducerState(value);
    }
  }

  @Override
  public Object extractResult(MinMaxReducerState acc) {
    return acc.getValue();
  }
}