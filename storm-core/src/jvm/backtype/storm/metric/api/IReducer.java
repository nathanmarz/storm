package backtype.storm.metric.api;

public interface IReducer<T> {
    T init();
    T reduce(T accumulator, Object input);
    Object extractResult(T accumulator);
}
