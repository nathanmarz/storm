package backtype.storm.metric;

public interface IReducer<T> {
    T init();
    T reduce(T accumulator, Object input);
    Object extractResult(T accumulator);
}
