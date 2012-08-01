package storm.trident.state;


public interface ValueUpdater<T> {
    T update(T stored);
}
