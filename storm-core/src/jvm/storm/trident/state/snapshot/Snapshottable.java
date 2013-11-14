package storm.trident.state.snapshot;

import storm.trident.state.ValueUpdater;


// used by Stream#persistentAggregate
public interface Snapshottable<T> extends ReadOnlySnapshottable<T> {
    T update(ValueUpdater updater);
    void set(T o);
}
