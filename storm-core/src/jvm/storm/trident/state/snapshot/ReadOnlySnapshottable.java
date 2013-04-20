package storm.trident.state.snapshot;

import storm.trident.state.State;

public interface ReadOnlySnapshottable<T> extends State {
    T get();    
}
