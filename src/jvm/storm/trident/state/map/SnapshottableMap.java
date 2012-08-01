package storm.trident.state.map;

import java.util.Arrays;
import java.util.List;
import storm.trident.state.ValueUpdater;
import storm.trident.state.snapshot.Snapshottable;


public class SnapshottableMap<T> implements MapState<T>, Snapshottable<T> {
    MapState<T> _delegate;
    List<List<Object>> _keys;
    
    public SnapshottableMap(MapState<T> delegate, List<Object> snapshotKey) {
        _delegate = delegate;
        _keys = Arrays.asList(snapshotKey);
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return _delegate.multiGet(keys);
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        return _delegate.multiUpdate(keys, updaters);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _delegate.multiPut(keys, vals);
    }

    @Override
    public void beginCommit(Long txid) {
        _delegate.beginCommit(txid);
    }

    @Override
    public void commit(Long txid) {
        _delegate.commit(txid);
    }

    @Override
    public T get() {
        return multiGet(_keys).get(0);
    }

    @Override
    public T update(ValueUpdater updater) {
        List<ValueUpdater> updaters = Arrays.asList(updater);
        return multiUpdate(_keys, updaters).get(0);
    }

    @Override
    public void set(T o) {
        multiPut(_keys, Arrays.asList(o));
    }
    
}
