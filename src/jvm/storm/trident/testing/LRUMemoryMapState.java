package storm.trident.testing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import storm.trident.state.ITupleCollection;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.util.LRUMap;
import backtype.storm.tuple.Values;

public class LRUMemoryMapState<T> implements Snapshottable<T>, ITupleCollection, MapState<T> {

    LRUMemoryMapStateBacking<OpaqueValue<T>> _backing;
    SnapshottableMap<T> _delegate;

    public LRUMemoryMapState(int cacheSize, String id) {
        _backing = new LRUMemoryMapStateBacking<OpaqueValue<T>>(cacheSize, id);
        _delegate = new SnapshottableMap<T>(OpaqueMap.build(_backing), new Values("$MEMORY-MAP-STATE-GLOBAL$"));
    }

    @Override
    public T update(ValueUpdater<T> updater) {
        return _delegate.update(updater);
    }

    public void set(T o) {
        _delegate.set(o);
    }

    public T get() {
        return _delegate.get();
    }

    public void beginCommit(Long txid) {
        _delegate.beginCommit(txid);
    }

    public void commit(Long txid) {
        _delegate.commit(txid);
    }

    public Iterator<List<Object>> getTuples() {
        return _backing.getTuples();
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater<T>> updaters) {
        return _delegate.multiUpdate(keys, updaters);
    }

    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _delegate.multiPut(keys, vals);
    }

    public List<T> multiGet(List<? extends List<Object>> keys) {
        return _delegate.multiGet(keys);
    }

    public static class Factory<T> implements StateFactory {

        String _id;
        int _maxSize;

        public Factory(int maxSize) {
            _id = UUID.randomUUID().toString();
            _maxSize = maxSize;
        }

        @Override
        public State makeState(Map conf, int partitionIndex, int numPartitions) {
            return new LRUMemoryMapState<T>(_maxSize, _id);
        }
    }

    static ConcurrentHashMap<String, Map<? extends List<Object>, Object>> _dbs = new ConcurrentHashMap<String, Map<? extends List<Object>, Object>>();
    static class LRUMemoryMapStateBacking<T> implements IBackingMap<T>, ITupleCollection {

        public static void clearAll() {
            _dbs.clear();
        }
        Map<List<Object>, T> db;
        Long currTx;

        public LRUMemoryMapStateBacking(int cacheSize, String id) {
            if (!_dbs.containsKey(id)) {
                _dbs.put(id, new LRUMap<List<Object>, Object>(cacheSize));
            }
            this.db = (Map<List<Object>, T>) _dbs.get(id);
        }

        @Override
        public List<T> multiGet(List<? extends List<Object>> keys) {
            List<T> ret = new ArrayList<T>();
            for (List<Object> key : keys) {
                ret.add(db.get(key));
            }
            return ret;
        }

        @Override
        public void multiPut(List<? extends List<Object>> keys, List<T> vals) {
            for (int i = 0; i < keys.size(); i++) {
                List<Object> key = keys.get(i);
                T val = vals.get(i);
                db.put(key, val);
            }
        }

        @Override
        public Iterator<List<Object>> getTuples() {
            return new Iterator<List<Object>>() {

                private Iterator<Map.Entry<List<Object>, T>> it = db.entrySet().iterator();

                public boolean hasNext() {
                    return it.hasNext();
                }

                public List<Object> next() {
                    Map.Entry<List<Object>, T> e = it.next();
                    List<Object> ret = new ArrayList<Object>();
                    ret.addAll(e.getKey());
                    ret.add(OpaqueValue.class.cast(e.getValue()).getCurr());
                    return ret;
                }

                public void remove() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }
    }
}
