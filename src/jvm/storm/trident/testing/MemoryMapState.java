package storm.trident.testing;

import backtype.storm.state.ITupleCollection;
import backtype.storm.tuple.Values;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.*;
import storm.trident.state.snapshot.Snapshottable;

public class MemoryMapState<T> implements IBackingMap<T>, ITupleCollection {

    public static class Factory implements StateFactory {

        String _id;

        public Factory() {
            _id = UUID.randomUUID().toString();
        }

        static class SnapshottableMapAndTupleCollection<G> extends SnapshottableMap<G> implements ITupleCollection {
            MemoryMapState _memoryMapState;
            public SnapshottableMapAndTupleCollection(String id) {
                super(null, new Values("$MEMORY-MAP-STATE-GLOBAL$"));
                _memoryMapState = new MemoryMapState(id);
                setDelegate(OpaqueMap.build(_memoryMapState));
            }

            public Iterator<List<Object>> getTuples() {
                return _memoryMapState.getTuples();
            }
        }

        @Override
        public State makeState(Map conf, int partitionIndex, int numPartitions) {
            return new SnapshottableMapAndTupleCollection(_id);
        }
    }

    public static void clearAll() {
        _dbs.clear();
    }
    static ConcurrentHashMap<String, Map<List<Object>, Object>> _dbs = new ConcurrentHashMap<String, Map<List<Object>, Object>>();
    Map<List<Object>, T> db;
    Long currTx;

    public MemoryMapState(String id) {
        if (!_dbs.containsKey(id)) {
            _dbs.put(id, new HashMap());
        }
        this.db = (Map<List<Object>, T>) _dbs.get(id);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> ret = new ArrayList();
        for (List<Object> key : keys) {
            ret.add(db.get(key));
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
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
                ret.add(((OpaqueValue)e.getValue()).getCurr());
                return ret;
            }

            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
}
