package storm.trident.testing;

import backtype.storm.state.ITupleCollection;
import backtype.storm.tuple.Values;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;


public class MemoryMapState<T> implements IBackingMap<T>, ITupleCollection {
    public static class Factory implements StateFactory {
        String _id;
        
        public Factory() {
            _id = UUID.randomUUID().toString();
        }
        
        @Override
        public State makeState(Map conf, int partitionIndex, int numPartitions) {
            return new SnapshottableMap(OpaqueMap.build(new CachedMap(new MemoryMapState(_id), 10)), new Values("$MEMORY-MAP-STATE-GLOBAL$"));
        }        
    }
    
    public static void clearAll() {
        _dbs.clear();
    }
    
    static ConcurrentHashMap<String,  Map<List<Object>, Object>> _dbs = new ConcurrentHashMap<String, Map<List<Object>, Object>>();
    
    Map<List<Object>, T> db;
    Long currTx;
    
    public MemoryMapState(String id) {
        if(!_dbs.containsKey(id)) {
           _dbs.put(id, new HashMap()); 
        }
        this.db = (Map<List<Object>, T>) _dbs.get(id);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> ret = new ArrayList();
        for(List<Object> key: keys) {
            ret.add(db.get(key));
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        for(int i=0; i<keys.size(); i++) {
            List<Object> key = keys.get(i);
            T val = vals.get(i);
            db.put(key, val);
        }
    }    

    @Override
    public Iterator<List<Object>> getTuples() {
        return new Iterator<List<Object>>() {
            private Iterator<Map.Entry<List<Object>,T>> it = db.entrySet().iterator();

            public boolean hasNext() {
                return it.hasNext();
            }

            public List<Object> next() {
                Map.Entry<List<Object>,T> e = it.next();
                List<Object> ret = new ArrayList<Object>(e.getKey().size()+1);
                ret.addAll(e.getKey());
                ret.add(e.getValue());
                return ret;
            }

            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
}
