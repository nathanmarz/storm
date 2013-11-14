package storm.trident.state.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.trident.state.ValueUpdater;


public class CachedBatchReadsMap<T> {
    public static class RetVal<T> {
        public boolean cached;
        public T val;

        public RetVal(T v, boolean c) {
            val = v;
            cached = c;
        }
    }

    Map<List<Object>, T> _cached = new HashMap<List<Object>, T>();
    
    public IBackingMap<T> _delegate;
    
    public CachedBatchReadsMap(IBackingMap<T> delegate) {
        _delegate = delegate;
    }

    public void reset() {
        _cached.clear();
    }
    
    public List<RetVal<T>> multiGet(List<List<Object>> keys) {
        // TODO: can optimize further by only querying backing map for keys not in the cache
        List<T> vals = _delegate.multiGet(keys);
        List<RetVal<T>> ret = new ArrayList(vals.size());
        for(int i=0; i<keys.size(); i++) {
            List<Object> key = keys.get(i);
            if(_cached.containsKey(key)) {
                ret.add(new RetVal(_cached.get(key), true));
            } else {
                ret.add(new RetVal(vals.get(i), false));
            }
        }
        return ret;
    }

    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _delegate.multiPut(keys, vals);
        cache(keys, vals);
    }
    
    private void cache(List<List<Object>> keys, List<T> vals) {
        for(int i=0; i<keys.size(); i++) {
            List<Object> key = keys.get(i);
            T val = vals.get(i);
            _cached.put(key, val);
        }
    }


    
}
