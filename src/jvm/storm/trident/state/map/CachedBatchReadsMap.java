package storm.trident.state.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.trident.state.ValueUpdater;

public class CachedBatchReadsMap<T> implements MapState<T> {
    Map<List<Object>, T> _cached = new HashMap<List<Object>, T>();
    
    public MapState<T> _delegate;
    
    public CachedBatchReadsMap(MapState<T> delegate) {
        _delegate = delegate;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> ret = _delegate.multiGet(keys);
        if(!_cached.isEmpty()) {
            ret = new ArrayList<T>(ret);
            for(int i=0; i<keys.size(); i++) {
                List<Object> key = keys.get(i);
                if(_cached.containsKey(key)) {
                    ret.set(i, _cached.get(key));
                }
            }
        }
        return ret;
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        List<T> vals = _delegate.multiUpdate(keys, updaters);
        cache(keys, vals);
        return vals;
    }

    @Override
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

    @Override
    public void beginCommit(Long txid) {
        _cached.clear(); //if a commit was pending and failed, we need to make sure to clear the cache
        _delegate.beginCommit(txid);
    }

    @Override
    public void commit(Long txid) {
        _cached.clear();
        _delegate.commit(txid);
    }
    
}
