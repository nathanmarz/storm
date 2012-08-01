package storm.trident.state.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.trident.util.LRUMap;

/**
 * Useful to layer over a map that communicates with a database. you generally layer opaque map over this over your database store
 * @author nathan
 * @param <T> 
 */
public class CachedMap<T> implements IBackingMap<T> {
    LRUMap _cache;
    IBackingMap _delegate;
    
    public CachedMap(IBackingMap delegate, int cacheSize) {
        _cache = new LRUMap(cacheSize);
        _delegate = delegate;
    }    
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        Map<List<Object>, T> results = new HashMap();
        List<List<Object>> toGet = new ArrayList();
        for(List<Object> key: keys) {
            if(_cache.containsKey(key)) {
                results.put(key, (T) _cache.get(key));                
            } else {
                toGet.add(key);
            }
        }

        List<T> fetchedVals = _delegate.multiGet(toGet);
        for(int i=0; i<toGet.size(); i++) {
            List<Object> key = toGet.get(i);
            T val = fetchedVals.get(i);
            _cache.put(key, val);
            results.put(key, val);
        }
        
        List<T> ret = new ArrayList(keys.size());
        for(List<Object> key: keys) {
            ret.add(results.get(key));
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        cache(keys, vals);
        _delegate.multiPut(keys, vals);
    }
    
    private void cache(List<List<Object>> keys, List<T> vals) {
        for(int i=0; i<keys.size(); i++) {
            _cache.put(keys.get(i), vals.get(i));
        }
    }

}
