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
    LRUMap<List<Object>, T> _cache;
    IBackingMap<T> _delegate;

    public CachedMap(IBackingMap<T> delegate, int cacheSize) {
        _cache = new LRUMap<List<Object>, T>(cacheSize);
        _delegate = delegate;
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        Map<List<Object>, T> results = new HashMap<List<Object>, T>();
        List<List<Object>> toGet = new ArrayList<List<Object>>();
        for(List<Object> key: keys) {
            if(_cache.containsKey(key)) {
                results.put(key, _cache.get(key));
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

        List<T> ret = new ArrayList<T>(keys.size());
        for(List<Object> key: keys) {
            ret.add(results.get(key));
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        cache(keys, values);
        _delegate.multiPut(keys, values);
    }

    private void cache(List<List<Object>> keys, List<T> values) {
        for(int i=0; i<keys.size(); i++) {
            _cache.put(keys.get(i), values.get(i));
        }
    }

}
