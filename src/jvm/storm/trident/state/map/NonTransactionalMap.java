package storm.trident.state.map;

import java.util.ArrayList;
import java.util.List;
import storm.trident.state.ValueUpdater;


public class NonTransactionalMap<T> implements MapState<T> {
    public static <T> MapState<T> build(IBackingMap<T> backing) {
        return new NonTransactionalMap<T>(backing);
    }
    
    IBackingMap<T> _backing;
    
    protected NonTransactionalMap(IBackingMap<T> backing) {
        _backing = backing;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return _backing.multiGet(keys);
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        List<T> curr = _backing.multiGet(keys);
        List<T> ret = new ArrayList(curr.size());
        for(int i=0; i<curr.size(); i++) {
            T currVal = curr.get(i);
            ValueUpdater updater = updaters.get(i);
            ret.add((T) updater.update(currVal));
        }
        _backing.multiPut(keys, ret);
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _backing.multiPut(keys, vals);
    }

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txid) {
    }  
}
