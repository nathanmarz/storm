package storm.trident.state.map;

import storm.trident.state.OpaqueValue;
import storm.trident.state.ValueUpdater;

import java.util.ArrayList;
import java.util.List;


public class OpaqueMap<T> implements MapState<T> {
    public static <T> MapState<T> build(IBackingMap<OpaqueValue> backing) {
        return new CachedBatchReadsMap<T>(new OpaqueMap<T>(backing));
    }
    
    IBackingMap<OpaqueValue> _backing;
    Long _currTx;
    
    protected OpaqueMap(IBackingMap<OpaqueValue> backing) {
        _backing = backing;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<OpaqueValue> curr = _backing.multiGet(keys);
        List<T> ret = new ArrayList<T>(curr.size());
        for(OpaqueValue val: curr) {
            if(val!=null) {
                ret.add((T) val.get(_currTx));
            } else {
                ret.add(null);
            }
        }
        return ret;
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        List<OpaqueValue> curr = _backing.multiGet(keys);
        List<OpaqueValue> newVals = new ArrayList<OpaqueValue>(curr.size());
        List<T> ret = new ArrayList<T>();
        for(int i=0; i<curr.size(); i++) {
            OpaqueValue<T> val = curr.get(i);
            ValueUpdater<T> updater = updaters.get(i);
            T prev;
            if(val==null) {
                prev = null;
            } else {
                prev = val.get(_currTx);
            }
            T newVal = updater.update(prev);
            ret.add(newVal);
            OpaqueValue<T> newOpaqueVal;
            if(val==null) {
                newOpaqueVal = new OpaqueValue<T>(_currTx, newVal);
            } else {
                newOpaqueVal = val.update(_currTx, newVal);
            }
            newVals.add(newOpaqueVal);
        }
        _backing.multiPut(keys, newVals);
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<ValueUpdater> updaters = new ArrayList<ValueUpdater>(vals.size());
        for(T val: vals) {
            updaters.add(new ReplaceUpdater<T>(val));
        }
        multiUpdate(keys, updaters);
    }

    @Override
    public void beginCommit(Long txid) {
        _currTx = txid;
    }

    @Override
    public void commit(Long txid) {
        _currTx = null;
    }
    
    static class ReplaceUpdater<T> implements ValueUpdater<T> {
        T _t;
        
        public ReplaceUpdater(T t) {
            _t = t;
        }
        
        @Override
        public T update(Object stored) {
            return _t;
        }        
    }    
}
