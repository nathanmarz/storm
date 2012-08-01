package storm.trident.state.map;

import java.util.ArrayList;
import java.util.List;
import storm.trident.state.OpaqueValue;
import storm.trident.state.ValueUpdater;


public class OpaqueMap<T> implements MapState<T> {
    public static MapState build(IBackingMap<OpaqueValue> backing) {
        return new CachedBatchReadsMap(new OpaqueMap(backing));
    }
    
    IBackingMap<OpaqueValue> _backing;
    Long _currTx;
    
    protected OpaqueMap(IBackingMap<OpaqueValue> backing) {
        _backing = backing;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<OpaqueValue> curr = _backing.multiGet(keys);
        List<T> ret = new ArrayList(curr.size());
        for(OpaqueValue val: curr) {
            if(val!=null) {
                ret.add((T)val.get(_currTx));                
            } else {
                ret.add(null);
            }
        }
        return ret;
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        List<OpaqueValue> curr = _backing.multiGet(keys);
        List<OpaqueValue> newVals = new ArrayList(curr.size());
        List<T> ret = new ArrayList();
        for(int i=0; i<curr.size(); i++) {
            OpaqueValue val = curr.get(i);
            ValueUpdater updater = updaters.get(i);
            Object prev;
            if(val==null) {
                prev = null;
            } else {
                prev = val.get(_currTx);
            }
            Object newVal = updater.update(prev);
            ret.add((T)newVal);
            OpaqueValue newOpaqueVal;
            if(val==null) {
                newOpaqueVal = new OpaqueValue(_currTx, newVal);
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
        List<ValueUpdater> updaters = new ArrayList(vals.size());
        for(T val: vals) {
            updaters.add(new ReplaceUpdater(val));
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
    
    static class ReplaceUpdater implements ValueUpdater {
        Object _o;
        
        public ReplaceUpdater(Object o) {
            _o = o;
        }
        
        @Override
        public Object update(Object stored) {
            return _o;
        }        
    }    
}
