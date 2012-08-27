package storm.trident.state.map;

import storm.trident.state.TransactionalValue;
import storm.trident.state.ValueUpdater;

import java.util.ArrayList;
import java.util.List;


public class TransactionalMap<T> implements MapState<T> {
    public static <T> MapState<T> build(IBackingMap<TransactionalValue> backing) {
        return new CachedBatchReadsMap<T>(new TransactionalMap<T>(backing));
    }
    
    IBackingMap<TransactionalValue> _backing;
    Long _currTx;
    
    protected TransactionalMap(IBackingMap<TransactionalValue> backing) {
        _backing = backing;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<TransactionalValue> vals = _backing.multiGet(keys);
        List<T> ret = new ArrayList<T>(vals.size());
        for(TransactionalValue v: vals) {
            if(v!=null) {
                ret.add((T) v.getVal());
            } else {
                ret.add(null);
            }
        }
        return ret;
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        List<TransactionalValue> curr = _backing.multiGet(keys);
        List<TransactionalValue> newVals = new ArrayList<TransactionalValue>(curr.size());
        List<T> ret = new ArrayList<T>();
        for(int i=0; i<curr.size(); i++) {
            TransactionalValue<T> val = curr.get(i);
            ValueUpdater<T> updater = updaters.get(i);
            TransactionalValue<T> newVal;
            if(val==null) {
                newVal = new TransactionalValue<T>(_currTx, updater.update(null));
            } else {
                if(_currTx!=null && _currTx.equals(val.getTxid())) {
                    newVal = val;
                } else {
                    newVal = new TransactionalValue<T>(_currTx, updater.update(val.getVal()));
                }    
            }
            ret.add(newVal.getVal());
            newVals.add(newVal);
        }
        _backing.multiPut(keys, newVals);
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<TransactionalValue> newVals = new ArrayList<TransactionalValue>(vals.size());
        for(T val: vals) {
            newVals.add(new TransactionalValue<T>(_currTx, val));
        }
        _backing.multiPut(keys, newVals);
    }

    @Override
    public void beginCommit(Long txid) {
        _currTx = txid;
    }

    @Override
    public void commit(Long txid) {
        _currTx = null;
    }  
}
