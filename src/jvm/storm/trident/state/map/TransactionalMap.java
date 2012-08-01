package storm.trident.state.map;

import java.util.ArrayList;
import java.util.List;
import storm.trident.state.TransactionalValue;
import storm.trident.state.ValueUpdater;


public class TransactionalMap<T> implements MapState<T> {
    public static MapState build(IBackingMap<TransactionalValue> backing) {
        return new CachedBatchReadsMap(new TransactionalMap(backing));
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
        List<TransactionalValue> newVals = new ArrayList(curr.size());
        List<T> ret = new ArrayList();
        for(int i=0; i<curr.size(); i++) {
            TransactionalValue val = curr.get(i);
            ValueUpdater updater = updaters.get(i);
            TransactionalValue newVal;
            if(val==null) {
                newVal = new TransactionalValue(_currTx, updater.update(null));
            } else {
                if(_currTx!=null && _currTx.equals(val.getTxid())) {
                    newVal = val;
                } else {
                    newVal = new TransactionalValue(_currTx, updater.update(val.getVal()));
                }    
            }
            ret.add((T)newVal.getVal());
            newVals.add(newVal);
        }
        _backing.multiPut(keys, newVals);
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<TransactionalValue> newVals = new ArrayList<TransactionalValue>(vals.size());
        for(T val: vals) {
            newVals.add(new TransactionalValue(_currTx, val));
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
