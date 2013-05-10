package storm.trident.topology.state;

import backtype.storm.utils.Utils;
import org.apache.zookeeper.KeeperException;

import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class RotatingTransactionalState {
    public static interface StateInitializer {
        Object init(long txid, Object lastState);
    }    

    private TransactionalState _state;
    private String _subdir;
    
    private TreeMap<Long, Object> _curr = new TreeMap<Long, Object>();
    
    public RotatingTransactionalState(TransactionalState state, String subdir) {
        _state = state;
        _subdir = subdir;
        state.mkdir(subdir);
        sync();
    }


    public Object getLastState() {
        if(_curr.isEmpty()) return null;
        else return _curr.lastEntry().getValue();
    }
    
    public void overrideState(long txid, Object state) {
        _state.setData(txPath(txid), state);
        _curr.put(txid, state);
    }

    public void removeState(long txid) {
        if(_curr.containsKey(txid)) {
            _curr.remove(txid);
            _state.delete(txPath(txid));
        }
    }
    
    public Object getState(long txid) {
        return _curr.get(txid);
    }
    
    public Object getState(long txid, StateInitializer init) {
        if(!_curr.containsKey(txid)) {
            SortedMap<Long, Object> prevMap = _curr.headMap(txid);
            SortedMap<Long, Object> afterMap = _curr.tailMap(txid);            
            
            Long prev = null;
            if(!prevMap.isEmpty()) prev = prevMap.lastKey();            
            
            Object data;
            if(afterMap.isEmpty()) {
                Object prevData;
                if(prev!=null) {
                    prevData = _curr.get(prev);
                } else {
                    prevData = null;
                }
                data = init.init(txid, prevData);
            } else {
                data = null;
            }
            _curr.put(txid, data);
            _state.setData(txPath(txid), data);
        }
        return _curr.get(txid);
    }
    
    public Object getPreviousState(long txid) {
        SortedMap<Long, Object> prevMap = _curr.headMap(txid);
        if(prevMap.isEmpty()) return null;
        else return prevMap.get(prevMap.lastKey());
    }
    
    public boolean hasCache(long txid) {
        return _curr.containsKey(txid);
    }
       
    /**
     * Returns null if it was created, the value otherwise.
     */
    public Object getStateOrCreate(long txid, StateInitializer init) {
        if(_curr.containsKey(txid)) {
            return _curr.get(txid);
        } else {
            getState(txid, init);
            return null;
        }
    }
    
    public void cleanupBefore(long txid) {
        SortedMap<Long, Object> toDelete = _curr.headMap(txid);
        for(long tx: new HashSet<Long>(toDelete.keySet())) {
            _curr.remove(tx);
            try {
                _state.delete(txPath(tx));
            } catch(RuntimeException e) {
                // Ignore NoNodeExists exceptions because when sync() it may populate _curr with stale data since
                // zookeeper reads are eventually consistent.
                if(!Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                    throw e;
                }
            }
        }
    }
    
    private void sync() {
        List<String> txids = _state.list(_subdir);
        for(String txid_s: txids) {
            Object data = _state.getData(txPath(txid_s));
            _curr.put(Long.parseLong(txid_s), data);
        }
    }
    
    private String txPath(long tx) {
        return txPath("" + tx);
    }

    private String txPath(String tx) {
        return _subdir + "/" + tx;
    }    
    
}
