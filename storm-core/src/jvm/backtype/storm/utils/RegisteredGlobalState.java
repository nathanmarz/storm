package backtype.storm.utils;

import java.util.HashMap;
import java.util.UUID;

/**
 * This class is used as part of testing Storm. It is used to keep track of "global metrics"
 * in an atomic way. For example, it is used for doing fine-grained detection of when a 
 * local Storm cluster is idle by tracking the number of transferred tuples vs the number of processed
 * tuples.
 */
public class RegisteredGlobalState {
    private static HashMap<String, Object> _states = new HashMap<String, Object>();
    private static final Object _lock = new Object();
    
    public static Object globalLock() {
        return _lock;
    }
    
    public static String registerState(Object init) {
        synchronized(_lock) {
            String id = UUID.randomUUID().toString();
            _states.put(id, init);
            return id;
        }
    }
    
    public static void setState(String id, Object init) {
        synchronized(_lock) {
            _states.put(id, init);
        }
    }
    
    public static Object getState(String id) {
        synchronized(_lock) {
            Object ret = _states.get(id);
            //System.out.println("State: " + ret.toString());
            return ret;
        }        
    }
    
    public static void clearState(String id) {
        synchronized(_lock) {
            _states.remove(id);
        }        
    }
}
