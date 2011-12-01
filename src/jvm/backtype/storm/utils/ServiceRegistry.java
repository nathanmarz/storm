package backtype.storm.utils;

import java.util.HashMap;
import java.util.UUID;

// this class should be combined with RegisteredGlobalState
public class ServiceRegistry {
    private static HashMap<String, Object> _services = new HashMap<String, Object>();
    private static final Object _lock = new Object();
    
    public static String registerService(Object service) {
        synchronized(_lock) {
            String id = UUID.randomUUID().toString();
            _services.put(id, service);
            return id;
        }
    }
    
    public static Object getService(String id) {
        synchronized(_lock) {
            return _services.get(id);
        }        
    }
    
    public static void unregisterService(String id) {
        synchronized(_lock) {
            _services.remove(id);
        }        
    }
}
