package backtype.storm.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class InprocMessaging {
    private static Map<Integer, LinkedBlockingQueue<Object>> _queues = new HashMap<Integer, LinkedBlockingQueue<Object>>();
    private static final Object _lock = new Object();
    private static int port = 1;
    
    public static int acquireNewPort() {
        int ret;
        synchronized(_lock) {
            ret = port;
            port++;
        }
        return ret;
    }
    
    public static void sendMessage(int port, Object msg) {
        getQueue(port).add(msg);
    }
    
    public static Object takeMessage(int port) throws InterruptedException {
        return getQueue(port).take();
    }

    public static Object pollMessage(int port) {
        return  getQueue(port).poll();
    }    
    
    private static LinkedBlockingQueue<Object> getQueue(int port) {
        synchronized(_lock) {
            if(!_queues.containsKey(port)) {
              _queues.put(port, new LinkedBlockingQueue<Object>());   
            }
            return _queues.get(port);
        }
    }

}
