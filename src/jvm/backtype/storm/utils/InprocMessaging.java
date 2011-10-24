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
        LinkedBlockingQueue<Object> queue = getQueue(port);
        synchronized(_lock) {
            queue.add(msg);
        }
    }
    
    public static Object takeMessage(int port) throws InterruptedException {
        LinkedBlockingQueue<Object> queue = getQueue(port);
        Object ret = queue.take();
        synchronized(_lock) {
            if(queue.size()==0) {
                _queues.remove(port);
            }
        }
        return ret;
    }

    public static Object pollMessage(int port) {
        LinkedBlockingQueue<Object> queue = getQueue(port);
        Object ret = queue.poll();
        synchronized(_lock) {
            if(queue.size()==0) {
                _queues.remove(port);
            }
        }
        return ret;
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
