package backtype.storm.utils;

import java.util.concurrent.ConcurrentLinkedQueue;


public class ThreadResourceManager<T> {
    public static interface ResourceFactory<X> {
        X makeResource();
    }
    
    ResourceFactory<T> _factory;
    ConcurrentLinkedQueue<T> _resources = new ConcurrentLinkedQueue<T>();
    
    public ThreadResourceManager(ResourceFactory<T> factory) {
        _factory = factory;
    }
    
    public T acquire() {
        T ret = _resources.poll();
        if(ret==null) {
            ret = _factory.makeResource();
        }
        return ret;
    }
    
    public void release(T resource) {
        _resources.add(resource);
    }
}
