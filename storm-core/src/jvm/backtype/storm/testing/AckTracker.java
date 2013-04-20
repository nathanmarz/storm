package backtype.storm.testing;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AckTracker implements AckFailDelegate {
    private static Map<String, AtomicInteger> acks = new ConcurrentHashMap<String, AtomicInteger>();
    
    private String _id;
    
    public AckTracker() {
        _id = UUID.randomUUID().toString();
        acks.put(_id, new AtomicInteger(0));
    }
    
    @Override
    public void ack(Object id) {
        acks.get(_id).incrementAndGet();
    }

    @Override
    public void fail(Object id) {
    }
    
    public int getNumAcks() {
        return acks.get(_id).intValue();
    }
    
    public void resetNumAcks() {
        acks.get(_id).set(0);
    }
    
}
