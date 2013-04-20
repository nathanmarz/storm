package backtype.storm.utils;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is
 * the ability to catch up to the producer by processing tuples in batches.
 */
public class DisruptorQueue {
    static final Object FLUSH_CACHE = new Object();
    static final Object INTERRUPT = new Object();
    
    RingBuffer<MutableObject> _buffer;
    Sequence _consumer;
    SequenceBarrier _barrier;
    
    // TODO: consider having a threadlocal cache of this variable to speed up reads?
    volatile boolean consumerStartedFlag = false;
    ConcurrentLinkedQueue<Object> _cache = new ConcurrentLinkedQueue();
    
    public DisruptorQueue(ClaimStrategy claim, WaitStrategy wait) {
        _buffer = new RingBuffer<MutableObject>(new ObjectEventFactory(), claim, wait);
        _consumer = new Sequence();
        _barrier = _buffer.newBarrier();
        _buffer.setGatingSequences(_consumer);
        if(claim instanceof SingleThreadedClaimStrategy) {
            consumerStartedFlag = true;
        }
    }
    
    public void consumeBatch(EventHandler<Object> handler) {
        consumeBatchToCursor(_barrier.getCursor(), handler);
    }
    
    public void haltWithInterrupt() {
        publish(INTERRUPT);
    }
    
    public void consumeBatchWhenAvailable(EventHandler<Object> handler) {
        try {
            final long nextSequence = _consumer.get() + 1;
            final long availableSequence = _barrier.waitFor(nextSequence, 10, TimeUnit.MILLISECONDS);
            if(availableSequence >= nextSequence) {
                consumeBatchToCursor(availableSequence, handler);
            }
        } catch (AlertException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    private void consumeBatchToCursor(long cursor, EventHandler<Object> handler) {
        for(long curr = _consumer.get() + 1; curr <= cursor; curr++) {
            try {
                MutableObject mo = _buffer.get(curr);
                Object o = mo.o;
                mo.setObject(null);
                if(o==FLUSH_CACHE) {
                    Object c = null;
                    while(true) {                        
                        c = _cache.poll();
                        if(c==null) break;
                        else handler.onEvent(c, curr, true);
                    }
                } else if(o==INTERRUPT) {
                    throw new InterruptedException("Disruptor processing interrupted");
                } else {
                    handler.onEvent(o, curr, curr == cursor);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        //TODO: only set this if the consumer cursor has changed?
        _consumer.set(cursor);
    }
    
    /*
     * Caches until consumerStarted is called, upon which the cache is flushed to the consumer
     */
    public void publish(Object obj) {
        try {
            publish(obj, true);
        } catch (InsufficientCapacityException ex) {
            throw new RuntimeException("This code should be unreachable!");
        }
    }
    
    public void tryPublish(Object obj) throws InsufficientCapacityException {
        publish(obj, false);
    }
    
    public void publish(Object obj, boolean block) throws InsufficientCapacityException {
        if(consumerStartedFlag) {
            final long id;
            if(block) {
                id = _buffer.next();
            } else {
                id = _buffer.tryNext(1);
            }
            final MutableObject m = _buffer.get(id);
            m.setObject(obj);
            _buffer.publish(id);
        } else {
            _cache.add(obj);
            if(consumerStartedFlag) flushCache();
        }
    }
    
    public void consumerStarted() {
        if(!consumerStartedFlag) {
            consumerStartedFlag = true;
            flushCache();
        }
    }
    
    private void flushCache() {
        publish(FLUSH_CACHE);
    }

    public static class ObjectEventFactory implements EventFactory<MutableObject> {
        @Override
        public MutableObject newInstance() {
            return new MutableObject();
        }        
    }
}
