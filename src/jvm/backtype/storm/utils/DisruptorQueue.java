package backtype.storm.utils;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.WaitStrategy;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is
 * the ability to catch up to the producer by processing tuples in batches.
 */
public class DisruptorQueue {
    static final Object FLUSH_CACHE = new Object();
    
    RingBuffer<MutableObject> _buffer;
    Sequence _consumer;
    SequenceBarrier _barrier;
    
    // TODO: consider having a threadlocal cache of this variable to speed up reads?
    volatile boolean consumerStartedFlag = false;
    ConcurrentLinkedQueue<Object> _cache = new ConcurrentLinkedQueue();
    
    public DisruptorQueue(ClaimStrategy claim, WaitStrategy wait) {
        _buffer = new RingBuffer<MutableObject>(new ObjectEventFactory(), claim, wait);
        _consumer = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        _buffer.setGatingSequences(_consumer);
        _barrier = _buffer.newBarrier(_consumer);
    }
    
    public void consumeBatch(EventHandler<Object> handler) {
        consumeBatchToCursor(_barrier.getCursor(), handler);
    }
    
    public void consumeBatchWhenAvailable(EventHandler<Object> handler) {
        try {
            final long availableSequence = _barrier.waitFor(_consumer.get() + 1);
            consumeBatchToCursor(availableSequence, handler);
        } catch (AlertException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    private void consumeBatchToCursor(long cursor, EventHandler<Object> handler) {
        for(long curr = _consumer.get() + 1; curr <= cursor; curr++) {
            try {
                Object o = _buffer.get(curr).o;
                if(o==FLUSH_CACHE) {
                    Object c = null;
                    while(true) {                        
                        c = _cache.poll();
                        if(c==null) break;
                        else handler.onEvent(c, curr, true);
                    }
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
        if(consumerStartedFlag) {
            final long id = _buffer.next();
            final MutableObject m = _buffer.get(id);
            m.setObject(obj);
            _buffer.publish(id);
        } else {
            _cache.add(obj);
            if(consumerStartedFlag) flushCache();
        }
    }
    
    public void consumerStarted() {
        consumerStartedFlag = true;
        flushCache();
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
