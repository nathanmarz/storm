package backtype.storm.utils;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;

/**
 *
 * A single producer / single consumer queue that doesn't block on reads.
 */
public class NonBlockingPairDisruptorQueue {
    RingBuffer<MutableObject> _buffer;
    Sequence _consumer;
    SequenceBarrier _barrier;
    
    public NonBlockingPairDisruptorQueue(Number bufferSize) {
        _buffer = new RingBuffer<MutableObject>(new DisruptorQueue.ObjectEventFactory(),
                        new SingleThreadedClaimStrategy(bufferSize.intValue()),
                        new BlockingWaitStrategy());
        _consumer = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        _buffer.setGatingSequences(_consumer);
        _barrier = _buffer.newBarrier(_consumer);
    }
    
    public void consumeBatch(EventHandler<Object> handler) {
        long cursor = _barrier.getCursor();
        for(long curr = _consumer.get() + 1; curr <= cursor; curr++) {
            try {
                handler.onEvent(_buffer.get(curr).o, curr, curr == cursor);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        _consumer.set(cursor);
    }
    
    public void publish(Object obj) {
        final long id = _buffer.next();
        final MutableObject m = _buffer.get(id);
        m.setObject(obj);
        _buffer.publish(id);        
    }
}
