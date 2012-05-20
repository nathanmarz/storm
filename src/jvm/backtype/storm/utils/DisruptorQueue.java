package backtype.storm.utils;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class allows publishers to start publishing before consumer is set up,
 * making it much easier to structure code around the disruptor.
 */
public class DisruptorQueue {
    private static final Object MARKER = new Object();
    private static final Object HALT_PROCESSING = new Object();
    Disruptor _disruptor;
    ExecutorService _executor;
    WaiterEventHandler _handler;
    RingBuffer<MutableObject> _buffer;
    
    
    public DisruptorQueue(Number bufferSize) {
        _executor = Executors.newSingleThreadExecutor();
        _disruptor = new Disruptor(new ObjectEventFactory(), _executor, new MultiThreadedClaimStrategy(bufferSize.intValue()), new YieldingWaitStrategy());
        _handler = new WaiterEventHandler();
        _disruptor.handleEventsWith(_handler);
        _buffer = _disruptor.start();
    }
    
    public void setHandler(EventHandler handler) {
        _handler.setHandler(handler);
        publish(MARKER);
    }
    
    public void publish(Object o) {
        final long id = _buffer.next();
        final MutableObject m = _buffer.get(id);
        m.setObject(o);
        _buffer.publish(id);
    }
    
    public void haltProcessing() {
        publish(HALT_PROCESSING);
    }
    
    public void shutdown() {
        _disruptor.shutdown();
        _executor.shutdown();
    }
    
    static class WaiterEventHandler implements EventHandler<MutableObject> {
        AtomicBoolean started = new AtomicBoolean(false);
        volatile EventHandler _promise = null;
        EventHandler _cached = null;
        List<CachedEvent> _toHandle = new ArrayList<CachedEvent>();
        boolean halted = false;
        
        public void setHandler(EventHandler handler) {
            if(_promise!=null) {
                throw new RuntimeException("Cannot set event handler more than once");
            }
            _promise = handler;
        }
        
        @Override
        public void onEvent(MutableObject t, long sequence, boolean isBatchEnd) throws Exception {
            Object o = t.getObject();
            if(o==HALT_PROCESSING) {
                _cached = null;
                halted = true;
            }
            if(_cached!=null && o != MARKER) {
                handleEvent(o, sequence, isBatchEnd);
            } else {
                if(!halted) {
                    if(_promise!=null) {
                        _cached = _promise;
                        for(CachedEvent e: _toHandle) {
                            handleEvent(e.o, e.seq, e.isBatchEnd);
                        }
                        _toHandle.clear();
                        if(o != MARKER) handleEvent(o, sequence, isBatchEnd);
                    } else {
                        if(o==MARKER) {
                            throw new RuntimeException("Got marker object before promise was set");
                        }
                        _toHandle.add(new CachedEvent(o, sequence, isBatchEnd));
                    }
                }
            }
        }
        
        private void handleEvent(Object o, long sequence, boolean isBatchEnd) throws Exception {
            _cached.onEvent(o, sequence, isBatchEnd);
        }
        
    }
    
    static class CachedEvent {
        public Object o;
        public long seq;
        public boolean isBatchEnd;
        
        public CachedEvent(Object o, long seq, boolean isBatchEnd) {
            this.o = o;
            this.seq = seq;
            this.isBatchEnd = isBatchEnd;
        }
    }

    static class ObjectEventFactory implements EventFactory<MutableObject> {
        @Override
        public MutableObject newInstance() {
            return new MutableObject();
        }        
    }
}
