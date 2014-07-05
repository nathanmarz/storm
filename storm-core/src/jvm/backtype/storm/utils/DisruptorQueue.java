/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.utils;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import backtype.storm.metric.api.IStatefulObject;

/**
 *
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is
 * the ability to catch up to the producer by processing tuples in batches.
 */
public class DisruptorQueue implements IStatefulObject {
    private static final Object FLUSH_CACHE = new Object();
    private static final Object INTERRUPT = new Object();
    private static final String PREFIX = "disruptor-";

    private final String _queueName;
    private final RingBuffer<MutableObject> _buffer;
    private final Sequence _consumer;
    private final SequenceBarrier _barrier;

    // TODO: consider having a threadlocal cache of this variable to speed up reads?
    volatile boolean consumerStartedFlag = false;

    private final HashMap<String, Object> state = new HashMap<String, Object>(4);
    private final ConcurrentLinkedQueue<Object> _cache = new ConcurrentLinkedQueue<Object>();
    private final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
    private final Lock readLock  = cacheLock.readLock();
    private final Lock writeLock = cacheLock.writeLock();

    public DisruptorQueue(String queueName, ProducerType producerType, int bufferSize, WaitStrategy wait) {
        this._queueName = PREFIX + queueName;
        _buffer = RingBuffer.create(producerType, new ObjectEventFactory(), bufferSize, wait);
        _consumer = new Sequence();
        _barrier = _buffer.newBarrier();
        _buffer.addGatingSequences(_consumer);
        if(producerType == ProducerType.SINGLE) {
            consumerStartedFlag = true;
        } else {
            // make sure we flush the pending messages in cache first
            try {
                publishDirect(FLUSH_CACHE, true);
            } catch (InsufficientCapacityException e) {
                throw new RuntimeException("This code should be unreachable!", e);
            }
        }
    }
    
    public String getName() {
      return _queueName;
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
            final long availableSequence = _barrier.waitFor(nextSequence);
            if(availableSequence >= nextSequence) {
                consumeBatchToCursor(availableSequence, handler);
            }
        } catch (AlertException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
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

        boolean publishNow = consumerStartedFlag;

        if (!publishNow) {
            readLock.lock(); 
            try {
                publishNow = consumerStartedFlag;
                if (!publishNow) {
                    _cache.add(obj);
                }
            } finally {
                readLock.unlock();
            }
        }
        
        if (publishNow) {
            publishDirect(obj, block);
        }
    }
    
    private void publishDirect(Object obj, boolean block) throws InsufficientCapacityException {
        final long id;
        if(block) {
            id = _buffer.next();
        } else {
            id = _buffer.tryNext(1);
        }
        final MutableObject m = _buffer.get(id);
        m.setObject(obj);
        _buffer.publish(id);
    }
    
    public void consumerStarted() {

        consumerStartedFlag = true;
        
        // Use writeLock to make sure all pending cache add opearation completed
        writeLock.lock();
        writeLock.unlock();
    }
    
    public long  population() { return (writePos() - readPos()); }
    public long  capacity()   { return _buffer.getBufferSize(); }
    public long  writePos()   { return _buffer.getCursor(); }
    public long  readPos()    { return _consumer.get(); }
    public float pctFull()    { return (1.0F * population() / capacity()); }

    @Override
    public Object getState() {
        // get readPos then writePos so it's never an under-estimate
        long rp = readPos();
        long wp = writePos();
        state.put("capacity",   capacity());
        state.put("population", wp - rp);
        state.put("write_pos",  wp);
        state.put("read_pos",   rp);
        return state;
    }

    public static class ObjectEventFactory implements EventFactory<MutableObject> {
        @Override
        public MutableObject newInstance() {
            return new MutableObject();
        }        
    }
}
