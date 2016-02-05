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
package org.apache.storm.utils;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.metric.internal.RateTracker;

/**
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is
 * the ability to catch up to the producer by processing tuples in batches.
 */
public class DisruptorQueue implements IStatefulObject {
    private static final Logger LOG = LoggerFactory.getLogger(DisruptorQueue.class);    
    private static final Object INTERRUPT = new Object();
    private static final String PREFIX = "disruptor-";
    private static final FlusherPool FLUSHER = new FlusherPool();

    private static class FlusherPool { 
        private Timer _timer = new Timer("disruptor-flush-trigger", true);
        private ThreadPoolExecutor _exec = new ThreadPoolExecutor(1, 100, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1024), new ThreadPoolExecutor.DiscardPolicy());
        private HashMap<Long, ArrayList<Flusher>> _pendingFlush = new HashMap<>();
        private HashMap<Long, TimerTask> _tt = new HashMap<>();

        public synchronized void start(Flusher flusher, final long flushInterval) {
            ArrayList<Flusher> pending = _pendingFlush.get(flushInterval);
            if (pending == null) {
                pending = new ArrayList<>();
                TimerTask t = new TimerTask() {
                    @Override
                    public void run() {
                        invokeAll(flushInterval);
                    }
                };
                _pendingFlush.put(flushInterval, pending);
                _timer.schedule(t, flushInterval, flushInterval);
                _tt.put(flushInterval, t);
            }
            pending.add(flusher);
        }

        private synchronized void invokeAll(long flushInterval) {
            ArrayList<Flusher> tasks = _pendingFlush.get(flushInterval);
            if (tasks != null) {
                for (Flusher f: tasks) {
                    _exec.submit(f);
                }
            }
        }

        public synchronized void stop(Flusher flusher, long flushInterval) {
            ArrayList<Flusher> pending = _pendingFlush.get(flushInterval);
            pending.remove(flusher);
            if (pending.size() == 0) {
                _pendingFlush.remove(flushInterval);
                _tt.remove(flushInterval).cancel();
            }
        }
    }

    private static class ObjectEventFactory implements EventFactory<AtomicReference<Object>> {
        @Override
        public AtomicReference<Object> newInstance() {
            return new AtomicReference<Object>();
        }
    }

    private interface ThreadLocalInserter {
        public void add(Object obj);
        public void forceBatch();
        public void flush(boolean block);
    }

    private class ThreadLocalJustInserter implements ThreadLocalInserter {
        private final ReentrantLock _flushLock;
        private final ConcurrentLinkedQueue<Object> _overflow;

        public ThreadLocalJustInserter() {
            _flushLock = new ReentrantLock();
            _overflow = new ConcurrentLinkedQueue<>();
        }

        //called by the main thread and should not block for an undefined period of time
        public synchronized void add(Object obj) {
            boolean inserted = false;
            if (_overflow.isEmpty()) {
                try {
                    publishDirectSingle(obj, false);
                    inserted = true;
                } catch (InsufficientCapacityException e) {
                    //Ignored
                }
            }

            if (!inserted) {
                _overflowCount.incrementAndGet();
                _overflow.add(obj);
            }

            if (_enableBackpressure && _cb != null && (_metrics.population() + _overflowCount.get()) >= _highWaterMark) {
                try {
                    if (!_throttleOn) {
                        _cb.highWaterMark();
                        _throttleOn = true;
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Exception during calling highWaterMark callback!", e);
                }
            }
        }

        //May be called by a background thread
        public void forceBatch() {
            //NOOP
        }

        //May be called by a background thread
        public void flush(boolean block) {
            if (block) {
                _flushLock.lock();
            } else if (!_flushLock.tryLock()) {
               //Someone else if flushing so don't do anything
               return;
            }
            try {
                while (!_overflow.isEmpty()) {
                    publishDirectSingle(_overflow.peek(), block);
                    _overflowCount.addAndGet(-1);
                    _overflow.poll();
                }
            } catch (InsufficientCapacityException e) {
                //Ignored we should not block
            } finally {
                _flushLock.unlock();
            }
        }
    }

    private class ThreadLocalBatcher implements ThreadLocalInserter {
        private final ReentrantLock _flushLock;
        private final ConcurrentLinkedQueue<ArrayList<Object>> _overflow;
        private ArrayList<Object> _currentBatch;

        public ThreadLocalBatcher() {
            _flushLock = new ReentrantLock();
            _overflow = new ConcurrentLinkedQueue<ArrayList<Object>>();
            _currentBatch = new ArrayList<Object>(_inputBatchSize);
        }

        //called by the main thread and should not block for an undefined period of time
        public synchronized void add(Object obj) {
            _currentBatch.add(obj);
            _overflowCount.incrementAndGet();
            if (_enableBackpressure && _cb != null && (_metrics.population() + _overflowCount.get()) >= _highWaterMark) {
                try {
                    if (!_throttleOn) {
                        _cb.highWaterMark();
                        _throttleOn = true;
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Exception during calling highWaterMark callback!", e);
                }
            }
            if (_currentBatch.size() >= _inputBatchSize) {
                boolean flushed = false;
                if (_overflow.isEmpty()) {
                    try {
                        publishDirect(_currentBatch, false);
                        _overflowCount.addAndGet(0 - _currentBatch.size());
                        _currentBatch.clear();
                        flushed = true;
                    } catch (InsufficientCapacityException e) {
                        //Ignored we will flush later
                    }
                }

                if (!flushed) {        
                    _overflow.add(_currentBatch);
                    _currentBatch = new ArrayList<Object>(_inputBatchSize);
                }
            }
        }

        //May be called by a background thread
        public synchronized void forceBatch() {
            if (!_currentBatch.isEmpty()) {
                _overflow.add(_currentBatch);
                _currentBatch = new ArrayList<Object>(_inputBatchSize);
            }
        }

        //May be called by a background thread
        public void flush(boolean block) {
            if (block) {
                _flushLock.lock();
            } else if (!_flushLock.tryLock()) {
               //Someone else if flushing so don't do anything
               return;
            }
            try {
                while (!_overflow.isEmpty()) {
                    publishDirect(_overflow.peek(), block);
                    _overflowCount.addAndGet(0 - _overflow.poll().size());
                }
            } catch (InsufficientCapacityException e) {
                //Ignored we should not block
            } finally {
                _flushLock.unlock();
            }
        }
    }

    private class Flusher implements Runnable {
        private AtomicBoolean _isFlushing = new AtomicBoolean(false);
        private final long _flushInterval;

        public Flusher(long flushInterval, String name) {
            _flushInterval = flushInterval;
        }

        public void run() {
            if (_isFlushing.compareAndSet(false, true)) {
                for (ThreadLocalInserter batcher: _batchers.values()) {
                    batcher.forceBatch();
                    batcher.flush(true);
                }
                _isFlushing.set(false);
            }
        }

        public void start() {
            FLUSHER.start(this, _flushInterval);
        }

        public void close() {
            FLUSHER.stop(this, _flushInterval);
        }
    }

    /**
     * This inner class provides methods to access the metrics of the disruptor queue.
     */
    public class QueueMetrics {
        private final RateTracker _rateTracker = new RateTracker(10000, 10);

        public long writePos() {
            return _buffer.getCursor();
        }

        public long readPos() {
            return _consumer.get();
        }

        public long overflow() {
            return _overflowCount.get();
        }

        public long population() {
            return writePos() - readPos();
        }

        public long capacity() {
            return _buffer.getBufferSize();
        }

        public float pctFull() {
            return (1.0F * population() / capacity());
        }

        public Object getState() {
            Map state = new HashMap<String, Object>();

            // get readPos then writePos so it's never an under-estimate
            long rp = readPos();
            long wp = writePos();

            final double arrivalRateInSecs = _rateTracker.reportRate();

            //Assume the queue is stable, in which the arrival rate is equal to the consumption rate.
            // If this assumption does not hold, the calculation of sojourn time should also consider
            // departure rate according to Queuing Theory.
            final double sojournTime = (wp - rp) / Math.max(arrivalRateInSecs, 0.00001) * 1000.0;

            state.put("capacity", capacity());
            state.put("population", wp - rp);
            state.put("write_pos", wp);
            state.put("read_pos", rp);
            state.put("arrival_rate_secs", arrivalRateInSecs);
            state.put("sojourn_time_ms", sojournTime); //element sojourn time in milliseconds
            state.put("overflow", _overflowCount.get());

            return state;
        }

        public void notifyArrivals(long counts) {
            _rateTracker.notify(counts);
        }
    }

    private final RingBuffer<AtomicReference<Object>> _buffer;
    private final Sequence _consumer;
    private final SequenceBarrier _barrier;
    private final int _inputBatchSize;
    private final ConcurrentHashMap<Long, ThreadLocalInserter> _batchers = new ConcurrentHashMap<Long, ThreadLocalInserter>();
    private final Flusher _flusher;
    private final QueueMetrics _metrics;

    private String _queueName = "";
    private DisruptorBackpressureCallback _cb = null;
    private int _highWaterMark = 0;
    private int _lowWaterMark = 0;
    private boolean _enableBackpressure = false;
    private final AtomicLong _overflowCount = new AtomicLong(0);
    private volatile boolean _throttleOn = false;

    public DisruptorQueue(String queueName, ProducerType type, int size, long readTimeout, int inputBatchSize, long flushInterval) {
        this._queueName = PREFIX + queueName;
        WaitStrategy wait;
        if (readTimeout <= 0) {
            wait = new LiteBlockingWaitStrategy();
        } else {
            wait = new TimeoutBlockingWaitStrategy(readTimeout, TimeUnit.MILLISECONDS);
        }

        _buffer = RingBuffer.create(type, new ObjectEventFactory(), size, wait);
        _consumer = new Sequence();
        _barrier = _buffer.newBarrier();
        _buffer.addGatingSequences(_consumer);
        _metrics = new QueueMetrics();
        //The batch size can be no larger than half the full queue size.
        //This is mostly to avoid contention issues.
        _inputBatchSize = Math.max(1, Math.min(inputBatchSize, size/2));

        _flusher = new Flusher(Math.max(flushInterval, 1), _queueName);
        _flusher.start();
    }

    public String getName() {
        return _queueName;
    }

    public boolean isFull() {
        return (_metrics.population() + _overflowCount.get()) >= _metrics.capacity();
    }

    public void haltWithInterrupt() {
        try {
            publishDirect(new ArrayList<Object>(Arrays.asList(INTERRUPT)), true);
            _flusher.close();
        } catch (InsufficientCapacityException e) {
            //This should be impossible
            throw new RuntimeException(e);
        }
    }

    public void consumeBatch(EventHandler<Object> handler) {
        if (_metrics.population() > 0) {
            consumeBatchWhenAvailable(handler);
        }
    }

    public void consumeBatchWhenAvailable(EventHandler<Object> handler) {
        try {
            final long nextSequence = _consumer.get() + 1;
            long availableSequence = _barrier.waitFor(nextSequence);

            if (availableSequence >= nextSequence) {
                consumeBatchToCursor(availableSequence, handler);
            }
        } catch (TimeoutException te) {
            //Ignored
        } catch (AlertException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void consumeBatchToCursor(long cursor, EventHandler<Object> handler) {
        for (long curr = _consumer.get() + 1; curr <= cursor; curr++) {
            try {
                AtomicReference<Object> mo = _buffer.get(curr);
                Object o = mo.getAndSet(null);
                if (o == INTERRUPT) {
                    throw new InterruptedException("Disruptor processing interrupted");
                } else if (o == null) {
                    LOG.error("NULL found in {}:{}", this.getName(), cursor);
                } else {
                    handler.onEvent(o, curr, curr == cursor);
                    if (_enableBackpressure && _cb != null && (_metrics.writePos() - curr + _overflowCount.get()) <= _lowWaterMark) {
                        try {
                            if (_throttleOn) {
                                _throttleOn = false;
                                _cb.lowWaterMark();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Exception during calling lowWaterMark callback!");
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        _consumer.set(cursor);
    }

    public void registerBackpressureCallback(DisruptorBackpressureCallback cb) {
        this._cb = cb;
    }

    private static Long getId() {
        return Thread.currentThread().getId();
    }

    private void publishDirectSingle(Object obj, boolean block) throws InsufficientCapacityException {
        long at;
        if (block) {
            at = _buffer.next();
        } else {
            at = _buffer.tryNext();
        }
        AtomicReference<Object> m = _buffer.get(at);
        m.set(obj);
        _buffer.publish(at);
        _metrics.notifyArrivals(1);
    }

    private void publishDirect(ArrayList<Object> objs, boolean block) throws InsufficientCapacityException {
        int size = objs.size();
        if (size > 0) {
            long end;
            if (block) {
                end = _buffer.next(size);
            } else {
                end = _buffer.tryNext(size);
            }
            long begin = end - (size - 1);
            long at = begin;
            for (Object obj: objs) {
                AtomicReference<Object> m = _buffer.get(at);
                m.set(obj);
                at++;
            }
            _buffer.publish(begin, end);
            _metrics.notifyArrivals(size);
        }
    }

    public void publish(Object obj) {
        Long id = getId();
        ThreadLocalInserter batcher = _batchers.get(id);
        if (batcher == null) {
            //This thread is the only one ever creating this, so this is safe
            if (_inputBatchSize > 1) {
                batcher = new ThreadLocalBatcher();
            } else {
                batcher = new ThreadLocalJustInserter();
            }
            _batchers.put(id, batcher);
        }
        batcher.add(obj);
        batcher.flush(false);
    }

    @Override
    public Object getState() {
        return _metrics.getState();
    }

    public DisruptorQueue setHighWaterMark(double highWaterMark) {
        this._highWaterMark = (int)(_metrics.capacity() * highWaterMark);
        return this;
    }

    public DisruptorQueue setLowWaterMark(double lowWaterMark) {
        this._lowWaterMark = (int)(_metrics.capacity() * lowWaterMark);
        return this;
    }

    public int getHighWaterMark() {
        return this._highWaterMark;
    }

    public int getLowWaterMark() {
        return this._lowWaterMark;
    }

    public DisruptorQueue setEnableBackpressure(boolean enableBackpressure) {
        this._enableBackpressure = enableBackpressure;
        return this;
    }

    //This method enables the metrics to be accessed from outside of the DisruptorQueue class
    public QueueMetrics getMetrics() {
        return _metrics;
    }
}
