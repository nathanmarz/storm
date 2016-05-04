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
package org.apache.storm.windowing;

import org.apache.storm.windowing.EvictionPolicy.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.storm.windowing.EvictionPolicy.Action.EXPIRE;
import static org.apache.storm.windowing.EvictionPolicy.Action.PROCESS;
import static org.apache.storm.windowing.EvictionPolicy.Action.STOP;

/**
 * Tracks a window of events and fires {@link WindowLifecycleListener} callbacks
 * on expiry of events or activation of the window due to {@link TriggerPolicy}.
 *
 * @param <T> the type of event in the window.
 */
public class WindowManager<T> implements TriggerHandler {
    private static final Logger LOG = LoggerFactory.getLogger(WindowManager.class);

    /**
     * Expire old events every EXPIRE_EVENTS_THRESHOLD to
     * keep the window size in check.
     */
    public static final int EXPIRE_EVENTS_THRESHOLD = 100;

    private final WindowLifecycleListener<T> windowLifecycleListener;
    private final ConcurrentLinkedQueue<Event<T>> queue;
    private final List<T> expiredEvents;
    private final Set<Event<T>> prevWindowEvents;
    private final AtomicInteger eventsSinceLastExpiry;
    private final ReentrantLock lock;
    private EvictionPolicy<T> evictionPolicy;
    private TriggerPolicy<T> triggerPolicy;

    public WindowManager(WindowLifecycleListener<T> lifecycleListener) {
        windowLifecycleListener = lifecycleListener;
        queue = new ConcurrentLinkedQueue<>();
        expiredEvents = new ArrayList<>();
        prevWindowEvents = new HashSet<>();
        eventsSinceLastExpiry = new AtomicInteger();
        lock = new ReentrantLock(true);
    }

    public void setEvictionPolicy(EvictionPolicy<T> evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
    }

    public void setTriggerPolicy(TriggerPolicy<T> triggerPolicy) {
        this.triggerPolicy = triggerPolicy;
    }

    /**
     * Add an event into the window, with {@link System#currentTimeMillis()} as
     * the tracking ts.
     *
     * @param event the event to add
     */
    public void add(T event) {
        add(event, System.currentTimeMillis());
    }

    /**
     * Add an event into the window, with the given ts as the tracking ts.
     *
     * @param event the event to track
     * @param ts    the timestamp
     */
    public void add(T event, long ts) {
        add(new EventImpl<T>(event, ts));
    }

    /**
     * Tracks a window event
     *
     * @param windowEvent the window event to track
     */
    public void add(Event<T> windowEvent) {
        // watermark events are not added to the queue.
        if (!windowEvent.isWatermark()) {
            queue.add(windowEvent);
        } else {
            LOG.debug("Got watermark event with ts {}", windowEvent.getTimestamp());
        }
        track(windowEvent);
        compactWindow();
    }

    /**
     * The callback invoked by the trigger policy.
     */
    @Override
    public boolean onTrigger() {
        List<Event<T>> windowEvents = null;
        List<T> expired = null;
        try {
            lock.lock();
            /*
             * scan the entire window to handle out of order events in
             * the case of time based windows.
             */
            windowEvents = scanEvents(true);
            expired = new ArrayList<>(expiredEvents);
            expiredEvents.clear();
        } finally {
            lock.unlock();
        }
        List<T> events = new ArrayList<>();
        List<T> newEvents = new ArrayList<>();
        for (Event<T> event : windowEvents) {
            events.add(event.get());
            if (!prevWindowEvents.contains(event)) {
                newEvents.add(event.get());
            }
        }
        prevWindowEvents.clear();
        if (!events.isEmpty()) {
            prevWindowEvents.addAll(windowEvents);
            LOG.debug("invoking windowLifecycleListener onActivation, [{}] events in window.", events.size());
            windowLifecycleListener.onActivation(events, newEvents, expired);
        } else {
            LOG.debug("No events in the window, skipping onActivation");
        }
        triggerPolicy.reset();
        return !events.isEmpty();
    }

    public void shutdown() {
        LOG.debug("Shutting down WindowManager");
        if (triggerPolicy != null) {
            triggerPolicy.shutdown();
        }
    }

    /**
     * expires events that fall out of the window every
     * EXPIRE_EVENTS_THRESHOLD so that the window does not grow
     * too big.
     */
    private void compactWindow() {
        if (eventsSinceLastExpiry.incrementAndGet() >= EXPIRE_EVENTS_THRESHOLD) {
            scanEvents(false);
        }
    }

    /**
     * feed the event to the eviction and trigger policies
     * for bookkeeping and optionally firing the trigger.
     */
    private void track(Event<T> windowEvent) {
        evictionPolicy.track(windowEvent);
        triggerPolicy.track(windowEvent);
    }

    /**
     * Scan events in the queue, using the expiration policy to check
     * if the event should be evicted or not.
     *
     * @param fullScan if set, will scan the entire queue; if not set, will stop
     *                 as soon as an event not satisfying the expiration policy is found
     * @return the list of events to be processed as a part of the current window
     */
    private List<Event<T>> scanEvents(boolean fullScan) {
        LOG.debug("Scan events, eviction policy {}", evictionPolicy);
        List<T> eventsToExpire = new ArrayList<>();
        List<Event<T>> eventsToProcess = new ArrayList<>();
        try {
            lock.lock();
            Iterator<Event<T>> it = queue.iterator();
            while (it.hasNext()) {
                Event<T> windowEvent = it.next();
                Action action = evictionPolicy.evict(windowEvent);
                if (action == EXPIRE) {
                    eventsToExpire.add(windowEvent.get());
                    it.remove();
                } else if (!fullScan || action == STOP) {
                    break;
                } else if (action == PROCESS) {
                    eventsToProcess.add(windowEvent);
                }
            }
            expiredEvents.addAll(eventsToExpire);
        } finally {
            lock.unlock();
        }
        eventsSinceLastExpiry.set(0);
        LOG.debug("[{}] events expired from window.", eventsToExpire.size());
        if (!eventsToExpire.isEmpty()) {
            LOG.debug("invoking windowLifecycleListener.onExpiry");
            windowLifecycleListener.onExpiry(eventsToExpire);
        }
        return eventsToProcess;
    }

    /**
     * Scans the event queue and returns the next earliest event ts
     * between the startTs and endTs
     *
     * @param startTs the start ts (exclusive)
     * @param endTs the end ts (inclusive)
     * @return the earliest event ts between startTs and endTs
     */
    public long getEarliestEventTs(long startTs, long endTs) {
        long minTs = Long.MAX_VALUE;
        for (Event<T> event : queue) {
            if (event.getTimestamp() > startTs && event.getTimestamp() <= endTs) {
                minTs = Math.min(minTs, event.getTimestamp());
            }
        }
        return minTs;
    }

    /**
     * Scans the event queue and returns number of events having
     * timestamp less than or equal to the reference time.
     *
     * @param referenceTime the reference timestamp in millis
     * @return the count of events with timestamp less than or equal to referenceTime
     */
    public int getEventCount(long referenceTime) {
        int count = 0;
        for (Event<T> event : queue) {
            if (event.getTimestamp() <= referenceTime) {
                ++count;
            }
        }
        return count;
    }

    /**
     * Scans the event queue and returns the list of event ts
     * falling between startTs (exclusive) and endTs (inclusive)
     * at each sliding interval counts.
     *
     * @param startTs the start timestamp (exclusive)
     * @param endTs the end timestamp (inclusive)
     * @param slidingCount the sliding interval count
     * @return the list of event ts
     */
    public List<Long> getSlidingCountTimestamps(long startTs, long endTs, int slidingCount) {
        List<Long> timestamps = new ArrayList<>();
        if (endTs > startTs) {
            int count = 0;
            long ts = Long.MIN_VALUE;
            for (Event<T> event : queue) {
                if (event.getTimestamp() > startTs && event.getTimestamp() <= endTs) {
                    ts = Math.max(ts, event.getTimestamp());
                    if (++count % slidingCount == 0) {
                        timestamps.add(ts);
                    }
                }
            }
        }
        return timestamps;
    }

    @Override
    public String toString() {
        return "WindowManager{" +
                "evictionPolicy=" + evictionPolicy +
                ", triggerPolicy=" + triggerPolicy +
                '}';
    }
}
