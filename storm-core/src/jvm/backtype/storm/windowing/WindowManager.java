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
package backtype.storm.windowing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static backtype.storm.topology.base.BaseWindowedBolt.Count;
import static backtype.storm.topology.base.BaseWindowedBolt.Duration;

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
    private final ConcurrentLinkedQueue<Event<T>> window;
    private final List<T> expiredEvents;
    private final Set<Event<T>> prevWindowEvents;
    private final AtomicInteger eventsSinceLastExpiry;
    private final ReentrantLock lock;
    private EvictionPolicy<T> evictionPolicy;
    private TriggerPolicy<T> triggerPolicy;

    public WindowManager(WindowLifecycleListener<T> lifecycleListener) {
        windowLifecycleListener = lifecycleListener;
        window = new ConcurrentLinkedQueue<>();
        expiredEvents = new ArrayList<>();
        prevWindowEvents = new HashSet<>();
        eventsSinceLastExpiry = new AtomicInteger();
        lock = new ReentrantLock(true);
    }

    public void setWindowLength(Count count) {
        this.evictionPolicy = new CountEvictionPolicy<>(count.value);
    }

    public void setWindowLength(Duration duration) {
        this.evictionPolicy = new TimeEvictionPolicy<>(duration.value);
    }

    public void setSlidingInterval(Count count) {
        this.triggerPolicy = new CountTriggerPolicy<>(count.value, this);
    }

    public void setSlidingInterval(Duration duration) {
        this.triggerPolicy = new TimeTriggerPolicy<>(duration.value, this);
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
        Event<T> windowEvent = new EventImpl<T>(event, ts);
        window.add(windowEvent);
        track(windowEvent);
        compactWindow();
    }

    /**
     * The callback invoked by the trigger policy.
     */
    @Override
    public void onTrigger() {
        List<Event<T>> windowEvents = null;
        List<T> expired = null;
        try {
            lock.lock();
            /*
             * scan the entire window to handle out of order events in
             * the case of time based windows.
             */
            windowEvents = expireEvents(true);
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
        prevWindowEvents.addAll(windowEvents);
        LOG.debug("invoking windowLifecycleListener onActivation, [{}] events in window.", windowEvents.size());
        windowLifecycleListener.onActivation(events, newEvents, expired);
        triggerPolicy.reset();
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
            expireEvents(false);
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
     * Expire events from the window, using the expiration policy to check
     * if the event should be evicted or not.
     *
     * @param fullScan if set, will scan the entire window; if not set, will stop
     *                 as soon as an event not satisfying the expiration policy is found
     * @return the list of remaining events in the window after expiry
     */
    private List<Event<T>> expireEvents(boolean fullScan) {
        LOG.debug("Expire events, eviction policy {}", evictionPolicy);
        List<T> eventsToExpire = new ArrayList<>();
        List<Event<T>> remaining = new ArrayList<>();
        try {
            lock.lock();
            Iterator<Event<T>> it = window.iterator();
            while (it.hasNext()) {
                Event<T> windowEvent = it.next();
                if (evictionPolicy.evict(windowEvent)) {
                    eventsToExpire.add(windowEvent.get());
                    it.remove();
                } else if (!fullScan) {
                    break;
                } else {
                    remaining.add(windowEvent);
                }
            }
            expiredEvents.addAll(eventsToExpire);
        } finally {
            lock.unlock();
        }
        eventsSinceLastExpiry.set(0);
        LOG.debug("[{}] events expired from window.", eventsToExpire.size());
        windowLifecycleListener.onExpiry(eventsToExpire);
        return remaining;
    }

    @Override
    public String toString() {
        return "WindowManager{" +
                "evictionPolicy=" + evictionPolicy +
                ", triggerPolicy=" + triggerPolicy +
                '}';
    }
}
