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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static backtype.storm.topology.base.BaseWindowedBolt.Count;
import static backtype.storm.topology.base.BaseWindowedBolt.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link WindowManager}
 */
public class WindowManagerTest {
    private WindowManager<Integer> windowManager;
    private Listener listener;

    private static class Listener implements WindowLifecycleListener<Integer> {
        List<Integer> onExpiryEvents = Collections.emptyList();
        List<Integer> onActivationEvents = Collections.emptyList();
        List<Integer> onActivationNewEvents = Collections.emptyList();
        List<Integer> onActivationExpiredEvents = Collections.emptyList();

        // all events since last clear
        List<List<Integer>> allOnExpiryEvents = new ArrayList<>();
        List<List<Integer>> allOnActivationEvents = new ArrayList<>();
        List<List<Integer>> allOnActivationNewEvents = new ArrayList<>();
        List<List<Integer>> allOnActivationExpiredEvents = new ArrayList<>();

        @Override
        public void onExpiry(List<Integer> events) {
            onExpiryEvents = events;
            allOnExpiryEvents.add(events);
        }

        @Override
        public void onActivation(List<Integer> events, List<Integer> newEvents, List<Integer> expired) {
            onActivationEvents = events;
            allOnActivationEvents.add(events);
            onActivationNewEvents = newEvents;
            allOnActivationNewEvents.add(newEvents);
            onActivationExpiredEvents = expired;
            allOnActivationExpiredEvents.add(expired);
        }

        void clear() {
            onExpiryEvents = Collections.emptyList();
            onActivationEvents = Collections.emptyList();
            onActivationNewEvents = Collections.emptyList();
            onActivationExpiredEvents = Collections.emptyList();

            allOnExpiryEvents.clear();
            allOnActivationEvents.clear();
            allOnActivationNewEvents.clear();
            allOnActivationExpiredEvents.clear();
        }
    }

    @Before
    public void setUp() {
        listener = new Listener();
        windowManager = new WindowManager<>(listener);
    }

    @After
    public void tearDown() {
        windowManager.shutdown();
    }

    @Test
    public void testCountBasedWindow() throws Exception {
        EvictionPolicy<Integer> evictionPolicy = new CountEvictionPolicy<Integer>(5);
        windowManager.setEvictionPolicy(evictionPolicy);
        windowManager.setTriggerPolicy(new CountTriggerPolicy<Integer>(2, windowManager, evictionPolicy));
        windowManager.add(1);
        windowManager.add(2);
        // nothing expired yet
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(seq(1, 2), listener.onActivationEvents);
        assertEquals(seq(1, 2), listener.onActivationNewEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        windowManager.add(3);
        windowManager.add(4);
        // nothing expired yet
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(seq(1, 4), listener.onActivationEvents);
        assertEquals(seq(3, 4), listener.onActivationNewEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        windowManager.add(5);
        windowManager.add(6);
        // 1 expired
        assertEquals(seq(1), listener.onExpiryEvents);
        assertEquals(seq(2, 6), listener.onActivationEvents);
        assertEquals(seq(5, 6), listener.onActivationNewEvents);
        assertEquals(seq(1), listener.onActivationExpiredEvents);
        listener.clear();
        windowManager.add(7);
        // nothing expires until threshold is hit
        assertTrue(listener.onExpiryEvents.isEmpty());
        windowManager.add(8);
        // 1 expired
        assertEquals(seq(2, 3), listener.onExpiryEvents);
        assertEquals(seq(4, 8), listener.onActivationEvents);
        assertEquals(seq(7, 8), listener.onActivationNewEvents);
        assertEquals(seq(2, 3), listener.onActivationExpiredEvents);
    }

    @Test
    public void testExpireThreshold() throws Exception {
        int threshold = WindowManager.EXPIRE_EVENTS_THRESHOLD;
        int windowLength = 5;
        windowManager.setEvictionPolicy(new CountEvictionPolicy<Integer>(5));
        windowManager.setTriggerPolicy(new TimeTriggerPolicy<Integer>(new Duration(1, TimeUnit.HOURS).value, windowManager));
        for (int i : seq(1, 5)) {
            windowManager.add(i);
        }
        // nothing expired yet
        assertTrue(listener.onExpiryEvents.isEmpty());
        for (int i : seq(6, 10)) {
            windowManager.add(i);
        }
        for (int i : seq(11, threshold)) {
            windowManager.add(i);
        }
        // window should be compacted and events should be expired.
        assertEquals(seq(1, threshold - windowLength), listener.onExpiryEvents);
    }


    @Test
    public void testTimeBasedWindow() throws Exception {
        EvictionPolicy<Integer> evictionPolicy = new TimeEvictionPolicy<Integer>(new Duration(1, TimeUnit.SECONDS).value);
        windowManager.setEvictionPolicy(evictionPolicy);
        /*
         * Don't wait for Timetrigger to fire since this could lead to timing issues in unit tests.
         * Set it to a large value and trigger manually.
          */
        windowManager.setTriggerPolicy(new TimeTriggerPolicy<Integer>(new Duration(1, TimeUnit.DAYS).value, windowManager, evictionPolicy));
        long now = System.currentTimeMillis();

        // add with past ts
        for (int i : seq(1, 50)) {
            windowManager.add(i, now - 1000);
        }

        // add with current ts
        for (int i : seq(51, WindowManager.EXPIRE_EVENTS_THRESHOLD)) {
            windowManager.add(i, now);
        }
        // first 50 should have expired due to expire events threshold
        assertEquals(50, listener.onExpiryEvents.size());

        // add more events with past ts
        for (int i : seq(WindowManager.EXPIRE_EVENTS_THRESHOLD + 1, WindowManager.EXPIRE_EVENTS_THRESHOLD + 100)) {
            windowManager.add(i, now - 1000);
        }
        // simulate the time trigger by setting the reference time and invoking onTrigger() manually
        evictionPolicy.setContext(now + 100);
        windowManager.onTrigger();

        // 100 events with past ts should expire
        assertEquals(100, listener.onExpiryEvents.size());
        assertEquals(seq(WindowManager.EXPIRE_EVENTS_THRESHOLD + 1, WindowManager.EXPIRE_EVENTS_THRESHOLD + 100),
                     listener.onExpiryEvents);
        List<Integer> activationsEvents = seq(51, WindowManager.EXPIRE_EVENTS_THRESHOLD);
        assertEquals(seq(51, WindowManager.EXPIRE_EVENTS_THRESHOLD), listener.onActivationEvents);
        assertEquals(seq(51, WindowManager.EXPIRE_EVENTS_THRESHOLD), listener.onActivationNewEvents);
        // activation expired list should contain even the ones expired due to EXPIRE_EVENTS_THRESHOLD
        List<Integer> expiredList = seq(1, 50);
        expiredList.addAll(seq(WindowManager.EXPIRE_EVENTS_THRESHOLD + 1, WindowManager.EXPIRE_EVENTS_THRESHOLD + 100));
        assertEquals(expiredList, listener.onActivationExpiredEvents);

        listener.clear();
        // add more events with current ts
        List<Integer> newEvents = seq(WindowManager.EXPIRE_EVENTS_THRESHOLD + 101, WindowManager.EXPIRE_EVENTS_THRESHOLD + 200);
        for (int i : newEvents) {
            windowManager.add(i, now);
        }
        activationsEvents.addAll(newEvents);
        // simulate the time trigger by setting the reference time and invoking onTrigger() manually
        evictionPolicy.setContext(now + 200);
        windowManager.onTrigger();
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(activationsEvents, listener.onActivationEvents);
        assertEquals(newEvents, listener.onActivationNewEvents);

    }


    @Test
    public void testTimeBasedWindowExpiry() throws Exception {
        EvictionPolicy<Integer> evictionPolicy = new TimeEvictionPolicy<Integer>(new Duration(100, TimeUnit.MILLISECONDS).value);
        windowManager.setEvictionPolicy(evictionPolicy);
        /*
         * Don't wait for Timetrigger to fire since this could lead to timing issues in unit tests.
         * Set it to a large value and trigger manually.
          */
        windowManager.setTriggerPolicy(new TimeTriggerPolicy<Integer>(new Duration(1, TimeUnit.DAYS).value, windowManager));
        long now = System.currentTimeMillis();
        // add 10 events
        for (int i : seq(1, 10)) {
            windowManager.add(i);
        }
        // simulate the time trigger by setting the reference time and invoking onTrigger() manually
        evictionPolicy.setContext(now + 60);
        windowManager.onTrigger();

        assertEquals(seq(1, 10), listener.onActivationEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        listener.clear();
        // wait so all events expire
        evictionPolicy.setContext(now + 120);
        windowManager.onTrigger();

        assertEquals(seq(1, 10), listener.onExpiryEvents);
        assertTrue(listener.onActivationEvents.isEmpty());
        listener.clear();
        evictionPolicy.setContext(now + 180);
        windowManager.onTrigger();
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        assertTrue(listener.onActivationEvents.isEmpty());

    }

    @Test
    public void testTumblingWindow() throws Exception {
        EvictionPolicy<Integer> evictionPolicy = new CountEvictionPolicy<Integer>(3);
        windowManager.setEvictionPolicy(evictionPolicy);
        windowManager.setTriggerPolicy(new CountTriggerPolicy<Integer>(3, windowManager, evictionPolicy));
        windowManager.add(1);
        windowManager.add(2);
        // nothing expired yet
        assertTrue(listener.onExpiryEvents.isEmpty());
        windowManager.add(3);
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(seq(1, 3), listener.onActivationEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        assertEquals(seq(1, 3), listener.onActivationNewEvents);

        listener.clear();
        windowManager.add(4);
        windowManager.add(5);
        windowManager.add(6);

        assertEquals(seq(1, 3), listener.onExpiryEvents);
        assertEquals(seq(4, 6), listener.onActivationEvents);
        assertEquals(seq(1, 3), listener.onActivationExpiredEvents);
        assertEquals(seq(4, 6), listener.onActivationNewEvents);

    }


    @Test
    public void testEventTimeBasedWindow() throws Exception {
        EvictionPolicy<Integer> evictionPolicy = new WatermarkTimeEvictionPolicy<>(20);
        windowManager.setEvictionPolicy(evictionPolicy);
        windowManager.setTriggerPolicy(new WatermarkTimeTriggerPolicy<Integer>(10, windowManager, evictionPolicy, windowManager));

        windowManager.add(1, 603);
        windowManager.add(2, 605);
        windowManager.add(3, 607);

        // This should trigger the scan to find
        // the next aligned window end ts, but not produce any activations
        windowManager.add(new WaterMarkEvent<Integer>(609));
        assertEquals(Collections.emptyList(), listener.allOnActivationEvents);

        windowManager.add(4, 618);
        windowManager.add(5, 626);
        windowManager.add(6, 636);
        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));

//        System.out.println(listener.allOnActivationEvents);
        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(seq(1,3), listener.allOnActivationEvents.get(0));
        assertEquals(seq(1, 4), listener.allOnActivationEvents.get(1));
        assertEquals(seq(4, 5), listener.allOnActivationEvents.get(2));

        assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(0));
        assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(1));
        assertEquals(seq(1, 3), listener.allOnActivationExpiredEvents.get(2));

        assertEquals(seq(1, 3), listener.allOnActivationNewEvents.get(0));
        assertEquals(seq(4, 4), listener.allOnActivationNewEvents.get(1));
        assertEquals(seq(5, 5), listener.allOnActivationNewEvents.get(2));

        assertEquals(seq(1, 3), listener.allOnExpiryEvents.get(0));

        // add more events with a gap in ts
        windowManager.add(7, 825);
        windowManager.add(8, 826);
        windowManager.add(9, 827);
        windowManager.add(10, 839);

        listener.clear();
        windowManager.add(new WaterMarkEvent<Integer>(834));

        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(seq(5, 6), listener.allOnActivationEvents.get(0));
        assertEquals(seq(6, 6), listener.allOnActivationEvents.get(1));
        assertEquals(seq(7, 9), listener.allOnActivationEvents.get(2));

        assertEquals(seq(4, 4), listener.allOnActivationExpiredEvents.get(0));
        assertEquals(seq(5, 5), listener.allOnActivationExpiredEvents.get(1));
        assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(2));

        assertEquals(seq(6,6), listener.allOnActivationNewEvents.get(0));
        assertEquals(Collections.emptyList(), listener.allOnActivationNewEvents.get(1));
        assertEquals(seq(7, 9), listener.allOnActivationNewEvents.get(2));

        assertEquals(seq(4, 4), listener.allOnExpiryEvents.get(0));
        assertEquals(seq(5, 5), listener.allOnExpiryEvents.get(1));
        assertEquals(seq(6, 6), listener.allOnExpiryEvents.get(2));
    }

    @Test
    public void testCountBasedWindowWithEventTs() throws Exception {
        EvictionPolicy<Integer> evictionPolicy = new WatermarkCountEvictionPolicy<>(3, windowManager);
        windowManager.setEvictionPolicy(evictionPolicy);
        windowManager.setTriggerPolicy(new WatermarkTimeTriggerPolicy<Integer>(10, windowManager, evictionPolicy, windowManager));

        windowManager.add(1, 603);
        windowManager.add(2, 605);
        windowManager.add(3, 607);
        windowManager.add(4, 618);
        windowManager.add(5, 626);
        windowManager.add(6, 636);
        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));

        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(seq(1, 3), listener.allOnActivationEvents.get(0));
        assertEquals(seq(2, 4), listener.allOnActivationEvents.get(1));
        assertEquals(seq(3, 5), listener.allOnActivationEvents.get(2));

        // add more events with a gap in ts
        windowManager.add(7, 665);
        windowManager.add(8, 666);
        windowManager.add(9, 667);
        windowManager.add(10, 679);

        listener.clear();
        windowManager.add(new WaterMarkEvent<Integer>(674));
//        System.out.println(listener.allOnActivationEvents);
        assertEquals(4, listener.allOnActivationEvents.size());
        // same set of events part of three windows
        assertEquals(seq(4, 6), listener.allOnActivationEvents.get(0));
        assertEquals(seq(4, 6), listener.allOnActivationEvents.get(1));
        assertEquals(seq(4, 6), listener.allOnActivationEvents.get(2));
        assertEquals(seq(7, 9), listener.allOnActivationEvents.get(3));
    }

    @Test
    public void testCountBasedTriggerWithEventTs() throws Exception {
        EvictionPolicy<Integer> evictionPolicy = new WatermarkTimeEvictionPolicy<Integer>(20);
        windowManager.setEvictionPolicy(evictionPolicy);
        windowManager.setTriggerPolicy(new WatermarkCountTriggerPolicy<Integer>(3, windowManager,
                                                                                evictionPolicy, windowManager));

        windowManager.add(1, 603);
        windowManager.add(2, 605);
        windowManager.add(3, 607);
        windowManager.add(4, 618);
        windowManager.add(5, 625);
        windowManager.add(6, 626);
        windowManager.add(7, 629);
        windowManager.add(8, 636);
        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));
//        System.out.println(listener.allOnActivationEvents);

        assertEquals(2, listener.allOnActivationEvents.size());
        assertEquals(seq(1, 3), listener.allOnActivationEvents.get(0));
        assertEquals(seq(3, 6), listener.allOnActivationEvents.get(1));

        // add more events with a gap in ts
        windowManager.add(9, 665);
        windowManager.add(10, 666);
        windowManager.add(11, 667);
        windowManager.add(12, 669);
        windowManager.add(12, 679);

        listener.clear();
        windowManager.add(new WaterMarkEvent<Integer>(674));
//        System.out.println(listener.allOnActivationEvents);
        assertEquals(2, listener.allOnActivationEvents.size());
        // same set of events part of three windows
        assertEquals(seq(9), listener.allOnActivationEvents.get(0));
        assertEquals(seq(9, 12), listener.allOnActivationEvents.get(1));
    }
    @Test
    public void testEventTimeLag() throws Exception {
        EvictionPolicy<Integer> evictionPolicy = new WatermarkTimeEvictionPolicy<>(20, 5);
        windowManager.setEvictionPolicy(evictionPolicy);
        windowManager.setTriggerPolicy(new WatermarkTimeTriggerPolicy<Integer>(10, windowManager, evictionPolicy, windowManager));

        windowManager.add(1, 603);
        windowManager.add(2, 605);
        windowManager.add(3, 607);
        windowManager.add(4, 618);
        windowManager.add(5, 626);
        windowManager.add(6, 632);
        windowManager.add(7, 629);
        windowManager.add(8, 636);
        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));
//        System.out.println(listener.allOnActivationEvents);
        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(seq(1, 3), listener.allOnActivationEvents.get(0));
        assertEquals(seq(1, 4), listener.allOnActivationEvents.get(1));
        // out of order events should be processed upto the lag
        assertEquals(Arrays.asList(4, 5, 7), listener.allOnActivationEvents.get(2));
    }

    @Test
    public void testScanStop() throws Exception {
        final Set<Integer> eventsScanned = new HashSet<>();
        EvictionPolicy<Integer> evictionPolicy = new WatermarkTimeEvictionPolicy<Integer>(20, 5) {

            @Override
            public Action evict(Event<Integer> event) {
                eventsScanned.add(event.get());
                return super.evict(event);
            }

        };
        windowManager.setEvictionPolicy(evictionPolicy);
        windowManager.setTriggerPolicy(new WatermarkTimeTriggerPolicy<Integer>(10, windowManager, evictionPolicy, windowManager));

        windowManager.add(1, 603);
        windowManager.add(2, 605);
        windowManager.add(3, 607);
        windowManager.add(4, 618);
        windowManager.add(5, 626);
        windowManager.add(6, 629);
        windowManager.add(7, 636);
        windowManager.add(8, 637);
        windowManager.add(9, 638);
        windowManager.add(10, 639);

        // send a watermark event, which should trigger three windows.
        windowManager.add(new WaterMarkEvent<Integer>(631));

        assertEquals(3, listener.allOnActivationEvents.size());
        assertEquals(seq(1, 3), listener.allOnActivationEvents.get(0));
        assertEquals(seq(1, 4), listener.allOnActivationEvents.get(1));

        // out of order events should be processed upto the lag
        assertEquals(Arrays.asList(4, 5, 6), listener.allOnActivationEvents.get(2));

        // events 8, 9, 10 should not be scanned at all since TimeEvictionPolicy lag 5s should break
        // the WindowManager scan loop early.
        assertEquals(new HashSet<>(seq(1, 7)), eventsScanned);
    }

    private List<Integer> seq(int start) {
        return seq(start, start);
    }

    private List<Integer> seq(int start, int stop) {
        List<Integer> ints = new ArrayList<>();
        for (int i = start; i <= stop; i++) {
            ints.add(i);
        }
        return ints;
    }
}