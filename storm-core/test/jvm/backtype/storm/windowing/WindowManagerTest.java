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
import java.util.Collections;
import java.util.List;
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

        @Override
        public void onExpiry(List<Integer> events) {
            onExpiryEvents = events;
        }

        @Override
        public void onActivation(List<Integer> events, List<Integer> newEvents, List<Integer> expired) {
            onActivationEvents = events;
            onActivationNewEvents = newEvents;
            onActivationExpiredEvents = expired;
        }

        void clear() {
            onExpiryEvents = Collections.emptyList();
            onActivationEvents = Collections.emptyList();
            onActivationNewEvents = Collections.emptyList();
            onActivationExpiredEvents = Collections.emptyList();
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
        windowManager.setWindowLength(new Count(5));
        windowManager.setSlidingInterval(new Count(2));
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
        windowManager.setWindowLength(new Count(5));
        windowManager.setSlidingInterval(new Duration(1, TimeUnit.HOURS));
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
        windowManager.setWindowLength(new Duration(1, TimeUnit.SECONDS));
        windowManager.setSlidingInterval(new Duration(100, TimeUnit.MILLISECONDS));
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
        // wait for time trigger
        Thread.sleep(120);

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
        // wait for time trigger
        Thread.sleep(120);
        assertTrue(listener.onExpiryEvents.isEmpty());
        assertEquals(activationsEvents, listener.onActivationEvents);
        assertEquals(newEvents, listener.onActivationNewEvents);

    }


    @Test
    public void testTimeBasedWindowExpiry() throws Exception {
        windowManager.setWindowLength(new Duration(100, TimeUnit.MILLISECONDS));
        windowManager.setSlidingInterval(new Duration(60, TimeUnit.MILLISECONDS));
        // add 10 events
        for (int i : seq(1, 10)) {
            windowManager.add(i);
        }
        Thread.sleep(70);
        assertEquals(seq(1, 10), listener.onActivationEvents);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        listener.clear();
        // wait so all events expire
        Thread.sleep(70);
        assertEquals(seq(1, 10), listener.onActivationExpiredEvents);
        assertTrue(listener.onActivationEvents.isEmpty());
        listener.clear();
        Thread.sleep(70);
        assertTrue(listener.onActivationExpiredEvents.isEmpty());
        assertTrue(listener.onActivationEvents.isEmpty());

    }

    @Test
    public void testTumblingWindow() throws Exception {
        windowManager.setWindowLength(new Count(3));
        windowManager.setSlidingInterval(new Count(3));
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