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

package org.apache.storm;

import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The timer defined in this file is very similar to java.util.Timer, except
 * it integrates with Storm's time simulation capabilities. This lets us test
 * code that does asynchronous work on the timer thread
 */

public class StormTimer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StormTimer.class);

    public static class QueueEntry {
        public final Long endTimeMs;
        public final Runnable func;
        public final String id;

        public QueueEntry(Long endTimeMs, Runnable func, String id) {
            this.endTimeMs = endTimeMs;
            this.func = func;
            this.id = id;
        }
    }

    public static class StormTimerTask extends Thread {

        //initialCapacity set to 11 since its the default inital capacity of PriorityBlockingQueue
        private PriorityBlockingQueue<QueueEntry> queue = new PriorityBlockingQueue<QueueEntry>(11, new Comparator<QueueEntry>() {
            @Override
            public int compare(QueueEntry o1, QueueEntry o2) {
                return o1.endTimeMs.intValue() - o2.endTimeMs.intValue();
            }
        });

        // boolean to indicate whether timer is active
        private AtomicBoolean active = new AtomicBoolean(false);

        // function to call when timer is killed
        private Thread.UncaughtExceptionHandler onKill;

        //random number generator
        private Random random = new Random();

        @Override
        public void run() {
            while (this.active.get()) {
                QueueEntry queueEntry = null;
                try {
                    queueEntry = this.queue.peek();
                    if ((queueEntry != null) && (Time.currentTimeMillis() >= queueEntry.endTimeMs)) {
                        // It is imperative to not run the function
                        // inside the timer lock. Otherwise, it is
                        // possible to deadlock if the fn deals with
                        // other locks, like the submit lock.
                        this.queue.remove(queueEntry);
                        queueEntry.func.run();
                    } else if (queueEntry != null) {
                        //  If any events are scheduled, sleep until
                        // event generation. If any recurring events
                        // are scheduled then we will always go
                        // through this branch, sleeping only the
                        // exact necessary amount of time. We give
                        // an upper bound, e.g. 1000 millis, to the
                        // sleeping time, to limit the response time
                        // for detecting any new event within 1 secs.
                        Time.sleep(Math.min(1000, (queueEntry.endTimeMs - Time.currentTimeMillis())));
                    } else {
                        // Otherwise poll to see if any new event
                        // was scheduled. This is, in essence, the
                        // response time for detecting any new event
                        // schedulings when there are no scheduled
                        // events.
                        Time.sleep(1000);
                    }
                } catch (Throwable e) {
                    if (!(Utils.exceptionCauseIsInstanceOf(InterruptedException.class, e))) {
                        this.onKill.uncaughtException(this, e);
                        this.setActive(false);
                    }
                }
            }
        }

        public void setOnKillFunc(Thread.UncaughtExceptionHandler onKill) {
            this.onKill = onKill;
        }

        public void setActive(boolean flag) {
            this.active.set(flag);
        }

        public boolean isActive() {
            return this.active.get();
        }

        public void add(QueueEntry queueEntry) {
            this.queue.add(queueEntry);
        }
    }

    //task to run
    private StormTimerTask task = new StormTimerTask();

    /**
     * Makes a Timer in the form of a StormTimerTask Object
     * @param name name of the timer
     * @param onKill function to call when timer is killed unexpectedly
     * @return StormTimerTask object that was initialized
     */
    public StormTimer (String name, Thread.UncaughtExceptionHandler onKill) {
        if (onKill == null) {
            throw new RuntimeException("onKill func is null!");
        }
        if (name == null) {
            this.task.setName("timer");
        } else {
            this.task.setName(name);
        }
        this.task.setOnKillFunc(onKill);
        this.task.setActive(true);

        this.task.setDaemon(true);
        this.task.setPriority(Thread.MAX_PRIORITY);
        this.task.start();
    }

    /**
     * Schedule a function to be executed in the timer
     * @param delaySecs the number of seconds to delay before running the function
     * @param func the function to run
     * @param checkActive whether to check is the timer is active
     * @param jitterMs add jitter to the run
     */
    public void schedule(int delaySecs, Runnable func, boolean checkActive, int jitterMs) {
        if (func == null) {
            throw new RuntimeException("function to schedule is null!");
        }
        if (checkActive) {
            checkActive();
        }
        String id = Utils.uuid();
        long endTimeMs = Time.currentTimeMillis() + Time.secsToMillisLong(delaySecs);
        if (jitterMs > 0) {
            endTimeMs = this.task.random.nextInt(jitterMs) + endTimeMs;
        }
        task.add(new QueueEntry(endTimeMs, func, id));
    }

    public void schedule(int delaySecs, Runnable func) {
        schedule(delaySecs, func, true, 0);
    }

    /**
     * Schedule a function to run recurrently
     * @param delaySecs the number of seconds to delay before running the function
     * @param recurSecs the time between each invocation
     * @param func the function to run
     */
    public void scheduleRecurring(int delaySecs, final int recurSecs, final Runnable func) {
        schedule(delaySecs, new Runnable() {
            @Override
            public void run() {
                func.run();
                // This avoids a race condition with cancel-timer.
                schedule(recurSecs, this, false, 0);
            }
        });
    }

    /**
     * schedule a function to run recurrently with jitter
     * @param delaySecs the number of seconds to delay before running the function
     * @param recurSecs the time between each invocation
     * @param jitterMs jitter added to the run
     * @param func the function to run
     */
    public void scheduleRecurringWithJitter(int delaySecs, final int recurSecs, final int jitterMs, final Runnable func) {
        schedule(delaySecs, new Runnable() {
            @Override
            public void run() {
                func.run();
                // This avoids a race condition with cancel-timer.
                schedule(recurSecs, this, false, jitterMs);
            }
        });
    }

    /**
     * check if timer is active
     */
    private void checkActive() {
        if (!this.task.isActive()) {
            throw new IllegalStateException("Timer is not active");
        }
    }

    /**
     * cancel timer
     */

    @Override
    public void close() throws Exception {
        checkActive();
        this.task.setActive(false);
        this.task.interrupt();
        this.task.join();
    }

    /**
     * is timer waiting. Used in timer simulation
     */
    public boolean isTimerWaiting() {
        return Time.isThreadWaiting(task);
    }
}
