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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class StormTimer {
    private static final Logger LOG = LoggerFactory.getLogger(StormTimer.class);

    public interface TimerFunc {
        public void run(Object o);
    }

    public static class StormTimerTask extends Thread {

        private PriorityBlockingQueue<Long> queue = new PriorityBlockingQueue<Long>(10, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return 0;
            }
        });

        private AtomicBoolean active = new AtomicBoolean(false);

        private TimerFunc onKill;

        private TimerFunc afn;

        private Random random = new Random();

        private Semaphore cancelNotifier = new Semaphore(0);

        private Object lock = new Object();

        @Override
        public void run() {
            LOG.info("in run...");
            while (this.active.get()) {
                try {
                    Long endTimeMillis;
                    synchronized (this.lock) {
                        endTimeMillis = this.queue.peek();
                    }
                    if ((endTimeMillis != null) && (currentTimeMillis() >= endTimeMillis)) {
                        synchronized (this.lock) {
                            this.queue.poll();
                        }
                        LOG.info("About to run function...");
                        this.afn.run(null);
                    } else if (endTimeMillis != null) {
                        Time.sleep(Math.min(1000, (endTimeMillis - currentTimeMillis())));
                    } else {
                        Time.sleep(1000);
                    }
                } catch (Throwable t) {
                    this.onKill.run(t);
                }
            }
            this.cancelNotifier.release();
        }

        public void setOnKillFunc(TimerFunc onKill) {
            this.onKill = onKill;
        }

        public void setFunc(TimerFunc func) {
            this.afn = func;
        }

        public void setActive(boolean flag) {
            this.active.set(flag);
        }

        public boolean isActive() {
            return this.active.get();
        }

        public void add(long endTime) {
            this.queue.add(endTime);
        }
    }

    public static StormTimerTask mkTimer(TimerFunc onKill, String name) {
        LOG.info("making Timer...");
        StormTimerTask task  = new StormTimerTask();
        task.setOnKillFunc(onKill);
        task.setActive(true);

        task.setDaemon(true);
        task.setPriority(Thread.MAX_PRIORITY);
        task.start();
        return task;
    }
    public static void schedule(StormTimerTask task, int delaySecs, TimerFunc afn, boolean checkActive, int jitterMs) {
        long endTimeMs = currentTimeMillis() + secsToMillisLong(delaySecs);
        if (jitterMs > 0) {
            endTimeMs = task.random.nextInt(jitterMs) + endTimeMs;
        }
        task.setFunc(afn);
        synchronized (task.lock) {
            task.add(endTimeMs);
        }
    }
    public static void schedule(StormTimerTask task, int delaySecs, TimerFunc afn) {
        schedule(task, delaySecs, afn, true, 0);
    }

    public static void scheduleRecurring(final StormTimerTask task, int delaySecs, final int recurSecs, final TimerFunc afn) {
        schedule(task, delaySecs, new TimerFunc() {
            @Override
            public void run(Object o) {
                LOG.info("scheduleRecurring running...");
                afn.run(null);
                LOG.info("scheduleRecurring schedule again...");

                schedule(task, recurSecs, this, false, 0);
            }
        });
    }

    public static void scheduleRecurringWithJitter(final StormTimerTask task, int delaySecs, final int recurSecs, final int jitterMs, final TimerFunc afn) {
        schedule(task, delaySecs, new TimerFunc() {
            @Override
            public void run(Object o) {
                LOG.info("scheduleRecurringWithJitter running...");
                afn.run(null);
                LOG.info("scheduleRecurringWithJitter schedule again...");

                schedule(task, recurSecs, this, false, jitterMs);
            }
        });
    }

    public static void checkActive(StormTimerTask task) {
        if (!task.isActive()) {
            throw new IllegalStateException("Timer is not active");
        }
    }

    public static void cancelTimer(StormTimerTask task) throws InterruptedException {
        checkActive(task);
        synchronized (task.lock) {
            task.setActive(false);
            task.interrupt();
        }
        task.cancelNotifier.acquire();
    }

    public static boolean isTimerWaiting(StormTimerTask task) {
        return Time.isThreadWaiting(task);
    }

    /**
     * function in util that haven't be translated to java
     */

    public static long secsToMillisLong(long secs) {
        return secs * 1000;
    }

    public static long currentTimeMillis() {
        return Time.currentTimeMillis();
    }


    public static void main(String[] argv) {
        mkTimer(new TimerFunc() {
            @Override
            public void run(Object o) {

            }
        }, "erer");
    }

}
