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
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class StormTimer {
    private static final Logger LOG = LoggerFactory.getLogger(StormTimer.class);

    public interface TimerFunc {
        public void run(Object o);
    }

    public static class QueueEntry {
        public final Long endTimeMs;
        public final TimerFunc afn;
        public final String id;

        public QueueEntry(Long endTimeMs, TimerFunc afn, String id) {
            this.endTimeMs = endTimeMs;
            this.afn = afn;
            this.id = id;
        }

        @Override
        public String toString() {
            return this.id + " " + this.endTimeMs + " " + this.afn;
        }
    }

    public static class StormTimerTask extends Thread {

        private PriorityBlockingQueue<QueueEntry> queue = new PriorityBlockingQueue<QueueEntry>(10, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((QueueEntry)o1).endTimeMs.intValue() - ((QueueEntry)o2).endTimeMs.intValue();
            }
        });

        private AtomicBoolean active = new AtomicBoolean(false);

        private TimerFunc onKill;

        private Random random = new Random();

        private Semaphore cancelNotifier = new Semaphore(0);

        private Object lock = new Object();

        @Override
        public void run() {
            LOG.info("in run...{}", this.getName());
            while (this.active.get()) {
                QueueEntry queueEntry = null;
                try {
                    synchronized (this.lock) {
                        queueEntry = this.queue.peek();
                    }
                    LOG.info("event: {} -- {}", this.getName(), queueEntry);

                    if ((queueEntry != null) && (Time.currentTimeMillis() >= queueEntry.endTimeMs)) {
                        synchronized (this.lock) {
                            this.queue.poll();
                        }
                        queueEntry.afn.run(null);
                    } else if (queueEntry != null) {
                        Time.sleep(Math.min(1000, (queueEntry.endTimeMs - Time.currentTimeMillis())));
                    } else {
                        Time.sleep(1000);
                    }
                } catch (Throwable t) {
                    if (!(Utils.exceptionCauseIsInstanceOf(InterruptedException.class, t))) {
                        LOG.info("Exception throw for event: {} --- {}", queueEntry, t);
                        this.onKill.run(t);
                        this.setActive(false);
                        throw new RuntimeException(t);
                    }
                }
            }
            this.cancelNotifier.release();
        }

        public void setOnKillFunc(TimerFunc onKill) {
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

    public static StormTimerTask mkTimer(String name, TimerFunc onKill) {
        if (onKill == null) {
            throw new RuntimeException("onKill func is null!");
        }
        StormTimerTask task  = new StormTimerTask();
        if (name == null) {
            task.setName("timer");
        } else {
            task.setName(name);
        }
        task.setOnKillFunc(onKill);
        task.setActive(true);

        task.setDaemon(true);
        task.setPriority(Thread.MAX_PRIORITY);
        task.start();
        return task;
    }
    public static void schedule(StormTimerTask task, int delaySecs, TimerFunc afn, boolean checkActive, int jitterMs) {
        if (task == null) {
            throw new RuntimeException("task is null!");
        }
        if (afn == null) {
            throw new RuntimeException("function to schedule is null!");
        }
        String id = Utils.uuid();
        long endTimeMs = Time.currentTimeMillis() + Time.secsToMillisLong(delaySecs);
        if (jitterMs > 0) {
            endTimeMs = task.random.nextInt(jitterMs) + endTimeMs;
        }
        LOG.info("add event: {}-{}-{}", id, endTimeMs, afn);
        synchronized (task.lock) {
            task.add(new QueueEntry(endTimeMs, afn, id));
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
        if (task == null) {
            throw new RuntimeException("task is null!");
        }
        if (!task.isActive()) {
            throw new IllegalStateException("Timer is not active");
        }
    }

    public static void cancelTimer(StormTimerTask task) throws InterruptedException {
        if (task == null) {
            throw new RuntimeException("task is null!");
        }
        LOG.info("cancel task: {} - {} - {}", task.getName(), task.getId(), task.queue);

        checkActive(task);
        synchronized (task.lock) {
            task.setActive(false);
            task.interrupt();
        }
        task.cancelNotifier.acquire();
    }

    public static boolean isTimerWaiting(StormTimerTask task) {
        if (task == null) {
            throw new RuntimeException("task is null!");
        }
        return Time.isThreadWaiting(task);
    }
}
