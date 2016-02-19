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
package org.apache.storm.event;

import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EventManagerImp implements EventManager {
    private static final Logger LOG = LoggerFactory.getLogger(EventManagerImp.class);

    private AtomicInteger added;
    private AtomicInteger processed;
    private AtomicBoolean running;
    private Thread runner;

    private LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();

    public EventManagerImp(boolean isDaemon) {
        added = new AtomicInteger();
        processed = new AtomicInteger();
        running = new AtomicBoolean(true);
        runner = new Thread() {
            @Override
            public void run() {
                while (running.get()) {
                    try {
                        Runnable r = queue.take();
                        if (r == null) {
                            return;
                        }

                        r.run();
                        proccessInc();
                    } catch (Throwable t) {
                        if (Utils.exceptionCauseIsInstanceOf(InterruptedIOException.class, t)) {
                            LOG.info("Event manager interrupted while doing IO");
                        } else if (Utils.exceptionCauseIsInstanceOf(InterruptedException.class, t)) {
                            LOG.info("Event manager interrupted");
                        } else {
                            LOG.error("{} Error when processing event", t);
                            Utils.exitProcess(20, "Error when processing an event");
                        }
                    }
                }
            }
        };
        runner.setDaemon(isDaemon);
        runner.start();
    }

    public void proccessInc() {
        processed.incrementAndGet();
    }

    @Override
    public void add(Runnable eventFn) {
        if (!running.get()) {
            throw new RuntimeException("Cannot add events to a shutdown event manager");
        }
        added.incrementAndGet();
        queue.add(eventFn);
    }

    @Override
    public boolean waiting() {
        return (Time.isThreadWaiting(runner) || (processed.get() == added.get()));
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        runner.interrupt();
        runner.join();
    }
}
