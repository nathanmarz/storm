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

import org.apache.storm.callback.IRunnableCallback;
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

    private LinkedBlockingQueue<IRunnableCallback> queue = new LinkedBlockingQueue<IRunnableCallback>();

    public EventManagerImp(boolean daemon) {
        added = new AtomicInteger();
        processed = new AtomicInteger();
        running = new AtomicBoolean(true);
        runner = new Thread() {
            @Override
            public void run() {
                while (running.get()) {
                    try {
                        IRunnableCallback r = queue.take();
                        if (r == null) {
                            return;
                        }

                        r.run();
                        proccessinc();
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
        runner.setDaemon(daemon);
        runner.start();
    }

    public void proccessinc() {
        processed.incrementAndGet();
    }

    @Override
    public void add(IRunnableCallback eventFn) {
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

    public void shutdown() {
        try {
            running.set(false);
            runner.interrupt();
            runner.join();
        } catch (InterruptedException e) {
            throw Utils.wrapInRuntime(e);
        }
    }
}
