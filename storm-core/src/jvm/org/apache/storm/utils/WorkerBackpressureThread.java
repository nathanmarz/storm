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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerBackpressureThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerBackpressureThread.class);
    private Object trigger;
    private Object workerData;
    private WorkerBackpressureCallback callback;
    private volatile boolean running = true;

    public WorkerBackpressureThread(Object trigger, Object workerData, WorkerBackpressureCallback callback) {
        this.trigger = trigger;
        this.workerData = workerData;
        this.callback = callback;
        this.setName("WorkerBackpressureThread");
        this.setDaemon(true);
        this.setUncaughtExceptionHandler(new BackpressureUncaughtExceptionHandler());
    }

    static public void notifyBackpressureChecker(Object trigger) {
        try {
            synchronized (trigger) {
                trigger.notifyAll();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void terminate() throws InterruptedException {
        running = false;
        interrupt();
        join();
    }

    public void run() {
        while (running) {
            try {
                synchronized(trigger) {
                    trigger.wait(100);
                }
                callback.onEvent(workerData); // check all executors and update zk backpressure throttle for the worker if needed
            } catch (InterruptedException interEx) {
                // ignored, we are shutting down.
            }
        }
    }
}

class BackpressureUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BackpressureUncaughtExceptionHandler.class);
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // note that exception that happens during connecting to ZK has been ignored in the callback implementation
        LOG.error("Received error or exception in WorkerBackpressureThread.. terminating the worker...", e);
        Runtime.getRuntime().exit(1);
    }
}
