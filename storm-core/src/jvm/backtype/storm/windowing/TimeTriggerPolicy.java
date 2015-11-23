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

import backtype.storm.topology.FailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Invokes {@link TriggerHandler#onTrigger()} after the duration.
 */
public class TimeTriggerPolicy<T> implements TriggerPolicy<T> {
    private static final Logger LOG = LoggerFactory.getLogger(TimeTriggerPolicy.class);

    private long duration;
    private final TriggerHandler handler;
    private final ScheduledExecutorService executor;
    private final ScheduledFuture<?> executorFuture;

    public TimeTriggerPolicy(long millis, TriggerHandler handler) {
        this.duration = millis;
        this.handler = handler;
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.executorFuture = executor.scheduleAtFixedRate(newTriggerTask(), duration, duration, TimeUnit.MILLISECONDS);
    }

    @Override
    public void track(Event<T> event) {
        checkFailures();
    }

    @Override
    public void reset() {
        checkFailures();
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "TimeTriggerPolicy{" +
                "duration=" + duration +
                '}';
    }

    /*
    * Check for uncaught exceptions during the execution
    * of the trigger and fail fast.
    * The uncaught exceptions will be wrapped in
    * ExecutionException and thrown when future.get() is invoked.
    */
    private void checkFailures() {
        if (executorFuture.isDone()) {
            try {
                executorFuture.get();
            } catch (InterruptedException ex) {
                LOG.error("Got exception ", ex);
                throw new FailedException(ex);
            } catch (ExecutionException ex) {
                LOG.error("Got exception ", ex);
                throw new FailedException(ex.getCause());
            }
        }
    }

    private Runnable newTriggerTask() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    handler.onTrigger();
                } catch (Throwable th) {
                    LOG.error("handler.onTrigger failed ", th);
                    /*
                     * propagate it so that task gets canceled and the exception
                     * can be retrieved from executorFuture.get()
                     */
                    throw th;
                }
            }
        };
    }
}
