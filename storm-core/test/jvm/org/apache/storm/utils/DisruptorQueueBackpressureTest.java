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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Assert;
import org.junit.Test;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptorQueueBackpressureTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(DisruptorQueueBackpressureTest.class);

    private final static int MESSAGES = 100;
    private final static int CAPACITY = 128;
    private final static double HIGH_WATERMARK = 0.6;
    private final static double LOW_WATERMARK = 0.2;

    @Test
    public void testBackPressureCallback() throws Exception {

        final DisruptorQueue queue = createQueue("testBackPressure", CAPACITY);
        queue.setEnableBackpressure(true);
        queue.setHighWaterMark(HIGH_WATERMARK);
        queue.setLowWaterMark(LOW_WATERMARK);

        final AtomicBoolean throttleOn = new AtomicBoolean(false);
        // we need to record the cursor because the DisruptorQueue does not update the readPos during batch consuming
        final AtomicLong consumerCursor = new AtomicLong(-1);

        DisruptorBackpressureCallbackImpl cb = new DisruptorBackpressureCallbackImpl(queue, throttleOn, consumerCursor);
        queue.registerBackpressureCallback(cb);

        for (int i = 0; i < MESSAGES; i++) {
            queue.publish(String.valueOf(i));
        }


        queue.consumeBatchWhenAvailable(new EventHandler<Object>() {
            @Override
            public void onEvent(Object o, long l, boolean b) throws Exception {
                 consumerCursor.set(l);
            }
        });


        Assert.assertEquals("Check the calling time of throttle on. ",
                queue.getHighWaterMark(), cb.highWaterMarkCalledPopulation);
        Assert.assertEquals("Checking the calling time of throttle off. ",
                queue.getLowWaterMark(), cb.lowWaterMarkCalledPopulation);
    }

    class DisruptorBackpressureCallbackImpl implements DisruptorBackpressureCallback {
        // the queue's population when the high water mark callback is called for the first time
        public long highWaterMarkCalledPopulation = -1;
        // the queue's population when the low water mark callback is called for the first time
        public long lowWaterMarkCalledPopulation = -1;

        DisruptorQueue queue;
        AtomicBoolean throttleOn;
        AtomicLong consumerCursor;

        public DisruptorBackpressureCallbackImpl(DisruptorQueue queue, AtomicBoolean throttleOn,
                                                 AtomicLong consumerCursor) {
            this.queue = queue;
            this.throttleOn = throttleOn;
            this.consumerCursor = consumerCursor;
        }

        @Override
        public void highWaterMark() throws Exception {
            if (!throttleOn.get()) {
                highWaterMarkCalledPopulation = queue.getMetrics().population() + queue.getMetrics().overflow();
                throttleOn.set(true);
            }
        }

        @Override
        public void lowWaterMark() throws Exception {
             if (throttleOn.get()) {
                 lowWaterMarkCalledPopulation = queue.getMetrics().writePos() - consumerCursor.get() + queue.getMetrics().overflow();
                 throttleOn.set(false);
             }
        }
    }

    private static DisruptorQueue createQueue(String name, int queueSize) {
        return new DisruptorQueue(name, ProducerType.MULTI, queueSize, 0L, 1, 1L);
    }
}
