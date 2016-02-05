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

import org.apache.storm.multilang.BoltMsg;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ShellBoltMessageQueueTest extends TestCase {
    @Test
    public void testPollTaskIdsFirst() throws InterruptedException {
        ShellBoltMessageQueue queue = new ShellBoltMessageQueue();

        // put bolt message first, then put task ids
        queue.putBoltMsg(new BoltMsg());
        ArrayList<Integer> taskIds = Lists.newArrayList(1, 2, 3);
        queue.putTaskIds(taskIds);

        Object msg = queue.poll(10, TimeUnit.SECONDS);

        // task ids should be pulled first
        assertTrue(msg instanceof List<?>);
        assertEquals(msg, taskIds);
    }

    @Test
    public void testPollWhileThereAreNoDataAvailable() throws InterruptedException {
        ShellBoltMessageQueue queue = new ShellBoltMessageQueue();

        long start = System.currentTimeMillis();
        Object msg = queue.poll(1, TimeUnit.SECONDS);
        long finish = System.currentTimeMillis();
        long waitDuration = finish - start;

        assertNull(msg);
        assertTrue("wait duration should be equal or greater than 1000, current: " + waitDuration, waitDuration >= 1000);
    }

    @Test
    public void testPollShouldReturnASAPWhenDataAvailable() throws InterruptedException {
        final ShellBoltMessageQueue queue = new ShellBoltMessageQueue();
        final List<Integer> taskIds = Lists.newArrayList(1, 2, 3);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // NOOP
                }

                queue.putTaskIds(taskIds);
            }
        });
        t.start();

        long start = System.currentTimeMillis();
        Object msg = queue.poll(10, TimeUnit.SECONDS);
        long finish = System.currentTimeMillis();

        assertEquals(msg, taskIds);
        assertTrue(finish - start < (10 * 1000));
    }
}
