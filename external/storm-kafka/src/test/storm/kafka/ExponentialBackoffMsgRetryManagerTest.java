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
package storm.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class ExponentialBackoffMsgRetryManagerTest {

    private static final Long TEST_OFFSET = 101L;
    private static final Long TEST_OFFSET2 = 102L;
    private static final Long TEST_OFFSET3 = 105L;
    private static final Long TEST_NEW_OFFSET = 103L;

    @Test
    public void testImmediateRetry() throws Exception {
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(0, 0d, 0);
        manager.failed(TEST_OFFSET);
        Long next = manager.nextFailedMessageToRetry();
        assertEquals("expect test offset next available for retry", TEST_OFFSET, next);
        assertTrue("message should be ready for retry immediately", manager.shouldRetryMsg(TEST_OFFSET));

        manager.retryStarted(TEST_OFFSET);

        manager.failed(TEST_OFFSET);
        next = manager.nextFailedMessageToRetry();
        assertEquals("expect test offset next available for retry", TEST_OFFSET, next);
        assertTrue("message should be ready for retry immediately", manager.shouldRetryMsg(TEST_OFFSET));
    }

    @Test
    public void testSingleDelay() throws Exception {
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(10, 1d, 100);
        manager.failed(TEST_OFFSET);
        Thread.sleep(5);
        Long next = manager.nextFailedMessageToRetry();
        assertNull("expect no message ready for retry yet", next);
        assertFalse("message should not be ready for retry yet", manager.shouldRetryMsg(TEST_OFFSET));

        Thread.sleep(10);
        next = manager.nextFailedMessageToRetry();
        assertEquals("expect test offset next available for retry", TEST_OFFSET, next);
        assertTrue("message should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET));
    }

    @Test
    public void testExponentialBackoff() throws Exception {
        final long initial = 10;
        final double mult = 2d;
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(initial, mult, initial * 10);

        long expectedWaitTime = initial;
        for (long i = 0L; i < 3L; ++i) {
            manager.failed(TEST_OFFSET);

            Thread.sleep((expectedWaitTime + 1L) / 2L);
            assertFalse("message should not be ready for retry yet", manager.shouldRetryMsg(TEST_OFFSET));

            Thread.sleep((expectedWaitTime + 1L) / 2L);
            Long next = manager.nextFailedMessageToRetry();
            assertEquals("expect test offset next available for retry", TEST_OFFSET, next);
            assertTrue("message should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET));

            manager.retryStarted(TEST_OFFSET);
            expectedWaitTime *= mult;
        }
    }

    @Test
    public void testRetryOrder() throws Exception {
        final long initial = 10;
        final double mult = 2d;
        final long max = 20;
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(initial, mult, max);

        manager.failed(TEST_OFFSET);
        Thread.sleep(initial);

        manager.retryStarted(TEST_OFFSET);
        manager.failed(TEST_OFFSET);
        manager.failed(TEST_OFFSET2);

        // although TEST_OFFSET failed first, it's retry delay time is longer b/c this is the second retry
        // so TEST_OFFSET2 should come first

        Thread.sleep(initial * 2);
        assertTrue("message "+TEST_OFFSET+"should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET));
        assertTrue("message "+TEST_OFFSET2+"should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET2));

        Long next = manager.nextFailedMessageToRetry();
        assertEquals("expect first message to retry is "+TEST_OFFSET2, TEST_OFFSET2, next);

        Thread.sleep(initial);

        // haven't retried yet, so first should still be TEST_OFFSET2
        next = manager.nextFailedMessageToRetry();
        assertEquals("expect first message to retry is "+TEST_OFFSET2, TEST_OFFSET2, next);
        manager.retryStarted(next);

        // now it should be TEST_OFFSET
        next = manager.nextFailedMessageToRetry();
        assertEquals("expect message to retry is now "+TEST_OFFSET, TEST_OFFSET, next);
        manager.retryStarted(next);

        // now none left
        next = manager.nextFailedMessageToRetry();
        assertNull("expect no message to retry now", next);
    }

    @Test
    public void testQueriesAfterRetriedAlready() throws Exception {
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(0, 0d, 0);
        manager.failed(TEST_OFFSET);
        Long next = manager.nextFailedMessageToRetry();
        assertEquals("expect test offset next available for retry", TEST_OFFSET, next);
        assertTrue("message should be ready for retry immediately", manager.shouldRetryMsg(TEST_OFFSET));

        manager.retryStarted(TEST_OFFSET);
        next = manager.nextFailedMessageToRetry();
        assertNull("expect no message ready after retried", next);
        assertFalse("message should not be ready after retried", manager.shouldRetryMsg(TEST_OFFSET));
    }

    @Test(expected = IllegalStateException.class)
    public void testRetryWithoutFail() throws Exception {
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(0, 0d, 0);
        manager.retryStarted(TEST_OFFSET);
    }

    @Test(expected = IllegalStateException.class)
    public void testFailRetryRetry() throws Exception {
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(0, 0d, 0);
        manager.failed(TEST_OFFSET);
        try {
            manager.retryStarted(TEST_OFFSET);
        } catch (IllegalStateException ise) {
            fail("IllegalStateException unexpected here: " + ise);
        }

        assertFalse("message should not be ready for retry", manager.shouldRetryMsg(TEST_OFFSET));
        manager.retryStarted(TEST_OFFSET);
    }

    @Test
    public void testMaxBackoff() throws Exception {
        final long initial = 10;
        final double mult = 2d;
        final long max = 20;
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(initial, mult, max);

        long expectedWaitTime = initial;
        for (long i = 0L; i < 4L; ++i) {
            manager.failed(TEST_OFFSET);

            Thread.sleep((expectedWaitTime + 1L) / 2L);
            assertFalse("message should not be ready for retry yet", manager.shouldRetryMsg(TEST_OFFSET));

            Thread.sleep((expectedWaitTime + 1L) / 2L);
            Long next = manager.nextFailedMessageToRetry();
            assertEquals("expect test offset next available for retry", TEST_OFFSET, next);
            assertTrue("message should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET));

            manager.retryStarted(TEST_OFFSET);
            expectedWaitTime = Math.min((long) (expectedWaitTime * mult), max);
        }
    }

    @Test
    public void testFailThenAck() throws Exception {
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(0, 0d, 0);
        manager.failed(TEST_OFFSET);
        assertTrue("message should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET));

        manager.acked(TEST_OFFSET);

        Long next = manager.nextFailedMessageToRetry();
        assertNull("expect no message ready after acked", next);
        assertFalse("message should not be ready after acked", manager.shouldRetryMsg(TEST_OFFSET));
    }

    @Test
    public void testAckThenFail() throws Exception {
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(0, 0d, 0);
        manager.acked(TEST_OFFSET);
        assertFalse("message should not be ready after acked", manager.shouldRetryMsg(TEST_OFFSET));

        manager.failed(TEST_OFFSET);

        Long next = manager.nextFailedMessageToRetry();
        assertEquals("expect test offset next available for retry", TEST_OFFSET, next);
        assertTrue("message should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET));
    }
    
    @Test
    public void testClearInvalidMessages() throws Exception {
        ExponentialBackoffMsgRetryManager manager = new ExponentialBackoffMsgRetryManager(0, 0d, 0);
        manager.failed(TEST_OFFSET);
        manager.failed(TEST_OFFSET2);
        manager.failed(TEST_OFFSET3);
        
        assertTrue("message should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET));
        assertTrue("message should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET2));
        assertTrue("message should be ready for retry", manager.shouldRetryMsg(TEST_OFFSET3));

        manager.clearInvalidMessages(TEST_NEW_OFFSET);

        Long next = manager.nextFailedMessageToRetry();
        assertEquals("expect test offset next available for retry", TEST_OFFSET3, next);
        
        manager.acked(TEST_OFFSET3);
        next = manager.nextFailedMessageToRetry();
        assertNull("expect no message ready after acked", next);
    }

}
