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

import junit.framework.TestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormBoundedExponentialBackoffRetryTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(StormBoundedExponentialBackoffRetryTest.class);

    @Test
    public void testExponentialSleepLargeRetries() throws Exception {
        int baseSleepMs = 10;
        int maxSleepMs = 1000;
        int maxRetries = 900;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);

    }

    @Test
    public void testExponentialSleep() throws Exception {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 40;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    @Test
    public void testExponentialSleepSmallMaxRetries() throws Exception {
        int baseSleepMs = 1000;
        int maxSleepMs = 5000;
        int maxRetries = 10;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    @Test
    public void testExponentialSleepZeroMaxTries() throws Exception {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 0;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    @Test
    public void testExponentialSleepSmallMaxTries() throws Exception {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 10;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    private void validateSleepTimes(int baseSleepMs, int maxSleepMs, int maxRetries) {
        StormBoundedExponentialBackoffRetry retryPolicy = new StormBoundedExponentialBackoffRetry(baseSleepMs, maxSleepMs, maxRetries);
        int retryCount = 0;
        int prevSleepMs = 0;
        LOG.info("The baseSleepMs [" + baseSleepMs + "] the maxSleepMs [" + maxSleepMs +
                "] the maxRetries [" + maxRetries + "]");
        while (retryCount <= maxRetries) {
            int currSleepMs = retryPolicy.getSleepTimeMs(retryCount, 0);
            LOG.info("For retryCount [" + retryCount + "] the previousSleepMs [" + prevSleepMs +
                    "] the currentSleepMs [" + currSleepMs + "]");
            assertTrue("For retryCount [" + retryCount + "] the previousSleepMs [" + prevSleepMs +
                            "] is not less than currentSleepMs [" + currSleepMs + "]",
                    (prevSleepMs < currSleepMs) || (currSleepMs == maxSleepMs));
            assertTrue("For retryCount [" + retryCount + "] the currentSleepMs [" + currSleepMs +
                            "] is less than baseSleepMs [" + baseSleepMs + "].",
                    (baseSleepMs <= currSleepMs) || (currSleepMs == maxSleepMs));
            assertTrue("For retryCount [" + retryCount + "] the currentSleepMs [" + currSleepMs +
                            "] is greater than maxSleepMs [" + maxSleepMs + "]",
                    maxSleepMs >= currSleepMs);
            prevSleepMs = currSleepMs;
            retryCount++;
        }
        int badRetryCount = maxRetries + 10;
        int currSleepMs = retryPolicy.getSleepTimeMs(badRetryCount, 0);
        LOG.info("For badRetryCount [" + badRetryCount + "] the previousSleepMs [" + prevSleepMs +
                "] the currentSleepMs [" + currSleepMs + "]");
        assertTrue("For the badRetryCount [" + badRetryCount + "] that's greater than maxRetries [" +
                        maxRetries + "]the currentSleepMs [" + currSleepMs + "] " +
                        "is greater than maxSleepMs [" + maxSleepMs + "]",
                maxSleepMs >= currSleepMs);
    }
}

