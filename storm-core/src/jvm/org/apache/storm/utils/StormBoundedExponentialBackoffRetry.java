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

import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class StormBoundedExponentialBackoffRetry extends BoundedExponentialBackoffRetry {
    private static final Logger LOG = LoggerFactory.getLogger(StormBoundedExponentialBackoffRetry.class);
    private int stepSize;
    private int expRetriesThreshold;
    private final Random random = new Random();
    private final int linearBaseSleepMs;

    /**
     * The class provides generic exponential-linear backoff retry strategy for
     * storm. It calculates threshold for exponentially increasing sleeptime
     * for retries. Beyond this threshold, the sleeptime increase is linear.
     *
     * Also adds jitter for exponential/linear retry.
     * It guarantees `currSleepTimeMs >= prevSleepTimeMs` and
     * `baseSleepTimeMs <= currSleepTimeMs <= maxSleepTimeMs`
     */

    public StormBoundedExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepTimeMs, int maxRetries) {
        super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
        expRetriesThreshold = 1;
        while ((1 << (expRetriesThreshold + 1)) < ((maxSleepTimeMs - baseSleepTimeMs) / 2)) {
            expRetriesThreshold++;
        }
        LOG.debug("The baseSleepTimeMs [{}] the maxSleepTimeMs [{}] the maxRetries [{}]", 
				baseSleepTimeMs, maxSleepTimeMs, maxRetries);
        if (baseSleepTimeMs > maxSleepTimeMs) {
            LOG.warn("Misconfiguration: the baseSleepTimeMs [" + baseSleepTimeMs + "] can't be greater than " +
                    "the maxSleepTimeMs [" + maxSleepTimeMs + "].");
        }
        if( maxRetries > 0 && maxRetries > expRetriesThreshold ) {
            this.stepSize = Math.max(1, (maxSleepTimeMs - (1 << expRetriesThreshold)) / (maxRetries - expRetriesThreshold));
        } else {
            this.stepSize = 1;
	}
        this.linearBaseSleepMs = super.getBaseSleepTimeMs() + (1 << expRetriesThreshold);
    }

    @Override
    public int getSleepTimeMs(int retryCount, long elapsedTimeMs) {
        if (retryCount < expRetriesThreshold) {
            int exp = 1 << retryCount;
            int jitter = random.nextInt(exp);
            int sleepTimeMs = super.getBaseSleepTimeMs() + exp + jitter;
            return sleepTimeMs;
        } else {
            int stepJitter = random.nextInt(stepSize);
            return Math.min(super.getMaxSleepTimeMs(), (linearBaseSleepMs +
                    (stepSize * (retryCount - expRetriesThreshold)) + stepJitter));
        }
    }
}
