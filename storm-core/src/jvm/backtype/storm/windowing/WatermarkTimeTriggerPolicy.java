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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles watermark events and triggers {@link TriggerHandler#onTrigger()} for each window
 * interval that has events to be processed up to the watermark ts.
 */
public class WatermarkTimeTriggerPolicy<T> implements TriggerPolicy<T> {
    private static final Logger LOG = LoggerFactory.getLogger(WatermarkTimeTriggerPolicy.class);
    private final long slidingIntervalMs;
    private final TriggerHandler handler;
    private final EvictionPolicy<T> evictionPolicy;
    private final WindowManager<T> windowManager;
    private long nextWindowEndTs = 0;

    public WatermarkTimeTriggerPolicy(long slidingIntervalMs, TriggerHandler handler, EvictionPolicy<T> evictionPolicy,
                                      WindowManager<T> windowManager) {
        this.slidingIntervalMs = slidingIntervalMs;
        this.handler = handler;
        this.evictionPolicy = evictionPolicy;
        this.windowManager = windowManager;
    }

    @Override
    public void track(Event<T> event) {
        if (event.isWatermark()) {
            handleWaterMarkEvent(event);
        }
    }

    @Override
    public void reset() {
        // NOOP
    }

    @Override
    public void shutdown() {
        // NOOP
    }

    /**
     * Invokes the trigger all pending windows up to the
     * watermark timestamp. The end ts of the window is set
     * in the eviction policy context so that the events falling
     * within that window can be processed.
     */
    private void handleWaterMarkEvent(Event<T> event) {
        long watermarkTs = event.getTimestamp();
        long windowEndTs = nextWindowEndTs;
        LOG.debug("Window end ts {} Watermark ts {}", windowEndTs, watermarkTs);
        while (windowEndTs <= watermarkTs) {
            evictionPolicy.setContext(windowEndTs);
            if (handler.onTrigger()) {
                windowEndTs += slidingIntervalMs;
            } else {
                /*
                 * No events were found in the previous window interval.
                 * Scan through the events in the queue to find the next
                 * window intervals based on event ts.
                 */
                long ts = getNextAlignedWindowTs(windowEndTs, watermarkTs);
                LOG.debug("Next aligned window end ts {}", ts);
                if (ts == Long.MAX_VALUE) {
                    LOG.debug("No events to process between {} and watermark ts {}", windowEndTs, watermarkTs);
                    break;
                }
                windowEndTs = ts;
            }
        }
        nextWindowEndTs = windowEndTs;
    }

    /**
     * Computes the next window by scanning the events in the window and
     * finds the next aligned window between the startTs and endTs. Return the end ts
     * of the next aligned window, i.e. the ts when the window should fire.
     *
     * @param startTs the start timestamp (excluding)
     * @param endTs the end timestamp (including)
     * @return the aligned window end ts for the next window or Long.MAX_VALUE if there
     * are no more events to be processed.
     */
    private long getNextAlignedWindowTs(long startTs, long endTs) {
        long nextTs = windowManager.getEarliestEventTs(startTs, endTs);
        if (nextTs == Long.MAX_VALUE || (nextTs % slidingIntervalMs == 0)) {
            return nextTs;
        }
        return nextTs + (slidingIntervalMs - (nextTs % slidingIntervalMs));
    }
}
