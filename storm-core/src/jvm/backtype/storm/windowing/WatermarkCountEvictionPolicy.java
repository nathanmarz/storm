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

/**
 * An eviction policy that tracks count based on watermark ts and
 * evicts events upto the watermark based on a threshold count.
 *
 * @param <T> the type of event tracked by this policy.
 */
public class WatermarkCountEvictionPolicy<T> extends CountEvictionPolicy<T> {
    private final WindowManager<T> windowManager;
    /*
     * The reference time in millis for window calculations and
     * expiring events. If not set it will default to System.currentTimeMillis()
     */
    private long referenceTime;

    public WatermarkCountEvictionPolicy(int count, WindowManager<T> windowManager) {
        super(count);
        this.windowManager = windowManager;
    }

    @Override
    public Action evict(Event<T> event) {
        if (event.getTimestamp() <= referenceTime) {
            return super.evict(event);
        } else {
            return Action.KEEP;
        }
    }

    @Override
    public void track(Event<T> event) {
        // NOOP
    }

    @Override
    public void setContext(Object context) {
        referenceTime = (Long) context;
        currentCount.set(windowManager.getEventCount(referenceTime));
    }

    @Override
    public String toString() {
        return "WatermarkCountEvictionPolicy{" +
                "referenceTime=" + referenceTime +
                "} " + super.toString();
    }
}
