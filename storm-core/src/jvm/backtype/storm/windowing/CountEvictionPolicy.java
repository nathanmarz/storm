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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An eviction policy that tracks event counts and can
 * evict based on a threshold count.
 *
 * @param <T> the type of event tracked by this policy.
 */
public class CountEvictionPolicy<T> implements EvictionPolicy<T> {
    private final int threshold;
    private final AtomicInteger currentCount;

    public CountEvictionPolicy(int count) {
        this.threshold = count;
        this.currentCount = new AtomicInteger();
    }

    @Override
    public boolean evict(Event<T> event) {
        /*
         * atomically decrement the count if its greater than threshold and
         * return if the event should be evicted
         */
        while (true) {
            int curVal = currentCount.get();
            if (curVal > threshold) {
                if (currentCount.compareAndSet(curVal, curVal - 1)) {
                    return true;
                }
            } else {
                break;
            }
        }
        return false;
    }

    @Override
    public void track(Event<T> event) {
        currentCount.incrementAndGet();
    }

    @Override
    public String toString() {
        return "CountEvictionPolicy{" +
                "threshold=" + threshold +
                ", currentCount=" + currentCount +
                '}';
    }
}
