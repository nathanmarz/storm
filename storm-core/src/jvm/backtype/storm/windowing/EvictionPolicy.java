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
 * Eviction policy tracks events and decides whether
 * an event should be evicted from the window or not.
 *
 * @param <T> the type of event that is tracked.
 */
public interface EvictionPolicy<T> {
    /**
     * Decides if an event should be evicted from the window or not.
     *
     * @param event the input event
     * @return true if the event should be evicted, false otherwise
     */
    boolean evict(Event<T> event);

    /**
     * Tracks the event to later decide whether
     * {@link EvictionPolicy#evict(Event)} should evict it or not.
     *
     * @param event the input event to be tracked
     */
    void track(Event<T> event);
}
