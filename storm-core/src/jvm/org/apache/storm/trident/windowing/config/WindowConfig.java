/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.trident.windowing.config;

import org.apache.storm.trident.windowing.strategy.WindowStrategy;

import java.io.Serializable;

/**
 * Windowing configuration with window and sliding length.
 */
public interface WindowConfig extends Serializable {

    /**
     * Returns the length of the window.
     * @return
     */
    public int getWindowLength();

    /**
     * Returns the sliding length of the moving window.
     * @return
     */
    public int getSlidingLength();

    /**
     * Gives the type of windowing. It can be any of {@code Type} values.
     *
     * @return
     */
    public <T> WindowStrategy<T> getWindowStrategy();

    public void validate();

    public enum Type {
        SLIDING_COUNT,
        TUMBLING_COUNT,
        SLIDING_DURATION,
        TUMBLING_DURATION
    }
}
