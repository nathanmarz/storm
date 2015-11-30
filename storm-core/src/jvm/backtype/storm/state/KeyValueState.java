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
package backtype.storm.state;

/**
 * A state that supports key-value mappings.
 */
public interface KeyValueState<K, V> extends State {
    /**
     * Maps the value with the key
     *
     * @param key   the key
     * @param value the value
     */
    void put(K key, V value);

    /**
     * Returns the value mapped to the key
     *
     * @param key the key
     * @return the value or null if no mapping is found
     */
    V get(K key);

    /**
     * Returns the value mapped to the key or defaultValue if no mapping is found.
     *
     * @param key          the key
     * @param defaultValue the value to return if no mapping is found
     * @return the value or defaultValue if no mapping is found
     */
    V get(K key, V defaultValue);
}
