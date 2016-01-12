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

import java.util.Map;
import java.util.Map.Entry;

/**
 * Expires keys that have not been updated in the configured number of seconds.
 * The algorithm used will take between expirationSecs and
 * expirationSecs * (1 + 1 / (numBuckets-1)) to actually expire the message.
 *
 * get, put, remove, containsKey, and size take O(numBuckets) time to run.
 *
 * The advantage of this design is that the expiration thread only locks the object
 * for O(1) time, meaning the object is essentially always available for gets/puts.
 */
//deprecated in favor of non-threaded RotatingMap
@Deprecated
public class TimeCacheMap<K, V> {
    //this default ensures things expire at most 50% past the expiration time
    private static final int DEFAULT_NUM_BUCKETS = 3;

    public interface ExpiredCallback<K, V> {
        void expire(K key, V val);
    }

    private final RotatingMap<K, V> _rotatingMap;

    private final Object _lock = new Object();
    private final Thread _cleaner;
    private ExpiredCallback<K, V> _callback;
    
    public TimeCacheMap(int expirationSecs, int numBuckets, ExpiredCallback<K, V> callback) {

        _rotatingMap = new RotatingMap<>(numBuckets);

        _callback = callback;
        final long expirationMillis = expirationSecs * 1000L;
        final long sleepTime = expirationMillis / (numBuckets-1);
        _cleaner = new Thread(new Runnable() {
            public void run() {
                try {
                    while(true) {
                        Map<K, V> dead = null;
                        Time.sleep(sleepTime);
                        synchronized(_lock) {
                            dead = _rotatingMap.rotate();
                        }
                        if(_callback!=null) {
                            for(Entry<K, V> entry: dead.entrySet()) {
                                _callback.expire(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                } catch (InterruptedException ex) {

                }
            }
        });
        _cleaner.setDaemon(true);
        _cleaner.start();
    }

    public TimeCacheMap(int expirationSecs, ExpiredCallback<K, V> callback) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS, callback);
    }

    public TimeCacheMap(int expirationSecs) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS);
    }

    public TimeCacheMap(int expirationSecs, int numBuckets) {
        this(expirationSecs, numBuckets, null);
    }

    public boolean containsKey(K key) {
        synchronized(_lock) {
            return _rotatingMap.containsKey(key);
        }
    }

    public V get(K key) {
        synchronized(_lock) {
            return _rotatingMap.get(key);
        }
    }

    public void put(K key, V value) {
        synchronized(_lock) {
            _rotatingMap.put(key, value);
        }
    }
    
    public Object remove(K key) {
        synchronized(_lock) {
            return _rotatingMap.remove(key);
        }
    }

    public int size() {
        synchronized(_lock) {
            return _rotatingMap.size();
        }
    }

    public void cleanup() {
        _cleaner.interrupt();
    }    
}
