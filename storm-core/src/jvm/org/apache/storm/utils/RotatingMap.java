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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
 *
 * Note: This class is not thread-safe since it does not protect against changes to
 * _buckets while it is being read
 *
 */
public class RotatingMap<K, V> {
    //this default ensures things expire at most 50% past the expiration time
    private static final int DEFAULT_NUM_BUCKETS = 3;

    public interface ExpiredCallback<K, V> {
        void expire(K key, V val);
    }

    private final LinkedList<HashMap<K, V>> _buckets;

    private final ExpiredCallback<K, V> _callback;
    
    public RotatingMap(int numBuckets, ExpiredCallback<K, V> callback) {
        if(numBuckets<2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        _buckets = new LinkedList<>();
        for(int i=0; i<numBuckets; i++) {
            _buckets.add(new HashMap<K, V>());
        }

        _callback = callback;
    }

    public RotatingMap(ExpiredCallback<K, V> callback) {
        this(DEFAULT_NUM_BUCKETS, callback);
    }

    public RotatingMap(int numBuckets) {
        this(numBuckets, null);
    }   
    
    public Map<K, V> rotate() {
        Map<K, V> dead = _buckets.removeLast();
        _buckets.addFirst(new HashMap<K, V>());
        if(_callback!=null) {
            for(Entry<K, V> entry: dead.entrySet()) {
                _callback.expire(entry.getKey(), entry.getValue());
            }
        }
        return dead;
    }

    public boolean containsKey(K key) {
        for(HashMap<K, V> bucket: _buckets) {
            if(bucket.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    public V get(K key) {
        for(HashMap<K, V> bucket: _buckets) {
            if(bucket.containsKey(key)) {
                return bucket.get(key);
            }
        }
        return null;
    }

    public void put(K key, V value) {
        Iterator<HashMap<K, V>> it = _buckets.iterator();
        HashMap<K, V> bucket = it.next();
        bucket.put(key, value);
        while(it.hasNext()) {
            bucket = it.next();
            bucket.remove(key);
        }
    }
    
    
    public Object remove(K key) {
        for(HashMap<K, V> bucket: _buckets) {
            if(bucket.containsKey(key)) {
                return bucket.remove(key);
            }
        }
        return null;
    }

    public int size() {
        int size = 0;
        for(HashMap<K, V> bucket: _buckets) {
            size+=bucket.size();
        }
        return size;
    }    
}
