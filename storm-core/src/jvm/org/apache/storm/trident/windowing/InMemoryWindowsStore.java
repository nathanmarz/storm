/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.trident.windowing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Inmemory store implementation of {@code WindowsStore} which can be backed by persistent store.
 *
 */
public class InMemoryWindowsStore implements WindowsStore, Serializable {

    private final ConcurrentHashMap<String, Object> store = new ConcurrentHashMap<>();

    private int maxSize;
    private AtomicInteger currentSize;
    private WindowsStore backingStore;

    public InMemoryWindowsStore() {
    }

    /**
     *
     * @param maxSize maximum size of inmemory store
     * @param backingStore backing store containing the entries
     */
    public InMemoryWindowsStore(int maxSize, WindowsStore backingStore) {
        this.maxSize = maxSize;
        currentSize = new AtomicInteger();
        this.backingStore = backingStore;
    }

    @Override
    public Object get(String key) {
        Object value = store.get(key);

        if(value == null && backingStore != null) {
            value = backingStore.get(key);
        }

        return value;
    }

    @Override
    public Iterable<Object> get(List<String> keys) {
        List<Object> values = new ArrayList<>();
        for (String key : keys) {
            values.add(get(key));
        }
        return values;
    }

    @Override
    public Iterable<String> getAllKeys() {
        if(backingStore != null) {
            return backingStore.getAllKeys();
        }

        final Enumeration<String> storeEnumeration = store.keys();
        final Iterator<String> resultIterator = new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return storeEnumeration.hasMoreElements();
            }

            @Override
            public String next() {
                return  storeEnumeration.nextElement();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove operation is not supported as it is immutable.");
            }
        };

        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return resultIterator;
            }
        };
    }

    @Override
    public void put(String key, Object value) {
        _put(key, value);

        if(backingStore != null) {
            backingStore.put(key, value);
        }
    }

    private void _put(String key, Object value) {
        if(!canAdd()) {
            return;
        }

        store.put(key, value);
        incrementCurrentSize();
    }

    private void incrementCurrentSize() {
        if(backingStore != null) {
            currentSize.incrementAndGet();
        }
    }

    private boolean canAdd() {
        return backingStore == null || currentSize.get() < maxSize;
    }

    @Override
    public void putAll(Collection<Entry> entries) {
        for (Entry entry : entries) {
            _put(entry.key, entry.value);
        }
        if(backingStore != null) {
            backingStore.putAll(entries);
        }
    }

    @Override
    public void remove(String key) {
        _remove(key);

        if(backingStore != null) {
            backingStore.remove(key);
        }
    }

    private void _remove(String key) {
        Object oldValue = store.remove(key);

        if(oldValue != null) {
            decrementSize();
            if(backingStore != null) {
                backingStore.remove(key);
            }
        }

    }

    private void decrementSize() {
        if(backingStore != null) {
            currentSize.decrementAndGet();
        }
    }

    @Override
    public void removeAll(Collection<String> keys) {
        for (String key : keys) {
            _remove(key);
        }

        if(backingStore != null) {
            backingStore.removeAll(keys);
        }
    }

    @Override
    public void shutdown() {
        store.clear();

        if(backingStore != null) {
            backingStore.shutdown();
        }
    }

    @Override
    public String toString() {
        return "InMemoryWindowsStore{" +
                " store:size = " + store.size() +
                " backingStore = " + backingStore +
                '}';
    }
}
