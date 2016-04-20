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
package org.apache.storm.hbase.trident.windowing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.storm.trident.windowing.WindowKryoSerializer;
import org.apache.storm.trident.windowing.WindowsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class stores entries into hbase instance of the given configuration.
 *
 */
public class HBaseWindowsStore implements WindowsStore {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseWindowsStore.class);
    public static final String UTF_8 = "utf-8";

    private final ThreadLocal<HTable> threadLocalHtable;
    private final ThreadLocal<WindowKryoSerializer> threadLocalWindowKryoSerializer;
    private final Queue<HTable> htables = new ConcurrentLinkedQueue<>();
    private final byte[] family;
    private final byte[] qualifier;

    public HBaseWindowsStore(final Map stormConf, final Configuration config, final String tableName, byte[] family, byte[] qualifier) {
        this.family = family;
        this.qualifier = qualifier;

        threadLocalHtable = new ThreadLocal<HTable>() {
            @Override
            protected HTable initialValue() {
                try {
                    HTable hTable = new HTable(config, tableName);
                    htables.add(hTable);
                    return hTable;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        threadLocalWindowKryoSerializer = new ThreadLocal<WindowKryoSerializer>(){
            @Override
            protected WindowKryoSerializer initialValue() {
                return new WindowKryoSerializer(stormConf);
            }
        };

    }

    private HTable htable() {
        return threadLocalHtable.get();
    }

    private WindowKryoSerializer windowKryoSerializer() {
        return threadLocalWindowKryoSerializer.get();
    }

    private byte[] effectiveKey(String key) {
        try {
            return key.getBytes(UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(String key) {
        WindowsStore.Entry.nonNullCheckForKey(key);

        byte[] effectiveKey = effectiveKey(key);
        Get get = new Get(effectiveKey);
        Result result = null;
        try {
            result = htable().get(get);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if(result.isEmpty()) {
            return null;
        }

        return windowKryoSerializer().deserialize(result.getValue(family, qualifier));
    }

    @Override
    public Iterable<Object> get(List<String> keys) {
        List<Get> gets = new ArrayList<>();
        for (String key : keys) {
            WindowsStore.Entry.nonNullCheckForKey(key);

            byte[] effectiveKey = effectiveKey(key);
            gets.add(new Get(effectiveKey));
        }

        Result[] results = null;
        try {
            results = htable().get(gets);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Object> values = new ArrayList<>();
        for (int i=0; i<results.length; i++) {
            Result result = results[i];
            if(result.isEmpty()) {
                LOG.error("Got empty result for key [{}]", keys.get(i));
                throw new RuntimeException("Received empty result for key: "+keys.get(i));
            }
            Object resultObject = windowKryoSerializer().deserialize(result.getValue(family, qualifier));
            values.add(resultObject);
        }

        return values;
    }

    @Override
    public Iterable<String> getAllKeys() {
        Scan scan = new Scan();
        // this filter makes sure to receive only Key or row but not values associated with those rows.
        scan.setFilter(new FirstKeyOnlyFilter());
        //scan.setCaching(1000);

        final Iterator<Result> resultIterator;
        try {
            resultIterator = htable().getScanner(scan).iterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final Iterator<String> iterator = new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return resultIterator.hasNext();
            }

            @Override
            public String next() {
                Result result = resultIterator.next();
                String key = null;
                try {
                    key = new String(result.getRow(), UTF_8);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
                return key;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove operation is not supported");
            }
        };

        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return iterator;
            }
        };
    }

    @Override
    public void put(String key, Object value) {
        WindowsStore.Entry.nonNullCheckForKey(key);
        WindowsStore.Entry.nonNullCheckForValue(value);

        if(value == null) {
            throw new IllegalArgumentException("Invalid value of null with key: "+key);
        }
        Put put = new Put(effectiveKey(key));
        put.addColumn(family, ByteBuffer.wrap(qualifier), System.currentTimeMillis(), windowKryoSerializer().serializeToByteBuffer(value));
        try {
            htable().put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putAll(Collection<Entry> entries) {
        List<Put> list = new ArrayList<>();
        for (Entry entry : entries) {
            Put put = new Put(effectiveKey(entry.key));
            put.addColumn(family, ByteBuffer.wrap(qualifier), System.currentTimeMillis(), windowKryoSerializer().serializeToByteBuffer(entry.value));
            list.add(put);
        }

        try {
            htable().put(list);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove(String key) {
        WindowsStore.Entry.nonNullCheckForKey(key);

        Delete delete = new Delete(effectiveKey(key), System.currentTimeMillis());
        try {
            htable().delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeAll(Collection<String> keys) {
        List<Delete> deleteBatch = new ArrayList<>();
        for (String key : keys) {
            WindowsStore.Entry.nonNullCheckForKey(key);

            Delete delete = new Delete(effectiveKey(key), System.currentTimeMillis());
            deleteBatch.add(delete);
        }
        try {
            htable().delete(deleteBatch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        // close all the created hTable instances
        for (HTable htable : htables) {
            try {
                htable.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

}