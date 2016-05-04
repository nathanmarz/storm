/*
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
package org.apache.storm.hbase.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.tuple.Values;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HBaseMapState<T> implements IBackingMap<T> {
    private static Logger LOG = LoggerFactory.getLogger(HBaseMapState.class);

    private int partitionNum;


    @SuppressWarnings("rawtypes")
    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps.newHashMap();

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    private Options<T> options;
    private Serializer<T> serializer;
    private HTable table;

    public HBaseMapState(final Options<T> options, Map map, int partitionNum) {
        this.options = options;
        this.serializer = options.serializer;
        this.partitionNum = partitionNum;

        final Configuration hbConfig = HBaseConfiguration.create();
        Map<String, Object> conf = (Map<String, Object>)map.get(options.configKey);
        if(conf == null){
            LOG.info("HBase configuration not found using key '" + options.configKey + "'");
            LOG.info("Using HBase config from first hbase-site.xml found on classpath.");
        } else {
            if (conf.get("hbase.rootdir") == null) {
                LOG.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
            }
            for (String key : conf.keySet()) {
                hbConfig.set(key, String.valueOf(conf.get(key)));
            }
        }

        try{
            UserProvider provider = HBaseSecurityUtil.login(map, hbConfig);
            this.table = provider.getCurrent().getUGI().doAs(new PrivilegedExceptionAction<HTable>() {
                @Override
                public HTable run() throws IOException {
                    return new HTable(hbConfig, options.tableName);
                }
            });
        } catch(Exception e){
            throw new RuntimeException("HBase bolt preparation failed: " + e.getMessage(), e);
        }

    }


    public static class Options<T> implements Serializable {

        public Serializer<T> serializer = null;
        public int cacheSize = 5000;
        public String globalKey = "$HBASE_STATE_GLOBAL$";
        public String configKey = "hbase.config";
        public String tableName;
        public String columnFamily;
        public TridentHBaseMapMapper mapMapper;
    }


    @SuppressWarnings("rawtypes")
    public static StateFactory opaque() {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(Options<OpaqueValue> opts) {

        return new Factory(StateType.OPAQUE, opts);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional() {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional() {
        Options<Object> options = new Options<Object>();
        return nonTransactional(options);
    }

    public static StateFactory nonTransactional(Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL, opts);
    }


    protected static class Factory implements StateFactory {
        private StateType stateType;
        private Options options;

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Factory(StateType stateType, Options options) {
            this.stateType = stateType;
            this.options = options;

            if (this.options.serializer == null) {
                this.options.serializer = DEFAULT_SERIALZERS.get(stateType);
            }

            if (this.options.serializer == null) {
                throw new RuntimeException("Serializer should be specified for type: " + stateType);
            }

            if (this.options.mapMapper == null) {
                throw new RuntimeException("MapMapper should be specified for type: " + stateType);
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            LOG.info("Preparing HBase State for partition {} of {}.", partitionIndex + 1, numPartitions);
            IBackingMap state = new HBaseMapState(options, conf, partitionIndex);

            if(options.cacheSize > 0) {
                state = new CachedMap(state, options.cacheSize);
            }

            MapState mapState;
            switch (stateType) {
                case NON_TRANSACTIONAL:
                    mapState = NonTransactionalMap.build(state);
                    break;
                case OPAQUE:
                    mapState = OpaqueMap.build(state);
                    break;
                case TRANSACTIONAL:
                    mapState = TransactionalMap.build(state);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown state type: " + stateType);
            }
            return new SnapshottableMap(mapState, new Values(options.globalKey));
        }

    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<Get> gets = new ArrayList<Get>();
        for(List<Object> key : keys){
            byte[] hbaseKey = this.options.mapMapper.rowKey(key);
            String qualifier = this.options.mapMapper.qualifier(key);

            LOG.info("Partition: {}, GET: {}", this.partitionNum, new String(hbaseKey));
            Get get = new Get(hbaseKey);
            get.addColumn(this.options.columnFamily.getBytes(), qualifier.getBytes());
            gets.add(get);
        }

        List<T> retval = new ArrayList<T>();
        try {
            Result[] results = this.table.get(gets);
            for (int i = 0; i < keys.size(); i++) {
                String qualifier = this.options.mapMapper.qualifier(keys.get(i));
                Result result = results[i];
                byte[] value = result.getValue(this.options.columnFamily.getBytes(), qualifier.getBytes());
                if(value != null) {
                    retval.add(this.serializer.deserialize(value));
                } else {
                    retval.add(null);
                }
            }
        } catch(IOException e){
            throw new FailedException("IOException while reading from HBase.", e);
        }
        return retval;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        List<Put> puts = new ArrayList<Put>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            byte[] hbaseKey = this.options.mapMapper.rowKey(keys.get(i));
            String qualifier = this.options.mapMapper.qualifier(keys.get(i));
            LOG.info("Partiton: {}, Key: {}, Value: {}", new Object[]{this.partitionNum, new String(hbaseKey), new String(this.serializer.serialize(values.get(i)))});
            Put put = new Put(hbaseKey);
            T val = values.get(i);
            put.add(this.options.columnFamily.getBytes(),
                    qualifier.getBytes(),
                    this.serializer.serialize(val));

            puts.add(put);
        }
        try {
            this.table.put(puts);
        } catch (InterruptedIOException e) {
            throw new FailedException("Interrupted while writing to HBase", e);
        } catch (RetriesExhaustedWithDetailsException e) {
            throw new FailedException("Retries exhaused while writing to HBase", e);
        } catch (IOException e) {
            throw new FailedException("IOException while writing to HBase", e);
        }
    }
}
