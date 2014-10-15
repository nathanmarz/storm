package org.apache.storm.hbase.trident.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.ByteArrayOutputStream;
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
                hbConfig.set(key, String.valueOf(map.get(key)));
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
        public String qualifier;
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
            LOG.info("Partition: {}, GET: {}", this.partitionNum, key);
            Get get = new Get(toRowKey(key));
            get.addColumn(this.options.columnFamily.getBytes(), this.options.qualifier.getBytes());
            gets.add(get);
        }

        List<T> retval = new ArrayList<T>();
        try {
            Result[] results = this.table.get(gets);
            for (Result result : results) {
                byte[] value = result.getValue(this.options.columnFamily.getBytes(), this.options.qualifier.getBytes());
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
            LOG.info("Partiton: {}, Key: {}, Value: {}", new Object[]{this.partitionNum, keys.get(i), new String(this.serializer.serialize(values.get(i)))});
            Put put = new Put(toRowKey(keys.get(i)));
            T val = values.get(i);
            put.add(this.options.columnFamily.getBytes(),
                    this.options.qualifier.getBytes(),
                    this.serializer.serialize(val));

            puts.add(put);
        }
        try {
            this.table.put(puts);
        } catch (InterruptedIOException e) {
            throw new FailedException("Interrupted while writing to HBase", e);
        } catch (RetriesExhaustedWithDetailsException e) {
            throw new FailedException("Retries exhaused while writing to HBase", e);
        }
    }


    private byte[] toRowKey(List<Object> keys) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            for (Object key : keys) {
                bos.write(String.valueOf(key).getBytes());
            }
            bos.close();
        } catch (IOException e){
            throw new RuntimeException("IOException creating HBase row key.", e);
        }
        return bos.toByteArray();
    }
}
