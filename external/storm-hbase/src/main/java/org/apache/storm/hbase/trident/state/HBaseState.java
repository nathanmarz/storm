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
package org.apache.storm.hbase.trident.state;

import org.apache.storm.Config;
import org.apache.storm.topology.FailedException;
import org.apache.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.common.HBaseClient;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseState implements State {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseState.class);

    private Options options;
    private HBaseClient hBaseClient;
    private Map map;
    private int numPartitions;
    private int partitionIndex;

    protected HBaseState(Map map, int partitionIndex, int numPartitions, Options options) {
        this.options = options;
        this.map = map;
        this.partitionIndex = partitionIndex;
        this.numPartitions = numPartitions;
    }

    public static class Options implements Serializable {
        private TridentHBaseMapper mapper;
        private Durability durability = Durability.SKIP_WAL;
        private HBaseProjectionCriteria projectionCriteria;
        private HBaseValueMapper rowToStormValueMapper;
        private String configKey;
        private String tableName;

        public Options withDurability(Durability durability) {
            this.durability = durability;
            return this;
        }

        public Options withProjectionCriteria(HBaseProjectionCriteria projectionCriteria) {
            this.projectionCriteria = projectionCriteria;
            return this;
        }

        public Options withConfigKey(String configKey) {
            this.configKey = configKey;
            return this;
        }

        public Options withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Options withRowToStormValueMapper(HBaseValueMapper rowToStormValueMapper) {
            this.rowToStormValueMapper = rowToStormValueMapper;
            return this;
        }

        public Options withMapper(TridentHBaseMapper mapper) {
            this.mapper = mapper;
            return this;
        }
    }

    protected void prepare() {
        final Configuration hbConfig = HBaseConfiguration.create();
        Map<String, Object> conf = (Map<String, Object>) map.get(options.configKey);
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

        //heck for backward compatibility, we need to pass TOPOLOGY_AUTO_CREDENTIALS to hbase conf
        //the conf instance is instance of persistentMap so making a copy.
        Map<String, Object> hbaseConfMap = new HashMap<String, Object>(conf);
        hbaseConfMap.put(Config.TOPOLOGY_AUTO_CREDENTIALS, map.get(Config.TOPOLOGY_AUTO_CREDENTIALS));

        this.hBaseClient = new HBaseClient(hbaseConfMap, hbConfig, options.tableName);
    }

    @Override
    public void beginCommit(Long aLong) {
        LOG.debug("beginCommit is noop.");
    }

    @Override
    public void commit(Long aLong) {
        LOG.debug("commit is noop.");
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        List<Mutation> mutations = Lists.newArrayList();

        for (TridentTuple tuple : tuples) {
            byte[] rowKey = options.mapper.rowKey(tuple);
            ColumnList cols = options.mapper.columns(tuple);
            mutations.addAll(hBaseClient.constructMutationReq(rowKey, cols, options.durability));
        }

        try {
            hBaseClient.batchMutate(mutations);
        } catch (Exception e) {
            collector.reportError(e);
            throw new FailedException(e);
        }
    }

    public List<List<Values>> batchRetrieve(List<TridentTuple> tridentTuples) {
        List<List<Values>> batchRetrieveResult = Lists.newArrayList();
        List<Get> gets = Lists.newArrayList();
        for (TridentTuple tuple : tridentTuples) {
            byte[] rowKey = options.mapper.rowKey(tuple);
            gets.add(hBaseClient.constructGetRequests(rowKey, options.projectionCriteria));
        }

        try {
            Result[] results = hBaseClient.batchGet(gets);
            for(int i = 0; i < results.length; i++) {
                Result result = results[i];
                TridentTuple tuple = tridentTuples.get(i);
                List<Values> values = options.rowToStormValueMapper.toValues(tuple, result);
                batchRetrieveResult.add(values);
            }
        } catch (Exception e) {
            LOG.warn("Batch get operation failed. Triggering replay.", e);
            throw new FailedException(e);
        }
        return batchRetrieveResult;
    }
}
