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
package org.apache.storm.hbase.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Basic bolt for writing to HBase.
 *
 * Note: Each HBaseBolt defined in a topology is tied to a specific table.
 *
 */
public class HBaseBolt  extends AbstractHBaseBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseBolt.class);

    boolean writeToWAL = true;

    public HBaseBolt(String tableName, HBaseMapper mapper) {
        super(tableName, mapper);
    }

    public HBaseBolt writeToWAL(boolean writeToWAL) {
        this.writeToWAL = writeToWAL;
        return this;
    }

    public HBaseBolt withConfigKey(String configKey) {
        this.configKey = configKey;
        return this;
    }

    @Override
    public void execute(Tuple tuple) {
        byte[] rowKey = this.mapper.rowKey(tuple);
        ColumnList cols = this.mapper.columns(tuple);
        List<Mutation> mutations = hBaseClient.constructMutationReq(rowKey, cols, writeToWAL? Durability.SYNC_WAL : Durability.SKIP_WAL);

        try {
            this.hBaseClient.batchMutate(mutations);
        } catch(Exception e){
            LOG.warn("Failing tuple. Error writing rowKey " + rowKey, e);
            this.collector.fail(tuple);
            return;
        }

        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
