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

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import com.google.common.collect.Lists;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic bolt for querying from HBase.
 *
 * Note: Each HBaseBolt defined in a topology is tied to a specific table.
 *
 */
public class HBaseLookupBolt extends AbstractHBaseBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupBolt.class);

    private HBaseValueMapper rowToTupleMapper;

    private HBaseProjectionCriteria projectionCriteria;

    public HBaseLookupBolt(String tableName, HBaseMapper mapper, HBaseValueMapper rowToTupleMapper){
        super(tableName, mapper);
        Validate.notNull(rowToTupleMapper, "rowToTupleMapper can not be null");
        this.rowToTupleMapper = rowToTupleMapper;
    }

    public HBaseLookupBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public HBaseLookupBolt withProjectionCriteria(HBaseProjectionCriteria projectionCriteria) {
        this.projectionCriteria = projectionCriteria;
        return this;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            collector.ack(tuple);
            return;
        }
        byte[] rowKey = this.mapper.rowKey(tuple);
        Get get = hBaseClient.constructGetRequests(rowKey, projectionCriteria);

        try {
            Result result = hBaseClient.batchGet(Lists.newArrayList(get))[0];
            for(Values values : rowToTupleMapper.toValues(tuple, result)) {
                this.collector.emit(tuple, values);
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        rowToTupleMapper.declareOutputFields(outputFieldsDeclarer);
    }
}
