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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

/**
 * Basic bolt for writing to HBase.
 *
 * Note: Each HBaseBolt defined in a topology is tied to a specific table.
 *
 */
// TODO support more configuration options, for now we're defaulting to the hbase-*.xml files found on the classpath
public class HBaseBolt  extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseBolt.class);

    private OutputCollector collector;

    private transient HTable table;
    private String tableName;
    private HBaseMapper mapper;
    boolean writeToWAL = true;
    private String configKey;

    public HBaseBolt(String tableName, HBaseMapper mapper){
        this.tableName = tableName;
        this.mapper = mapper;
    }

    public HBaseBolt writeToWAL(boolean writeToWAL){
        this.writeToWAL = writeToWAL;
        return this;
    }

    public HBaseBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        final Configuration hbConfig = HBaseConfiguration.create();

        Map<String, Object> conf = (Map<String, Object>)map.get(this.configKey);
        if(conf == null){
            throw new IllegalArgumentException("HBase configuration not found using key '" + this.configKey + "'");
        }
        if(conf.get("hbase.rootdir") == null){
            LOG.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
        }
        for(String key : conf.keySet()){
            hbConfig.set(key, String.valueOf(map.get(key)));
        }

        try{
            UserProvider provider = HBaseSecurityUtil.login(map, hbConfig);
            this.table = provider.getCurrent().getUGI().doAs(new PrivilegedExceptionAction<HTable>() {
                @Override
                public HTable run() throws IOException {
                    return new HTable(hbConfig, tableName);
                }
            });
        } catch(Exception e){
            throw new RuntimeException("HBase bolt preparation failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        byte[] rowKey = this.mapper.rowKey(tuple);
        ColumnList cols = this.mapper.columns(tuple);
        if(cols.hasColumns()){
            Put put = new Put(rowKey);
            // TODO fix call to deprecated method
            put.setWriteToWAL(this.writeToWAL);
            for(ColumnList.Column col : cols.getColumns()){
                if(col.getTs() > 0){
                    put.add(
                            col.getFamily(),
                            col.getQualifier(),
                            col.getTs(),
                            col.getValue()
                    );
                } else{
                    put.add(
                            col.getFamily(),
                            col.getQualifier(),
                            col.getValue()
                    );
                }
            }
            try{
                this.table.put(put);
            } catch(RetriesExhaustedWithDetailsException e){
                LOG.warn("Failing tuple. Error writing column.", e);
                this.collector.fail(tuple);
                return;
            } catch (InterruptedIOException e) {
                LOG.warn("Failing tuple. Error writing column.", e);
                this.collector.fail(tuple);
                return;
            }
        }
        if(cols.hasCounters()){
            Increment inc = new Increment(rowKey);
            // TODO fix call to deprecated method
            inc.setWriteToWAL(this.writeToWAL);
            for(ColumnList.Counter cnt : cols.getCounters()){
                inc.addColumn(
                        cnt.getFamily(),
                        cnt.getQualifier(),
                        cnt.getIncrement()
                );
            }
            try{
                this.table.increment(inc);
            } catch (IOException e) {
                LOG.warn("Failing tuple. Error incrementing counter.", e);
                this.collector.fail(tuple);
                return;
            }
        }
        this.collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
