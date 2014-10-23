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
import backtype.storm.topology.base.BaseRichBolt;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// TODO support more configuration options, for now we're defaulting to the hbase-*.xml files found on the classpath
public abstract class AbstractHBaseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseBolt.class);

    protected OutputCollector collector;

    protected transient HBaseClient hBaseClient;
    protected String tableName;
    protected HBaseMapper mapper;
    protected String configKey;

    public AbstractHBaseBolt(String tableName, HBaseMapper mapper) {
        Validate.notEmpty(tableName, "Table name can not be blank or null");
        Validate.notNull(mapper, "mapper can not be null");
        this.tableName = tableName;
        this.mapper = mapper;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        final Configuration hbConfig = HBaseConfiguration.create();

        Map<String, Object> conf = (Map<String, Object>)map.get(this.configKey);
        if(conf == null) {
            throw new IllegalArgumentException("HBase configuration not found using key '" + this.configKey + "'");
        }
        if(conf.get("hbase.rootdir") == null) {
            LOG.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
        }
        for(String key : conf.keySet()) {
            hbConfig.set(key, String.valueOf(conf.get(key)));
        }

        this.hBaseClient = new HBaseClient(conf, hbConfig, tableName);
    }
}
