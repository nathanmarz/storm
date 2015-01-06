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
package org.apache.storm.jdbc.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import org.apache.commons.lang.Validate;
import org.apache.storm.jdbc.common.JDBCClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractJdbcBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcBolt.class);

    protected OutputCollector collector;

    protected transient JDBCClient jdbcClient;
    protected String tableName;
    protected JdbcMapper mapper;
    protected String configKey;

    public AbstractJdbcBolt(String tableName, JdbcMapper mapper) {
        Validate.notEmpty(tableName, "Table name can not be blank or null");
        Validate.notNull(mapper, "mapper can not be null");
        this.tableName = tableName;
        this.mapper = mapper;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;

        Map<String, Object> conf = (Map<String, Object>)map.get(this.configKey);
        Validate.notEmpty(conf, "Hikari configuration not found using key '" + this.configKey + "'");

        this.jdbcClient = new JDBCClient(conf);
    }
}
