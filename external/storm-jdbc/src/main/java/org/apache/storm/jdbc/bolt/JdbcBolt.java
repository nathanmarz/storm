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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Basic bolt for writing to any Database table.
 * <p/>
 * Note: Each JdbcBolt defined in a topology is tied to a specific table.
 */
public class JdbcBolt extends AbstractJdbcBolt {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcBolt.class);

    private String tableName;
    private JdbcMapper jdbcMapper;

    public JdbcBolt(String configKey) {
        super(configKey);
    }

    public JdbcBolt withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public JdbcBolt withJdbcMapper(JdbcMapper jdbcMapper) {
        this.jdbcMapper = jdbcMapper;
        return this;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            List<Column> columns = jdbcMapper.getColumns(tuple);
            List<List<Column>> columnLists = new ArrayList<List<Column>>();
            columnLists.add(columns);
            this.jdbcClient.insert(this.tableName, columnLists);
        } catch (Exception e) {
            LOG.warn("Failing tuple.", e);
            this.collector.fail(tuple);
            this.collector.reportError(e);
            return;
        }

        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
