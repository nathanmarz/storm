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

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.commons.lang.Validate;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractJdbcBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcBolt.class);

    protected OutputCollector collector;

    protected transient JdbcClient jdbcClient;
    protected String configKey;
    protected Integer queryTimeoutSecs;
    protected ConnectionProvider connectionProvider;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;

        connectionProvider.prepare();

        if(queryTimeoutSecs == null) {
            queryTimeoutSecs = Integer.parseInt(map.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
        }

        this.jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
    }

    public AbstractJdbcBolt(ConnectionProvider connectionProvider) {
        Validate.notNull(connectionProvider);
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void cleanup() {
        connectionProvider.cleanup();
    }
}
