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
package org.apache.storm.jdbc.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;


public class UserPersistanceTopology extends AbstractUserTopology {
    private static final String USER_SPOUT = "USER_SPOUT";
    private static final String LOOKUP_BOLT = "LOOKUP_BOLT";
    private static final String PERSISTANCE_BOLT = "PERSISTANCE_BOLT";

    public static void main(String[] args) throws Exception {
        new UserPersistanceTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
        JdbcLookupBolt departmentLookupBolt = new JdbcLookupBolt(JDBC_CONF)
                .withJdbcLookupMapper(this.jdbcLookupMapper)
                .withSelectSql(SELECT_QUERY);
        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(JDBC_CONF)
                .withTableName(TABLE_NAME)
                .withJdbcMapper(this.jdbcMapper);

        // userSpout ==> jdbcBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(USER_SPOUT, this.userSpout, 1);
        builder.setBolt(LOOKUP_BOLT, departmentLookupBolt, 1).shuffleGrouping(USER_SPOUT);
        builder.setBolt(PERSISTANCE_BOLT, userPersistanceBolt, 1).shuffleGrouping(LOOKUP_BOLT);
        return builder.createTopology();
    }
}
