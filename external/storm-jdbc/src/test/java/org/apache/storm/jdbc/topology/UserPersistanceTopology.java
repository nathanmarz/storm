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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.google.common.collect.Maps;
import org.apache.storm.jdbc.bolt.JdbcBolt;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.spout.UserSpout;

import java.util.Map;


public class UserPersistanceTopology {
    private static final String USER_SPOUT = "USER_SPOUT";
    private static final String USER_BOLT = "USER_BOLT";

    public static void main(String[] args) throws Exception {
        if(args.length < 5) {
            System.out.println("Usage: UserPersistanceTopology <dataSourceClassName> <dataSource.url> " +
                    "<user> <password> <tableName> [topology name]");
        }
        Map map = Maps.newHashMap();
        map.put("dataSourceClassName",args[0]);//com.mysql.jdbc.jdbc2.optional.MysqlDataSource
        map.put("dataSource.url", args[1]);//jdbc:mysql://localhost/test
        map.put("dataSource.user",args[2]);//root
        map.put("dataSource.password",args[3]);//password
        String tableName = args[4];//database table name
        JdbcMapper jdbcMapper = new SimpleJdbcMapper(tableName, map);

        Config config = new Config();

        config.put("jdbc.conf", map);

        UserSpout spout = new UserSpout();
        JdbcBolt bolt = new JdbcBolt(tableName, jdbcMapper)
                .withConfigKey("jdbc.conf");

        // userSpout ==> jdbcBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(USER_SPOUT, spout, 1);
        builder.setBolt(USER_BOLT, bolt, 1).shuffleGrouping(USER_SPOUT);

        if (args.length == 5) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(30000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else if (args.length == 6) {
            StormSubmitter.submitTopology(args[6], config, builder.createTopology());
        } else {
            System.out.println("Usage: UserPersistanceTopology <dataSourceClassName> <dataSource.url> " +
                    "<user> <password> <tableName> [topology name]");
        }
    }
}
