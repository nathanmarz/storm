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

import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import com.google.common.collect.Lists;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.spout.UserSpout;
import org.apache.storm.jdbc.trident.state.JdbcQuery;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;

import java.sql.Types;

public class UserPersistanceTridentTopology extends AbstractUserTopology {

    public static void main(String[] args) throws Exception {
        new UserPersistanceTridentTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
        TridentTopology topology = new TridentTopology();

        JdbcState.Options options = new JdbcState.Options()
                .withConnectionProvider(connectionProvider)
                .withMapper(this.jdbcMapper)
                .withJdbcLookupMapper(new SimpleJdbcLookupMapper(new Fields("dept_name"), Lists.newArrayList(new Column("user_id", Types.INTEGER))))
                .withTableName(TABLE_NAME)
                .withSelectQuery(SELECT_QUERY);

        JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);

        Stream stream = topology.newStream("userSpout", new UserSpout());
        TridentState state = topology.newStaticState(jdbcStateFactory);
        stream = stream.stateQuery(state, new Fields("user_id","user_name","create_date"), new JdbcQuery(), new Fields("dept_name"));
        stream.partitionPersist(jdbcStateFactory, new Fields("user_id","user_name","dept_name","create_date"),  new JdbcUpdater(), new Fields());
        return topology.build();
    }
}
