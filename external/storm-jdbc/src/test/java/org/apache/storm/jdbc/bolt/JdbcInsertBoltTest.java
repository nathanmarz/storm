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

import com.google.common.collect.Lists;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * Created by pbrahmbhatt on 10/29/15.
 */
public class JdbcInsertBoltTest {

    @Test
    public void testValidation() {
        ConnectionProvider provider = new HikariCPConnectionProvider(new HashMap<String, Object>());
        JdbcMapper mapper = new SimpleJdbcMapper(Lists.newArrayList(new Column("test", 0)));
        expectIllegaArgs(null, mapper);
        expectIllegaArgs(provider, null);

        try {
            JdbcInsertBolt bolt = new JdbcInsertBolt(provider, mapper);
            bolt.withInsertQuery("test");
            bolt.withTableName("test");
            Assert.fail("Should have thrown IllegalArgumentException.");
        } catch(IllegalArgumentException ne) {
            //expected
        }

        try {
            JdbcInsertBolt bolt = new JdbcInsertBolt(provider, mapper);
            bolt.withTableName("test");
            bolt.withInsertQuery("test");
            Assert.fail("Should have thrown IllegalArgumentException.");
        } catch(IllegalArgumentException ne) {
            //expected
        }
    }

    private void expectIllegaArgs(ConnectionProvider provider, JdbcMapper mapper) {
        try {
            JdbcInsertBolt bolt = new JdbcInsertBolt(provider, mapper);
            Assert.fail("Should have thrown IllegalArgumentException.");
        } catch(IllegalArgumentException ne) {
            //expected
        }
    }

}
