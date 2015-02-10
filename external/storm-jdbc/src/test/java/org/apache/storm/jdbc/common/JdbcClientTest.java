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
package org.apache.storm.jdbc.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JdbcClientTest {

    private JdbcClient client;

    private static final String tableName = "user_details";
    @Before
    public void setup() {
        Map map = Maps.newHashMap();
        map.put("dataSourceClassName","org.hsqldb.jdbc.JDBCDataSource");//com.mysql.jdbc.jdbc2.optional.MysqlDataSource
        map.put("dataSource.url", "jdbc:hsqldb:mem:test");//jdbc:mysql://localhost/test
        map.put("dataSource.user","SA");//root
        map.put("dataSource.password","");//password

        int queryTimeoutSecs = 60;
        this.client = new JdbcClient(map, queryTimeoutSecs);
        client.executeSql("create table user_details (id integer, user_name varchar(100), created_timestamp TIMESTAMP)");
    }

    @Test
    public void testInsertAndSelect() {
        int id1 = 1;
        String name1 = "bob";
        Timestamp createDate1 = new Timestamp(System.currentTimeMillis());

        List<Column> row1 = Lists.newArrayList(
                new Column("ID",id1, Types.INTEGER),
                new Column("USER_NAME",name1, Types.VARCHAR),
                new Column("CREATED_TIMESTAMP", createDate1 , Types.TIMESTAMP));

        int id2 = 2;
        String name2 = "alice";
        Timestamp createDate2 = new Timestamp(System.currentTimeMillis());
        List<Column> row2 = Lists.newArrayList(
                new Column("ID",id2, Types.INTEGER),
                new Column("USER_NAME",name2, Types.VARCHAR),
                new Column("CREATED_TIMESTAMP", createDate2 , Types.TIMESTAMP));

        List<List<Column>> rows = Lists.newArrayList(row1, row2);
        client.insert(tableName, rows);

        List<List<Column>> selectedRows = client.select("select * from user_details where id = ?", Lists.newArrayList(new Column("id", id1, Types.INTEGER)));
        List<List<Column>> expectedRows = Lists.newArrayList();
        expectedRows.add(row1);

        Assert.assertEquals(expectedRows, selectedRows);

        selectedRows = client.select("select * from user_details order by id", Lists.<Column>newArrayList());
        Assert.assertEquals(rows, selectedRows);
    }

    @After
    public void cleanup() {
        client.executeSql("drop table " + tableName);
    }
}
