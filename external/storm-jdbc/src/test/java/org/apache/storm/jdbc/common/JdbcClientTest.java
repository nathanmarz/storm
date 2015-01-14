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

import java.sql.Date;
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
        client.executeSql("create table user_details (id integer, user_name varchar(100), create_date date)");
    }

    @Test
    public void testInsertAndSelect() {
        int id = 1;
        String name = "bob";
        Date createDate = new Date(System.currentTimeMillis());

        List<Column> columns = Lists.newArrayList(
                new Column("id",id, Types.INTEGER),
                new Column("user_name",name, Types.VARCHAR),
                new Column("create_date", createDate , Types.DATE)
                );

        List<List<Column>> columnList = new ArrayList<List<Column>>();
        columnList.add(columns);
        client.insert(tableName, columnList);

        List<List<Column>> rows = client.select("select * from user_details where id = ?", Lists.newArrayList(new Column("id", id, Types.INTEGER)));
        for(List<Column> row : rows) {
            for(Column column : row) {
                if(column.getColumnName().equalsIgnoreCase("id")) {
                    Assert.assertEquals(id, column.getVal());
                } else if(column.getColumnName().equalsIgnoreCase("user_name")) {
                    Assert.assertEquals(name, column.getVal());
                } else if(column.getColumnName().equalsIgnoreCase("create_date")) {
                    Assert.assertEquals(createDate.toString(), column.getVal().toString());
                } else {
                    throw new AssertionError("Unknown column" + column);
                }
            }
        }
    }

    @After
    public void cleanup() {
        client.executeSql("drop table " + tableName);
    }
}
