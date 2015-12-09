/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyInitialStatus;
import backtype.storm.tuple.Values;
import org.apache.storm.sql.runtime.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TestStormSql {
  private static class MockDataSourceProvider implements DataSourcesProvider {
    @Override
    public String scheme() {
      return "mock";
    }

    @Override
    public DataSource construct(
        URI uri, String inputFormatClass, String outputFormatClass,
        List<FieldInfo> fields) {
      return new TestUtils.MockDataSource();
    }

    @Override
    public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                  String properties, List<FieldInfo> fields) {
      return new TestUtils.MockSqlTridentDataSource();
    }
  }

  @BeforeClass
  public static void setUp() {
    DataSourcesRegistry.providerMap().put("mock", new MockDataSourceProvider());
  }

  @AfterClass
  public static void tearDown() {
    DataSourcesRegistry.providerMap().remove("mock");
  }

  @Test
  public void testExternalDataSource() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT) LOCATION 'mock:///foo'");
    stmt.add("SELECT STREAM ID + 1 FROM FOO WHERE ID > 2");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(4, values.get(0).get(0));
    Assert.assertEquals(5, values.get(1).get(0));
  }
}
