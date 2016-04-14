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
package org.apache.storm.sql;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.tools.ValidationException;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.tuple.Values;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

  private static class MockNestedDataSourceProvider implements DataSourcesProvider {
    @Override
    public String scheme() {
      return "mocknested";
    }

    @Override
    public DataSource construct(
            URI uri, String inputFormatClass, String outputFormatClass,
            List<FieldInfo> fields) {
      return new TestUtils.MockNestedDataSource();
    }

    @Override
    public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                  String properties, List<FieldInfo> fields) {
      throw new UnsupportedOperationException("Not supported");
    }
  }


  @BeforeClass
  public static void setUp() {
    DataSourcesRegistry.providerMap().put("mock", new MockDataSourceProvider());
    DataSourcesRegistry.providerMap().put("mocknested", new MockNestedDataSourceProvider());
  }

  @AfterClass
  public static void tearDown() {
    DataSourcesRegistry.providerMap().remove("mock");
    DataSourcesRegistry.providerMap().remove("mocknested");
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

  @Test
  public void testExternalDataSourceNested() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
    stmt.add("SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
                     "FROM FOO " +
                     "WHERE NESTEDMAPFIELD['a']['b'] = 2 AND ARRAYFIELD[1] = 200");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    System.out.println(values);
    Map<String, Integer> map = ImmutableMap.of("b", 2, "c", 4);
    Map<String, Map<String, Integer>> nestedMap = ImmutableMap.of("a", map);
    Assert.assertEquals(new Values(2, map, nestedMap, Arrays.asList(100, 200, 300)), values.get(0));
  }

  @Test
  public void testExternalNestedInvalidAccess() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
    stmt.add("SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
                     "FROM FOO " +
                     "WHERE NESTEDMAPFIELD['a']['b'] = 2 AND ARRAYFIELD['a'] = 200");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(0, values.size());
  }

  @Test(expected = ValidationException.class)
  public void testExternalUdfType() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, NAME VARCHAR) LOCATION 'mock:///foo'");
    stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
    stmt.add("SELECT STREAM MYPLUS(NAME, 1) FROM FOO WHERE ID = 0");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    System.out.println(values);

  }

  @Test
  public void testExternalUdfType2() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, NAME VARCHAR) LOCATION 'mock:///foo'");
    stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
    stmt.add("SELECT STREAM ID FROM FOO WHERE MYPLUS(ID, 1) = 'x'");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(0, values.size());
  }

  @Test
  public void testExternalUdf() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT) LOCATION 'mock:///foo'");
    stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
    stmt.add("SELECT STREAM MYPLUS(ID, 1) FROM FOO WHERE ID > 2");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(4, values.get(0).get(0));
    Assert.assertEquals(5, values.get(1).get(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testExternalUdfUsingJar() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT) LOCATION 'mock:///foo'");
    stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus' USING JAR 'foo'");
    stmt.add("SELECT STREAM MYPLUS(ID, 1) FROM FOO WHERE ID > 2");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
  }
}
