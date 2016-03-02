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
package org.apache.storm.sql.compiler;

import org.apache.storm.tuple.Values;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.storm.sql.TestUtils;
import org.apache.storm.sql.compiler.backends.standalone.PlanCompiler;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.AbstractValuesProcessor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestExprSemantic {
  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);

  @Test
  public void testLogicalExpr() throws Exception {
    Values v = testExpr(
        Lists.newArrayList("ID > 0 OR ID < 1", "ID > 0 AND ID < 1",
                           "NOT (ID > 0 AND ID < 1)"));
    assertEquals(new Values(true, false, true), v);
  }

  @Test
  public void testExpectOperator() throws Exception {
    Values v = testExpr(
        Lists.newArrayList("TRUE IS TRUE", "TRUE IS NOT TRUE",
                           "UNKNOWN IS TRUE", "UNKNOWN IS NOT TRUE",
                           "TRUE IS FALSE", "UNKNOWN IS NULL",
                           "UNKNOWN IS NOT NULL"));
    assertEquals(new Values(true, false, false, true, false, true, false), v);
  }

  @Test
  public void testArithmeticWithNull() throws Exception {
    Values v = testExpr(
      Lists.newArrayList(
          "1 + CAST(NULL AS INT)", "CAST(NULL AS INT) + 1", "CAST(NULL AS INT) + CAST(NULL AS INT)", "1 + 2"
      ));
    assertEquals(new Values(null, null, null, 3), v);
  }

  @Test
  public void testNotWithNull() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "NOT TRUE", "NOT FALSE", "NOT UNKNOWN"
        ));
    assertEquals(new Values(false, true, null), v);
  }

  @Test
  public void testAndWithNull() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "UNKNOWN AND TRUE", "UNKNOWN AND FALSE", "UNKNOWN AND UNKNOWN",
            "TRUE AND TRUE", "TRUE AND FALSE", "TRUE AND UNKNOWN",
            "FALSE AND TRUE", "FALSE AND FALSE", "FALSE AND UNKNOWN"
        ));
    assertEquals(new Values(null, false, null, true, false, null, false,
                            false, false), v);
  }

  @Test
  public void testAndWithNullable() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "ADDR = 'a' AND NAME = 'a'", "NAME = 'a' AND ADDR = 'a'", "NAME = 'x' AND ADDR = 'a'", "ADDR = 'a' AND NAME = 'x'"
            ));
    assertEquals(new Values(false, false, null, null), v);
  }

  @Test
  public void testOrWithNullable() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "ADDR = 'a'  OR NAME = 'a'", "NAME = 'a' OR ADDR = 'a' ", "NAME = 'x' OR ADDR = 'a' ", "ADDR = 'a'  OR NAME = 'x'"
            ));
    assertEquals(new Values(null, null, true, true), v);
  }

  @Test
  public void testOrWithNull() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "UNKNOWN OR TRUE", "UNKNOWN OR FALSE", "UNKNOWN OR UNKNOWN",
            "TRUE OR TRUE", "TRUE OR FALSE", "TRUE OR UNKNOWN",
            "FALSE OR TRUE", "FALSE OR FALSE", "FALSE OR UNKNOWN"
            ));
    assertEquals(new Values(true, null, null, true, true, true, true,
                            false, null), v);
  }

  @Test
  public void testEquals() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "1 = 2", "UNKNOWN = UNKNOWN", "'a' = 'a'", "'a' = UNKNOWN", "UNKNOWN = 'a'", "'a' = 'b'",
            "1 <> 2", "UNKNOWN <> UNKNOWN", "'a' <> 'a'", "'a' <> UNKNOWN", "UNKNOWN <> 'a'", "'a' <> 'b'"
        ));
    assertEquals(new Values(false, null, true, null, null, false,
        true, null, false, null, null, true), v);
  }

  @Test
  public void testStringMethods() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "UPPER('a')", "LOWER('A')", "INITCAP('foo')",
            "SUBSTRING('foo', 2)", "CHARACTER_LENGTH('foo')", "CHAR_LENGTH('foo')",
            "'ab' || 'cd'"
        ));
    assertEquals(new Values("A", "a", "Foo", "oo", 3, 3, "abcd"), v);
  }

  private Values testExpr(List<String> exprs) throws Exception {
    String sql = "SELECT " + Joiner.on(',').join(exprs) + " FROM FOO" +
        " WHERE ID > 0 AND ID < 2";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
    PlanCompiler compiler = new PlanCompiler(typeFactory);
    AbstractValuesProcessor proc = compiler.compile(state.tree);
    Map<String, DataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockDataSource());
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    proc.initialize(data, h);
    return values.get(0);
  }

}
