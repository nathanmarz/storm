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

import backtype.storm.tuple.Values;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.storm.sql.storm.ChannelHandler;
import org.apache.storm.sql.storm.DataSource;
import org.apache.storm.sql.storm.runtime.AbstractValuesProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPlanCompiler {
  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);

  @Test
  public void testCompile() throws Exception {
    String sql = "SELECT ID + 1 FROM FOO WHERE ID > 2";
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
    PlanCompiler compiler = new PlanCompiler(typeFactory);
    AbstractValuesProcessor proc = compiler.compile(state.tree);
    Map<String, DataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockDataSource());
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    proc.initialize(data, h);
    Assert.assertArrayEquals(new Values[] { new Values(4), new Values(5)},
                             values.toArray());
  }
}
