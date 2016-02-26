/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * <p>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.storm.sql.compiler.backends.trident;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.storm.sql.TestUtils;
import org.apache.storm.sql.TestUtils.MockSqlTridentDataSource.CollectDataFunction;
import org.apache.storm.sql.compiler.TestCompilerUtils;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.trident.AbstractTridentProcessor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.storm.trident.TridentTopology;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.storm.sql.TestUtils.MockSqlTridentDataSource.CollectDataFunction.*;

public class TestPlanCompiler {
  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);

  @Before
  public void setUp() {
    getCollectedValues().clear();
  }

  @Test
  public void testCompile() throws Exception {
    final int EXPECTED_VALUE_SIZE = 2;
    String sql = "SELECT ID FROM FOO WHERE ID > 2";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
    PlanCompiler compiler = new PlanCompiler(typeFactory);
    final AbstractTridentProcessor proc = compiler.compile(state.tree());
    final Map<String, ISqlTridentDataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockSqlTridentDataSource());
    final TridentTopology topo = proc.build(data);
    Fields f = proc.outputStream().getOutputFields();
    proc.outputStream().each(f, new CollectDataFunction(), new Fields()).toStream();
    runTridentTopology(EXPECTED_VALUE_SIZE, proc, topo);
    Assert.assertArrayEquals(new Values[] { new Values(3), new Values(4)}, getCollectedValues().toArray());
  }

  @Test
  public void testInsert() throws Exception {
    final int EXPECTED_VALUE_SIZE = 1;
    String sql = "INSERT INTO BAR SELECT ID, NAME, ADDR FROM FOO WHERE ID > 3";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
    PlanCompiler compiler = new PlanCompiler(typeFactory);
    final AbstractTridentProcessor proc = compiler.compile(state.tree());
    final Map<String, ISqlTridentDataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockSqlTridentDataSource());
    data.put("BAR", new TestUtils.MockSqlTridentDataSource());
    final TridentTopology topo = proc.build(data);
    runTridentTopology(EXPECTED_VALUE_SIZE, proc, topo);
    Assert.assertArrayEquals(new Values[] { new Values(4, "x", "y")}, getCollectedValues().toArray());
  }

  private void runTridentTopology(final int expectedValueSize, AbstractTridentProcessor proc,
                                  TridentTopology topo) throws Exception {
    final Config conf = new Config();
    conf.setMaxSpoutPending(20);

    ILocalCluster cluster = new LocalCluster();
    StormTopology stormTopo = topo.build();
    try {
      Utils.setClassLoaderForJavaDeSerialize(proc.getClass().getClassLoader());
      cluster.submitTopology("storm-sql", conf, stormTopo);
      waitForCompletion(1000 * 1000, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return getCollectedValues().size() < expectedValueSize;
        }
      });
    } finally {
      Utils.resetClassLoaderForJavaDeSerialize();
      cluster.shutdown();
    }
  }

  private void waitForCompletion(long timeout, Callable<Boolean> cond) throws Exception {
    long start = TestUtils.monotonicNow();
    while (TestUtils.monotonicNow() - start < timeout && cond.call()) {
      Thread.sleep(100);
    }
  }
}
