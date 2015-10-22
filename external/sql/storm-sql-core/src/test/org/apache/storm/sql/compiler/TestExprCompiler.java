/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.compiler;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestExprCompiler {
  @Test
  public void testLiteral() throws Exception {
    String sql = "SELECT 1,1.0,TRUE,'FOO' FROM FOO";
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    LogicalProject project = (LogicalProject) state.tree;
    String[] res = new String[project.getChildExps().size()];
    for (int i = 0; i < project.getChildExps().size(); ++i) {
      StringWriter sw = new StringWriter();
      try (PrintWriter pw = new PrintWriter(sw)) {
        ExprCompiler compiler = new ExprCompiler(pw, typeFactory);
        res[i] = project.getChildExps().get(i).accept(compiler);
      }
    }

    assertArrayEquals(new String[] {"1", "1.0E0", "true", "\"FOO\""}, res);
  }

  @Test
  public void testInputRef() throws Exception {
    String sql = "SELECT ID FROM FOO";
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    LogicalProject project = (LogicalProject) state.tree;
    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw)) {
      ExprCompiler compiler = new ExprCompiler(pw, typeFactory);
      project.getChildExps().get(0).accept(compiler);
    }

    assertEquals("int t0 = (int)(_data.get(0));\n", sw.toString());
  }

  @Test
  public void testCallExpr() throws Exception {
    String sql = "SELECT 1>2, 3+5, 1-1.0, 3+ID FROM FOO";
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    LogicalProject project = (LogicalProject) state.tree;
    String[] res = new String[project.getChildExps().size()];
    List<StringWriter> sw = new ArrayList<>();
    for (int i = 0; i < project.getChildExps().size(); ++i) {
      sw.add(new StringWriter());
    }

    for (int i = 0; i < project.getChildExps().size(); ++i) {
      try (PrintWriter pw = new PrintWriter(sw.get(i))) {
        ExprCompiler compiler = new ExprCompiler(pw, typeFactory);
        res[i] = project.getChildExps().get(i).accept(compiler);
      }
    }
    assertArrayEquals(new String[]{"1 > 2", "3 + 5", "1 - 1.0E0", "3 + t0"},
                      res);
  }

}
