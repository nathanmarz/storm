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
package org.apache.storm.sql.compiler.backends.standalone;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.storm.sql.compiler.TestCompilerUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.hamcrest.CoreMatchers.containsString;

public class TestRelNodeCompiler {
  @Test
  public void testFilter() throws Exception {
    String sql = "SELECT ID + 1 FROM FOO WHERE ID > 3";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(
        RelDataTypeSystem.DEFAULT);
    LogicalProject project = (LogicalProject) state.tree();
    LogicalFilter filter = (LogicalFilter) project.getInput();

    try (StringWriter sw = new StringWriter();
         PrintWriter pw = new PrintWriter(sw)
    ) {
      RelNodeCompiler compiler = new RelNodeCompiler(pw, typeFactory);
      compiler.visitFilter(filter);
      pw.flush();
      Assert.assertThat(sw.toString(), containsString("> 3"));
    }

    try (StringWriter sw = new StringWriter();
         PrintWriter pw = new PrintWriter(sw)
    ) {
      RelNodeCompiler compiler = new RelNodeCompiler(pw, typeFactory);
      compiler.visitProject(project);
      pw.flush();
      Assert.assertThat(sw.toString(), containsString("plus("));
    }
  }
}
