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

import com.google.common.base.Joiner;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * Compile RelNodes into individual functions.
 */
class RelNodeCompiler extends PostOrderRelNodeVisitor<Void> {
  private final PrintWriter pw;
  private final JavaTypeFactory typeFactory;

  public Set<String> getReferredTables() {
    return Collections.unmodifiableSet(referredTables);
  }

  private final Set<String> referredTables = new TreeSet<>();

  RelNodeCompiler(PrintWriter pw, JavaTypeFactory typeFactory) {
    this.pw = pw;
    this.typeFactory = typeFactory;
  }

  @Override
  Void visitFilter(Filter filter) throws Exception {
    beginFunction(filter);
    pw.print("  if (_data == null) return null;\n");
    ExprCompiler compiler = new ExprCompiler(pw, typeFactory);
    String r = filter.getCondition().accept(compiler);
    pw.print(String.format("  return %s ? _data : null;\n", r));
    endFunction();
    return null;
  }

  @Override
  Void visitProject(Project project) throws Exception {
    beginFunction(project);
    pw.print("  if (_data == null) return null;\n");
    ExprCompiler compiler = new ExprCompiler(pw, typeFactory);

    int size = project.getChildExps().size();
    String[] res = new String[size];
    for (int i = 0; i < size; ++i) {
      res[i] = project.getChildExps().get(i).accept(compiler);
    }

    pw.print(String.format("  return new Values(%s);\n", Joiner.on(',').join
        (res)));
    endFunction();
    return null;
  }

  @Override
  Void defaultValue(RelNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  Void visitTableScan(TableScan scan) throws Exception {
    String tableName = Joiner.on('_').join(scan.getTable().getQualifiedName());
    referredTables.add(tableName);
    beginFunction(scan);
    pw.print(String.format("  return _datasources[TABLE_%s].next();\n",
                           tableName));
    endFunction();
    return null;
  }

  private void beginFunction(RelNode n) {
    pw.print(String.format("private Values %s(%s) {\n", getFunctionName(n), n
        .getInputs().isEmpty() ? "" : "Values _data"));
  }

  private void endFunction() {
    pw.print("}\n");
  }

  static String getFunctionName(RelNode n) {
    return n.getClass().getSimpleName() + "_" + n.getId();
  }
}
