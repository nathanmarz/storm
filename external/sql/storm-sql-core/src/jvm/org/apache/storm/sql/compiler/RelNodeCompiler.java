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
  public static Joiner NEW_LINE_JOINER = Joiner.on('\n');

  private final PrintWriter pw;
  private final JavaTypeFactory typeFactory;
  private static final String STAGE_PROLOGUE = NEW_LINE_JOINER.join(
    "  private static final ChannelHandler %1$s = ",
    "    new AbstractChannelHandler() {",
    "    @Override",
    "    public void dataReceived(ChannelContext ctx, Values _data) {",
    ""
  );

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
    beginStage(filter);
    ExprCompiler compiler = new ExprCompiler(pw, typeFactory);
    String r = filter.getCondition().accept(compiler);
    pw.print(String.format("    if (%s) { ctx.emit(_data); }\n", r));
    endStage();
    return null;
  }

  @Override
  Void visitProject(Project project) throws Exception {
    beginStage(project);
    ExprCompiler compiler = new ExprCompiler(pw, typeFactory);

    int size = project.getChildExps().size();
    String[] res = new String[size];
    for (int i = 0; i < size; ++i) {
      res[i] = project.getChildExps().get(i).accept(compiler);
    }

    pw.print(String.format("    ctx.emit(new Values(%s));\n",
                           Joiner.on(',').join(res)));
    endStage();
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
    beginStage(scan);
    pw.print("    ctx.emit(_data);\n");
    endStage();
    return null;
  }

  private void beginStage(RelNode n) {
    pw.print(String.format(STAGE_PROLOGUE, getStageName(n)));
  }

  private void endStage() {
    pw.print("  }\n  };\n");
  }

  static String getStageName(RelNode n) {
    return n.getClass().getSimpleName().toUpperCase() + "_" + n.getId();
  }
}
