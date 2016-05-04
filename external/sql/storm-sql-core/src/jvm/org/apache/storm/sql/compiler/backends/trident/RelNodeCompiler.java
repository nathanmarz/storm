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

import org.apache.storm.tuple.Fields;
import com.google.common.base.Joiner;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.storm.sql.compiler.ExprCompiler;
import org.apache.storm.sql.compiler.PostOrderRelNodeVisitor;

import java.io.PrintWriter;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Compile RelNodes into individual functions.
 */
class RelNodeCompiler extends PostOrderRelNodeVisitor<Void> {
  public static Joiner NEW_LINE_JOINER = Joiner.on('\n');

  private final PrintWriter pw;
  private final JavaTypeFactory typeFactory;
  private static final String STAGE_PROLOGUE = NEW_LINE_JOINER.join(
    "  private static final BaseFunction %1$s = ",
    "    new BaseFunction() {",
    "    @Override",
    "    public void execute(TridentTuple tuple, TridentCollector collector) {",
    "      List<Object> _data = tuple.getValues();",
    ""
  );

  private final IdentityHashMap<RelNode, Fields> outputFields = new IdentityHashMap<>();

  RelNodeCompiler(PrintWriter pw, JavaTypeFactory typeFactory) {
    this.pw = pw;
    this.typeFactory = typeFactory;
  }

  @Override
  public Void visitFilter(Filter filter) throws Exception {
    beginStage(filter);
    ExprCompiler compiler = new ExprCompiler(pw, typeFactory);
    String r = filter.getCondition().accept(compiler);
    pw.print(String.format("    if (%s) { collector.emit(_data); }\n", r));
    endStage();
    return null;
  }

  @Override
  public Void visitProject(Project project) throws Exception {
    beginStage(project);
    ExprCompiler compiler = new ExprCompiler(pw, typeFactory);

    int size = project.getChildExps().size();
    String[] res = new String[size];
    for (int i = 0; i < size; ++i) {
      res[i] = project.getChildExps().get(i).accept(compiler);
    }

    pw.print(String.format("    collector.emit(new Values(%s));\n",
                           Joiner.on(',').join(res)));
    endStage();
    return null;
  }

  @Override
  public Void defaultValue(RelNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visitTableScan(TableScan scan) throws Exception {
    return null;
  }

  @Override
  public Void visitTableModify(TableModify modify) throws Exception {
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
