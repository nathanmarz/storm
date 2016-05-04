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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.storm.sql.compiler.CompilerUtil;
import org.apache.storm.sql.compiler.PostOrderRelNodeVisitor;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.runtime.trident.AbstractTridentProcessor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;

public class PlanCompiler {
  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");
  private static final String PACKAGE_NAME = "org.apache.storm.sql.generated";
  private static final String PROLOGUE = NEW_LINE_JOINER.join(
      "// GENERATED CODE", "package " + PACKAGE_NAME + ";", "",
      "import java.util.List;",
      "import java.util.Map;",
      "import org.apache.storm.tuple.Fields;",
      "import org.apache.storm.tuple.Values;",
      "import org.apache.storm.sql.runtime.ISqlTridentDataSource;",
      "import org.apache.storm.sql.runtime.trident.AbstractTridentProcessor;",
      "import org.apache.storm.trident.Stream;",
      "import org.apache.storm.trident.TridentTopology;",
      "import org.apache.storm.trident.fluent.IAggregatableStream;",
      "import org.apache.storm.trident.operation.TridentCollector;",
      "import org.apache.storm.trident.operation.BaseFunction;",
      "import org.apache.storm.trident.spout.IBatchSpout;",
      "import org.apache.storm.trident.tuple.TridentTuple;",
      "",
      "public final class TridentProcessor extends AbstractTridentProcessor {",
      "");
  private static final String INITIALIZER_PROLOGUE = NEW_LINE_JOINER.join(
      "  @Override",
      "  public TridentTopology build(Map<String, ISqlTridentDataSource> _sources) {",
      "    TridentTopology topo = new TridentTopology();",
      ""
  );

  private final JavaTypeFactory typeFactory;
  private CompilingClassLoader compilingClassLoader;
  public PlanCompiler(JavaTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
  }

  private String generateJavaSource(RelNode root) throws Exception {
    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw)) {
      RelNodeCompiler compiler = new RelNodeCompiler(pw, typeFactory);
      printPrologue(pw);
      compiler.traverse(root);
      printMain(pw, root);
      printEpilogue(pw);
    }
    return sw.toString();
  }

  private static class MainFuncCompiler extends PostOrderRelNodeVisitor<Void> {
    private final PrintWriter pw;
    private static final String TABLESCAN_TMPL = NEW_LINE_JOINER.join(
        "if (!_sources.containsKey(%2$s))",
        "    throw new RuntimeException(\"Cannot find table \" + %2$s);",
        "Stream _%1$s = topo.newStream(%2$s, _sources.get(%2$s).getProducer());",
        ""
    );

    private static final String TABLEMODIFY_TMPL = NEW_LINE_JOINER.join(
        "Stream _%1$s = _%3$s.each(new Fields(%4$s), _sources.get(%2$s).getConsumer(), new Fields(%5$s));",
        ""
    );
    private static final String TRANSFORMATION_TMPL = NEW_LINE_JOINER.join(
        "Stream _%1$s = _%2$s.each(new Fields(%3$s), %1$s, new Fields(%4$s)).toStream().project(new Fields(%4$s));",
        ""
    );

    private MainFuncCompiler(PrintWriter pw) {
      this.pw = pw;
    }

    @Override
    public Void defaultValue(RelNode n) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void visitFilter(Filter filter) throws Exception {
      visitTransformation(filter);
      return null;
    }

    @Override
    public Void visitTableModify(TableModify modify) throws Exception {
      Preconditions.checkArgument(modify.isInsert(), "Only INSERT statement is supported.");
      String name = RelNodeCompiler.getStageName(modify);
      RelNode input = modify.getInput();
      String inputName = RelNodeCompiler.getStageName(input);
      pw.print(String.format(TABLEMODIFY_TMPL, name, CompilerUtil.escapeJavaString(
          Joiner.on('.').join(modify.getTable().getQualifiedName()), true),
          inputName, getFieldString(input), getFieldString(modify)));
      return null;
    }

    @Override
    public Void visitTableScan(TableScan scan) throws Exception {
      String name = RelNodeCompiler.getStageName(scan);
      pw.print(String.format(TABLESCAN_TMPL, name, CompilerUtil.escapeJavaString(
          Joiner.on('.').join(scan.getTable().getQualifiedName()), true)));
      return null;
    }

    @Override
    public Void visitProject(Project project) throws Exception {
      visitTransformation(project);
      return null;
    }

    private static String getFieldString(RelNode n) {
      int id = n.getId();
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (String f: n.getRowType().getFieldNames()) {
        if (!first) {
          sb.append(", ");
        }
        if (n instanceof TableScan) {
          sb.append(CompilerUtil.escapeJavaString(f, true));
        } else {
          sb.append(CompilerUtil.escapeJavaString(String.format("%d_%s", id, f), true));
        }
        first = false;
      }
      return sb.toString();
    }

    private void visitTransformation(SingleRel node) {
      String name = RelNodeCompiler.getStageName(node);
      RelNode input = node.getInput();
      String inputName = RelNodeCompiler.getStageName(input);
      pw.print(String.format(TRANSFORMATION_TMPL, name, inputName,
          getFieldString(input), getFieldString(node)));
    }
  }

  private void printMain(PrintWriter pw, RelNode root) throws Exception {
    pw.print(INITIALIZER_PROLOGUE);
    MainFuncCompiler compiler = new MainFuncCompiler(pw);
    compiler.traverse(root);
    pw.print(String.format("  this.outputStream = _%s;\n", RelNodeCompiler.getStageName(root)));
    pw.print("  return topo; \n}\n");
  }

  public AbstractTridentProcessor compile(RelNode plan) throws Exception {
    String javaCode = generateJavaSource(plan);
    compilingClassLoader = new CompilingClassLoader(getClass().getClassLoader(),
        PACKAGE_NAME + ".TridentProcessor",
        javaCode, null);
    return (AbstractTridentProcessor) compilingClassLoader.loadClass(PACKAGE_NAME + ".TridentProcessor").newInstance();
  }

  public CompilingClassLoader getCompilingClassLoader() {
    return compilingClassLoader;
  }

  private static void printEpilogue(
      PrintWriter pw) throws Exception {
    pw.print("}\n");
  }

  private static void printPrologue(PrintWriter pw) {
    pw.append(PROLOGUE);
  }
}
