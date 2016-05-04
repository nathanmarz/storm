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

import com.google.common.base.Joiner;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.storm.sql.compiler.CompilerUtil;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.runtime.AbstractValuesProcessor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLClassLoader;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

public class PlanCompiler {
  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");
  private static final String PACKAGE_NAME = "org.apache.storm.sql.generated";
  private static final String PROLOGUE = NEW_LINE_JOINER.join(
      "// GENERATED CODE", "package " + PACKAGE_NAME + ";", "",
      "import java.util.Iterator;", "import java.util.Map;",
      "import org.apache.storm.tuple.Values;",
      "import org.apache.storm.sql.runtime.AbstractChannelHandler;",
      "import org.apache.storm.sql.runtime.Channels;",
      "import org.apache.storm.sql.runtime.ChannelContext;",
      "import org.apache.storm.sql.runtime.ChannelHandler;",
      "import org.apache.storm.sql.runtime.DataSource;",
      "import org.apache.storm.sql.runtime.AbstractValuesProcessor;",
      "public final class Processor extends AbstractValuesProcessor {", "");
  private static final String INITIALIZER_PROLOGUE = NEW_LINE_JOINER.join(
      "  @Override",
      "  public void initialize(Map<String, DataSource> data,",
      "                         ChannelHandler result) {",
      "    ChannelContext r = Channels.chain(Channels.voidContext(), result);",
      ""
  );

  private final JavaTypeFactory typeFactory;

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

  private void printMain(PrintWriter pw, RelNode root) {
    Set<TableScan> tables = new HashSet<>();
    pw.print(INITIALIZER_PROLOGUE);
    chainOperators(pw, root, tables);
    for (TableScan n : tables) {
      String escaped = CompilerUtil.escapeJavaString(
          Joiner.on('.').join(n.getTable().getQualifiedName()), true);
      String r = NEW_LINE_JOINER.join(
          "    if (!data.containsKey(%1$s))",
          "      throw new RuntimeException(\"Cannot find table \" + %1$s);",
          "  data.get(%1$s).open(CTX_%2$d);",
          "");
      pw.print(String.format(r, escaped, n.getId()));
    }
    pw.print("  }\n");
  }

  private void chainOperators(
      PrintWriter pw, RelNode root, Set<TableScan> tables) {
    String lastCtx = "r";
    Queue<RelNode> q = new ArrayDeque<>();
    q.add(root);
    RelNode n;
    while ((n = q.poll()) != null) {
      pw.print(
          String.format("    ChannelContext CTX_%d = Channels.chain(%2$s, %3$s);\n",
              n.getId(), lastCtx, RelNodeCompiler.getStageName(n)));
      lastCtx = String.format("CTX_%d", n.getId());

      if (n instanceof TableScan) {
        tables.add((TableScan)n);
      }

      for (RelNode i : n.getInputs()) {
        q.add(i);
      }
    }
  }

  public AbstractValuesProcessor compile(RelNode plan) throws Exception {
    String javaCode = generateJavaSource(plan);
    ClassLoader cl = new CompilingClassLoader(getClass().getClassLoader(),
                                              PACKAGE_NAME + ".Processor",
                                              javaCode, null);
    return (AbstractValuesProcessor) cl.loadClass(
        PACKAGE_NAME + ".Processor").newInstance();
  }

  private static void printEpilogue(
      PrintWriter pw) throws Exception {
    pw.print("}\n");
  }

  private static void printPrologue(PrintWriter pw) {
    pw.append(PROLOGUE);
  }
}
