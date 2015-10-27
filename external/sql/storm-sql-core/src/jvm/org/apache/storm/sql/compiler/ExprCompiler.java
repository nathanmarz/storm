/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.compiler;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.IdentityHashMap;
import java.util.Map;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE_INTEGER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;

/**
 * Compile RexNode on top of the Tuple abstraction.
 */
class ExprCompiler implements RexVisitor<String> {
  private final PrintWriter pw;
  private final Map<RexNode, String> expr = new IdentityHashMap<>();
  private final JavaTypeFactory typeFactory;
  private static final ImpTable IMP_TABLE = new ImpTable();

  ExprCompiler(PrintWriter pw, JavaTypeFactory typeFactory) {
    this.pw = pw;
    this.typeFactory = typeFactory;
  }

  @Override
  public String visitInputRef(RexInputRef rexInputRef) {
    if (expr.containsKey(rexInputRef)) {
      return expr.get(rexInputRef);
    }
    String name = reserveName(rexInputRef);
    String typeName = javaTypeName(rexInputRef);
    pw.print(String.format("%s %s = (%s)(_data.get(%d));\n", typeName, name,
                           typeName, rexInputRef.getIndex()));
    expr.put(rexInputRef, name);
    return name;
  }

  @Override
  public String visitLocalRef(RexLocalRef rexLocalRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitLiteral(RexLiteral rexLiteral) {
    Object v = rexLiteral.getValue();
    RelDataType ty = rexLiteral.getType();
    switch(rexLiteral.getTypeName()) {
      case BOOLEAN:
        return v.toString();
      case CHAR:
        return CompilerUtil.escapeJavaString(((NlsString) v).getValue(), true);
      case NULL:
        return "null";
      case DOUBLE:
      case BIGINT:
      case DECIMAL:
        switch (ty.getSqlTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
            return Long.toString(((BigDecimal) v).longValueExact());
          case BIGINT:
            return Long.toString(((BigDecimal)v).longValueExact()) + 'L';
          case DECIMAL:
          case FLOAT:
          case REAL:
          case DOUBLE:
            return Util.toScientificNotation((BigDecimal) v);
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return null;
  }

  @Override
  public String visitCall(RexCall rexCall) {
    return IMP_TABLE.compile(this, rexCall);
  }

  @Override
  public String visitOver(RexOver rexOver) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitCorrelVariable(
      RexCorrelVariable rexCorrelVariable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitDynamicParam(
      RexDynamicParam rexDynamicParam) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitRangeRef(RexRangeRef rexRangeRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitFieldAccess(
      RexFieldAccess rexFieldAccess) {
    throw new UnsupportedOperationException();
  }

  private String javaTypeName(RexNode node) {
    Type ty = typeFactory.getJavaClass(node.getType());
    return ((Class<?>)ty).getCanonicalName();
  }

  private String reserveName(RexNode node) {
    String name = "t" + expr.size();
    expr.put(node, name);
    return name;
  }

  private interface CallExprPrinter {
    String translate(ExprCompiler compiler, RexCall call);
  }

  /**
   * Inspired by Calcite's RexImpTable, the ImpTable class maps the operators
   * to their corresponding implementation that generates the expressions in
   * the format of Java source code.
   */
  private static class ImpTable {
    private final Map<SqlOperator, CallExprPrinter> translators;

    private ImpTable() {
      ImmutableMap.Builder<SqlOperator, CallExprPrinter> builder =
          ImmutableMap.builder();
      builder.put(infixBinary(LESS_THAN, "<"))
          .put(infixBinary(LESS_THAN_OR_EQUAL, "<="))
          .put(infixBinary(GREATER_THAN, ">"))
          .put(infixBinary(GREATER_THAN_OR_EQUAL, ">="))
          .put(infixBinary(PLUS, "+"))
          .put(infixBinary(MINUS, "-"))
          .put(infixBinary(MULTIPLY, "*"))
          .put(infixBinary(DIVIDE, "/"))
          .put(infixBinary(DIVIDE_INTEGER, "/"))
          .put(AND, AND_EXPR)
          .put(OR, OR_EXPR)
          .put(NOT, NOT_EXPR);
      this.translators = builder.build();
    }

    private String compile(ExprCompiler compiler, RexCall call) {
      SqlOperator op = call.getOperator();
      CallExprPrinter printer = translators.get(op);
      if (printer == null) {
        throw new UnsupportedOperationException();
      } else {
        return printer.translate(compiler, call);
      }
    }

    private Map.Entry<SqlOperator, CallExprPrinter> infixBinary
        (SqlOperator op, final String javaOperator) {
      CallExprPrinter trans = new CallExprPrinter() {
        @Override
        public String translate(
            ExprCompiler compiler, RexCall call) {
          int size = call.getOperands().size();
          assert size == 2;
          String[] ops = new String[size];
          for (int i = 0; i < size; ++i) {
            ops[i] = call.getOperands().get(i).accept(compiler);
          }
          return String.format("%s %s %s", ops[0], javaOperator, ops[1]);
        }
      };
      return new AbstractMap.SimpleImmutableEntry<>(op, trans);
    }

    private static final CallExprPrinter AND_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName(call);
        PrintWriter pw = compiler.pw;
        pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call),
                               val));
        String lhs = call.getOperands().get(0).accept(compiler);
        pw.print(String.format("if (!(%2$s)) { %1$s = false; }\n", val, lhs));
        pw.print("else {\n");
        String rhs = call.getOperands().get(1).accept(compiler);
        pw.print(String.format("  %1$s = %2$s;\n}\n", val, rhs));
        return val;
      }
    };

    private static final CallExprPrinter OR_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName(call);
        PrintWriter pw = compiler.pw;
        pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call),
                               val));
        String lhs = call.getOperands().get(0).accept(compiler);
        pw.print(String.format("if (%2$s) { %1$s = true; }\n", val, lhs));
        pw.print("else {\n");
        String rhs = call.getOperands().get(1).accept(compiler);
        pw.print(String.format("  %1$s = %2$s;\n}\n", val, rhs));
        return val;
      }
    };

    private static final CallExprPrinter NOT_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName(call);
        PrintWriter pw = compiler.pw;
        String lhs = call.getOperands().get(0).accept(compiler);
        pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call),
                               val));
        pw.print(String.format("%1$s = !(%2$s);\n", val, lhs));
        return val;
      }
    };
  }

}
