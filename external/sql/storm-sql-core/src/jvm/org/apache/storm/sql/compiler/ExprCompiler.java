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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_TRUE;
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
  private final JavaTypeFactory typeFactory;
  private static final ImpTable IMP_TABLE = new ImpTable();
  private int nameCount;

  ExprCompiler(PrintWriter pw, JavaTypeFactory typeFactory) {
    this.pw = pw;
    this.typeFactory = typeFactory;
  }

  @Override
  public String visitInputRef(RexInputRef rexInputRef) {
    String name = reserveName(rexInputRef);
    String typeName = javaTypeName(rexInputRef);
    pw.print(String.format("%s %s = (%s)(_data.get(%d));\n", typeName, name,
                           typeName, rexInputRef.getIndex()));
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
    String name = "t" + ++nameCount;
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
          .put(expect(IS_NULL, null))
          .put(expectNot(IS_NOT_NULL, null))
          .put(expect(IS_TRUE, true))
          .put(expectNot(IS_NOT_TRUE, true))
          .put(expect(IS_FALSE, false))
          .put(expectNot(IS_NOT_FALSE, false))
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

    private Map.Entry<SqlOperator, CallExprPrinter> expect(
        SqlOperator op, final Boolean expect) {
      return expect0(op, expect, false);
    }

    private Map.Entry<SqlOperator, CallExprPrinter> expectNot(
        SqlOperator op, final Boolean expect) {
      return expect0(op, expect, true);
    }

    private Map.Entry<SqlOperator, CallExprPrinter> expect0(
        SqlOperator op, final Boolean expect, final boolean negate) {
      CallExprPrinter trans = new CallExprPrinter() {
        @Override
        public String translate(
            ExprCompiler compiler, RexCall call) {
          assert call.getOperands().size() == 1;
          String val = compiler.reserveName(call);
          RexNode operand = call.getOperands().get(0);
          boolean nullable = operand.getType().isNullable();
          String op = operand.accept(compiler);
          PrintWriter pw = compiler.pw;
          if (!nullable) {
            if (expect == null) {
              pw.print(String.format("boolean %s = %b;\n", val, !negate));
            } else {
              pw.print(String.format("boolean %s = %s == %b;\n", val, op,
                                     expect ^ negate));
            }
          } else {
            String expr;
            if (expect == null) {
              expr = String.format("%s == null", op);
            } else {
              expr = String.format("%s == Boolean.%s", op, expect ? "TRUE" :
                  "FALSE");
            }
            if (negate) {
              expr = String.format("!(%s)", expr);
            }
            pw.print(String.format("boolean %s = %s;\n", val, expr));
          }
          return val;
        }
      };
      return new AbstractMap.SimpleImmutableEntry<>(op, trans);
    }


    // If any of the arguments are false, result is false;
    // else if any arguments are null, result is null;
    // else true.
    private static final CallExprPrinter AND_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName(call);
        PrintWriter pw = compiler.pw;
        pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call),
                               val));
        RexNode op0 = call.getOperands().get(0);
        RexNode op1 = call.getOperands().get(1);
        boolean lhsNullable = op0.getType().isNullable();
        boolean rhsNullable = op1.getType().isNullable();
        String lhs = op0.accept(compiler);
        if (!lhsNullable) {
          pw.print(String.format("if (!(%2$s)) { %1$s = false; }\n", val, lhs));
          pw.print("else {\n");
          String rhs = op1.accept(compiler);
          pw.print(String.format("  %1$s = %2$s;\n}\n", val, rhs));
        } else {
          String foldedLHS = foldNullExpr(
              String.format("%1$s == null || %1$s", lhs), "true", lhs);
          pw.print(String.format("if (%s) {\n", foldedLHS));
          String rhs = op1.accept(compiler);
          String s;
          if (rhsNullable) {
            s = foldNullExpr(
                String.format("(%2$s != null && !(%2$s)) ? false : %1$s", lhs,
                              rhs),
                "null", rhs);
          } else {
            s = String.format("!(%2$s) ? false : %1$s", lhs, rhs);
          }
          pw.print(String.format("  %1$s = %2$s;\n", val, s));
          pw.print(String.format("} else { %1$s = false; }\n", val));
        }
        return val;
      }
    };

    // If any of the arguments are true, result is true;
    // else if any arguments are null, result is null;
    // else false.
    private static final CallExprPrinter OR_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName(call);
        PrintWriter pw = compiler.pw;
        pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call),
                               val));
        RexNode op0 = call.getOperands().get(0);
        RexNode op1 = call.getOperands().get(1);
        boolean lhsNullable = op0.getType().isNullable();
        boolean rhsNullable = op1.getType().isNullable();
        String lhs = op0.accept(compiler);
        if (!lhsNullable) {
          pw.print(String.format("if (%2$s) { %1$s = true; }\n", val, lhs));
          pw.print("else {\n");
          String rhs = op1.accept(compiler);
          pw.print(String.format("  %1$s = %2$s;\n}\n", val, rhs));
        } else {
          String foldedLHS = foldNullExpr(
              String.format("%1$s == null || !(%1$s)", lhs), "true", lhs);
          pw.print(String.format("if (%s) {\n", foldedLHS));
          String rhs = op1.accept(compiler);
          String s;
          if (rhsNullable) {
            s = foldNullExpr(
                String.format("(%2$s != null && %2$s) ? true : %1$s", lhs, rhs),
                "null", rhs);
          } else {
            s = String.format("%2$s ? %2$s : %1$s", lhs, rhs);
          }
          pw.print(String.format("  %1$s = %2$s;\n", val, s));
          pw.print(String.format("} else { %1$s = true; }\n", val));
        }
        return val;
      }
    };

    private static final CallExprPrinter NOT_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName(call);
        PrintWriter pw = compiler.pw;
        RexNode op = call.getOperands().get(0);
        String lhs = op.accept(compiler);
        boolean nullable = call.getType().isNullable();
        pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call),
                               val));
        if (!nullable) {
          pw.print(String.format("%1$s = !(%2$s);\n", val, lhs));
        } else {
          String s = foldNullExpr(
              String.format("%1$s == null ? null : !(%1$s)", lhs), "null", lhs);
          pw.print(String.format("%1$s = %2$s;\n", val, s));
        }
        return val;
      }
    };

    private static String foldNullExpr(String notNullExpr, String
        nullExpr, String op) {
      if (op.equals("null")) {
        return nullExpr;
      } else {
        return notNullExpr;
      }
    }
  }

}
