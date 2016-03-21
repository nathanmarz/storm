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
package org.apache.storm.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import java.util.List;

public class SqlCreateFunction extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator(
            "CREATE_FUNCTION", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
                SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... o) {
            assert functionQualifier == null;
            return new SqlCreateFunction(pos, (SqlIdentifier) o[0], o[1], o[2]);
        }

        @Override
        public void unparse(
                SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            SqlCreateFunction t = (SqlCreateFunction) call;
            UnparseUtil u = new UnparseUtil(writer, leftPrec, rightPrec);
            u.keyword("CREATE", "FUNCTION").node(t.functionName).keyword("AS").node(t.className);
            if (t.jarName != null) {
                u.keyword("USING", "JAR").node(t.jarName);
            }
        }
    };

    private final SqlIdentifier functionName;
    private final SqlNode className;
    private final SqlNode jarName;

    public SqlCreateFunction(SqlParserPos pos, SqlIdentifier functionName, SqlNode className, SqlNode jarName) {
        super(pos);
        this.functionName = functionName;
        this.className = className;
        this.jarName = jarName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(functionName, className);
    }


    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        getOperator().unparse(writer, this, leftPrec, rightPrec);
    }

    public String functionName() {
        return functionName.toString();
    }

    public String className() {
        return ((NlsString)SqlLiteral.value(className)).getValue();
    }

    public String jarName() {
        return jarName == null ? null : ((NlsString)SqlLiteral.value(jarName)).getValue();
    }
}
