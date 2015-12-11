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
package org.apache.storm.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.net.URI;
import java.util.List;

public class SqlCreateTable extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator(
      "CREATE_TABLE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... o) {
      assert functionQualifier == null;
      return new SqlCreateTable(pos, (SqlIdentifier) o[0], (SqlNodeList) o[1],
                                o[2], o[3], o[4], o[5], o[6]);
    }

    @Override
    public void unparse(
        SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlCreateTable t = (SqlCreateTable) call;
      UnparseUtil u = new UnparseUtil(writer, leftPrec, rightPrec);
      u.keyword("CREATE", "EXTERNAL", "TABLE").node(t.tblName).nodeList(
          t.fieldList);
      if (t.inputFormatClass != null && t.outputFormatClass != null) {
        u.keyword("STORED", "AS", "INPUTFORMAT").node(
            t.inputFormatClass).keyword("OUTPUTFORMAT").node(
            t.outputFormatClass);
      }
      u.keyword("LOCATION").node(t.location);
      if (t.properties != null) {
        u.keyword("TBLPROPERTIES").node(t.properties);
      }
      if (t.query != null) {
        u.keyword("AS").node(t.query);
      }
    }
  };

  private final SqlIdentifier tblName;
  private final SqlNodeList fieldList;
  private final SqlNode inputFormatClass;
  private final SqlNode outputFormatClass;
  private final SqlNode location;
  private final SqlNode properties;
  private final SqlNode query;

  public SqlCreateTable(
      SqlParserPos pos, SqlIdentifier tblName, SqlNodeList fieldList,
      SqlNode inputFormatClass, SqlNode outputFormatClass, SqlNode location,
      SqlNode properties, SqlNode query) {
    super(pos);
    this.tblName = tblName;
    this.fieldList = fieldList;
    this.inputFormatClass = inputFormatClass;
    this.outputFormatClass = outputFormatClass;
    this.location = location;
    this.properties = properties;
    this.query = query;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    getOperator().unparse(writer, this, leftPrec, rightPrec);
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tblName, fieldList, inputFormatClass,
                                    outputFormatClass, location, properties,
                                    query);
  }

  public String tableName() {
    return tblName.toString();
  }

  public URI location() {
    return URI.create(SqlLiteral.stringValue(location));
  }

  public String inputFormatClass() {
    return getString(inputFormatClass);
  }

  public String outputFormatClass() {
    return getString(outputFormatClass);
  }

  public String properties() {
    return getString(properties);
  }

  private String getString(SqlNode n) {
    return n == null ? null : SqlLiteral.stringValue(n);
  }

  @SuppressWarnings("unchecked")
  public List<ColumnDefinition> fieldList() {
    return (List<ColumnDefinition>)((List<? extends SqlNode>)fieldList.getList());
  }

}
