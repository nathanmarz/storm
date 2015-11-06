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
package org.apache.storm.sql;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.storm.sql.compiler.PlanCompiler;
import org.apache.storm.sql.parser.ColumnConstraint;
import org.apache.storm.sql.parser.ColumnDefinition;
import org.apache.storm.sql.parser.SqlCreateTable;
import org.apache.storm.sql.parser.StormParser;
import org.apache.storm.sql.runtime.*;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.sql.compiler.CompilerUtil.TableBuilderInfo;

class StormSqlImpl extends StormSql {
  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);
  private final SchemaPlus schema = Frameworks.createRootSchema(true);

  @Override
  public void execute(
      Iterable<String> statements, ChannelHandler result)
      throws Exception {
    Map<String, DataSource> dataSources = new HashMap<>();
    for (String sql : statements) {
      StormParser parser = new StormParser(sql);
      SqlNode node = parser.impl().parseSqlStmtEof();
      if (node instanceof SqlCreateTable) {
        handleCreateTable((SqlCreateTable) node, dataSources);
      } else {
        FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(
            schema).build();
        Planner planner = Frameworks.getPlanner(config);
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode tree = planner.convert(validate);
        PlanCompiler compiler = new PlanCompiler(typeFactory);
        AbstractValuesProcessor proc = compiler.compile(tree);
        proc.initialize(dataSources, result);
      }
    }
  }

  private void handleCreateTable(
      SqlCreateTable n, Map<String, DataSource> dataSources) {
    TableBuilderInfo builder = new TableBuilderInfo(typeFactory);
    List<FieldInfo> fields = new ArrayList<>();
    for (ColumnDefinition col : n.fieldList()) {
      builder.field(col.name(), col.type());
      RelDataType dataType = col.type().deriveType(typeFactory);
      Class<?> javaType = (Class<?>)typeFactory.getJavaClass(dataType);
      ColumnConstraint constraint = col.constraint();
      boolean isPrimary = constraint != null && constraint instanceof ColumnConstraint.PrimaryKey;
      fields.add(new FieldInfo(col.name(), javaType, isPrimary));
    }

    Table table = builder.build();
    schema.add(n.tableName(), table);
    DataSource ds = DataSourcesRegistry.construct(n.location(), n
        .inputFormatClass(), n.outputFormatClass(), fields);
    if (ds == null) {
      throw new RuntimeException("Cannot construct data source for " + n
          .tableName());
    } else if (dataSources.containsKey(n.tableName())) {
      throw new RuntimeException("Duplicated definition for table " + n
          .tableName());
    }
    dataSources.put(n.tableName(), ds);
  }
}
