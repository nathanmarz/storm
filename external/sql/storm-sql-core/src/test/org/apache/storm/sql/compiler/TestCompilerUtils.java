/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.compiler;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestCompilerUtils {

    public static class MyPlus {
        public static Integer eval(Integer x, Integer y) {
            return x + y;
        }
    }

    public static CalciteState sqlOverDummyTable(String sql)
            throws RelConversionException, ValidationException, SqlParseException {
        SchemaPlus schema = Frameworks.createRootSchema(true);
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl
                (RelDataTypeSystem.DEFAULT);
        StreamableTable streamableTable = new CompilerUtil.TableBuilderInfo(typeFactory)
                .field("ID", SqlTypeName.INTEGER)
                .field("NAME", typeFactory.createType(String.class))
                .field("ADDR", typeFactory.createType(String.class))
                .build();
        Table table = streamableTable.stream();
        schema.add("FOO", table);
        schema.add("BAR", table);
        FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(
                schema).build();
        Planner planner = Frameworks.getPlanner(config);
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode tree = planner.convert(validate);
        return new CalciteState(schema, tree);
    }

    public static CalciteState sqlOverNestedTable(String sql)
            throws RelConversionException, ValidationException, SqlParseException {
        SchemaPlus schema = Frameworks.createRootSchema(true);
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl
                (RelDataTypeSystem.DEFAULT);

        StreamableTable streamableTable = new CompilerUtil.TableBuilderInfo(typeFactory)
                .field("ID", SqlTypeName.INTEGER)
                .field("MAPFIELD", SqlTypeName.ANY)
                .field("NESTEDMAPFIELD", SqlTypeName.ANY)
                .field("ARRAYFIELD", SqlTypeName.ANY)
                .build();
        Table table = streamableTable.stream();
        schema.add("FOO", table);
        schema.add("BAR", table);
        schema.add("MYPLUS", ScalarFunctionImpl.create(MyPlus.class, "eval"));
        List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
        sqlOperatorTables.add(SqlStdOperatorTable.instance());
        sqlOperatorTables.add(new CalciteCatalogReader(CalciteSchema.from(schema),
                                                       false,
                                                       Collections.<String>emptyList(), typeFactory));
        SqlOperatorTable chainedSqlOperatorTable = new ChainedSqlOperatorTable(sqlOperatorTables);
        FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(
                schema).operatorTable(chainedSqlOperatorTable).build();
        Planner planner = Frameworks.getPlanner(config);
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode tree = planner.convert(validate);
        return new CalciteState(schema, tree);
    }

    public static class CalciteState {
        final SchemaPlus schema;
        final RelNode tree;

        private CalciteState(SchemaPlus schema, RelNode tree) {
            this.schema = schema;
            this.tree = tree;
        }

        public SchemaPlus schema() {
            return schema;
        }

        public RelNode tree() {
            return tree;
        }
    }
}
