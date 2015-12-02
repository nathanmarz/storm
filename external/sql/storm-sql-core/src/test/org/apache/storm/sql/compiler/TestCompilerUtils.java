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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.*;

public class TestCompilerUtils {
    public static CalciteState sqlOverDummyTable(String sql)
        throws RelConversionException, ValidationException, SqlParseException {
        SchemaPlus schema = Frameworks.createRootSchema(true);
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl
            (RelDataTypeSystem.DEFAULT);
        StreamableTable streamableTable = new CompilerUtil.TableBuilderInfo(typeFactory)
            .field("ID", SqlTypeName.INTEGER).build();
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

    public static class CalciteState {
        final SchemaPlus schema;
        final RelNode tree;

        private CalciteState(SchemaPlus schema, RelNode tree) {
            this.schema = schema;
            this.tree = tree;
        }

        public SchemaPlus schema() { return schema; }
        public RelNode tree() { return tree; }
    }
}
