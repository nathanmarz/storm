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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.ArrayList;

public class CompilerUtil {
  static String escapeJavaString(String s, boolean nullMeansNull) {
      if(s == null) {
        return nullMeansNull ? "null" : "\"\"";
      } else {
        String s1 = Util.replace(s, "\\", "\\\\");
        String s2 = Util.replace(s1, "\"", "\\\"");
        String s3 = Util.replace(s2, "\n\r", "\\n");
        String s4 = Util.replace(s3, "\n", "\\n");
        String s5 = Util.replace(s4, "\r", "\\r");
        return "\"" + s5 + "\"";
      }
  }

  public static class TableBuilderInfo {
    private final RelDataTypeFactory typeFactory;

    public TableBuilderInfo(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    private static class FieldType {
      private final String name;
      private final RelDataType relDataType;

      private FieldType(String name, RelDataType relDataType) {
        this.name = name;
        this.relDataType = relDataType;
      }

    }

    private final ArrayList<FieldType> fields = new ArrayList<>();
    private final ArrayList<Object[]> rows = new ArrayList<>();
    private Statistic stats;

    public TableBuilderInfo field(String name, SqlTypeName type) {
      RelDataType dataType = typeFactory.createSqlType(type);
      fields.add(new FieldType(name, dataType));
      return this;
    }

    public TableBuilderInfo field(String name, SqlTypeName type, int
        precision) {
      RelDataType dataType = typeFactory.createSqlType(type, precision);
      fields.add(new FieldType(name, dataType));
      return this;
    }

    public TableBuilderInfo field(
        String name, SqlDataTypeSpec type) {
      RelDataType dataType = type.deriveType(typeFactory);
      fields.add(new FieldType(name, dataType));
      return this;
    }

    public TableBuilderInfo statistics(Statistic stats) {
      this.stats = stats;
      return this;
    }

    @VisibleForTesting
    public TableBuilderInfo rows(Object[] data) {
      rows.add(data);
      return this;
    }

    public Table build() {
      final Statistic stat = stats;
      return new Table() {
        @Override
        public RelDataType getRowType(
            RelDataTypeFactory relDataTypeFactory) {
          RelDataTypeFactory.FieldInfoBuilder b = relDataTypeFactory.builder();
          for (FieldType f : fields) {
            b.add(f.name, f.relDataType);
          }
          return b.build();
        }

        @Override
        public Statistic getStatistic() {
          return stat != null ? stat : Statistics.of(rows.size(),
                                                     ImmutableList.<ImmutableBitSet>of());
        }

        @Override
        public Schema.TableType getJdbcTableType() {
          return Schema.TableType.TABLE;
        }
      };
    }
  }
}
