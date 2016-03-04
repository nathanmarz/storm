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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.storm.sql.parser.ColumnConstraint;

import java.util.ArrayList;

import static org.apache.calcite.rel.RelFieldCollation.Direction;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection;
import static org.apache.calcite.sql.validate.SqlMonotonicity.INCREASING;

public class CompilerUtil {
  public static String escapeJavaString(String s, boolean nullMeansNull) {
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
    private int primaryKey = -1;
    private SqlMonotonicity primaryKeyMonotonicity;
    private Statistic stats;

    public TableBuilderInfo field(String name, SqlTypeName type) {
      return field(name, typeFactory.createSqlType(type));
    }

    public TableBuilderInfo field(String name, RelDataType type) {
      fields.add(new FieldType(name, type));
      return this;
    }

    public TableBuilderInfo field(String name, SqlDataTypeSpec type, ColumnConstraint constraint) {
      RelDataType dataType = type.deriveType(typeFactory);
      if (constraint instanceof ColumnConstraint.PrimaryKey) {
        ColumnConstraint.PrimaryKey pk = (ColumnConstraint.PrimaryKey) constraint;
        Preconditions.checkState(primaryKey == -1, "There are more than one primary key in the table");
        primaryKey = fields.size();
        primaryKeyMonotonicity = pk.monotonicity();
      }
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

    public StreamableTable build() {
      final Statistic stat = buildStatistic();
      final Table tbl = new Table() {
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

      return new StreamableTable() {
        @Override
        public Table stream() {
          return tbl;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
          return tbl.getRowType(relDataTypeFactory);
        }

        @Override
        public Statistic getStatistic() {
          return tbl.getStatistic();
        }

        @Override
        public Schema.TableType getJdbcTableType() {
          return Schema.TableType.TABLE;
        }
      };
    }

    private Statistic buildStatistic() {
      if (stats != null || primaryKey == -1) {
        return stats;
      }
      Direction dir = primaryKeyMonotonicity == INCREASING ? ASCENDING : DESCENDING;
      RelFieldCollation collation = new RelFieldCollation(primaryKey, dir, NullDirection.UNSPECIFIED);
      return Statistics.of(fields.size(), ImmutableList.of(ImmutableBitSet.of(primaryKey)),
          ImmutableList.of(RelCollations.of(collation)));
    }
  }
}
