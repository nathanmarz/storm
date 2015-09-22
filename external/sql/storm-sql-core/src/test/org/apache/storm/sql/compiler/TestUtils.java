package org.apache.storm.sql.compiler;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;

class TestUtils {
  static class TableBuilderInfo {
    private static class FieldType {
      private static final int NO_PRECISION = -1;
      private final String name;
      private final SqlTypeName type;
      private final int precision;

      private FieldType(String name, SqlTypeName type, int precision) {
        this.name = name;
        this.type = type;
        this.precision = precision;
      }

      private FieldType(String name, SqlTypeName type) {
        this(name, type, NO_PRECISION);
      }
    }

    private final ArrayList<FieldType> fields = new ArrayList<>();
    private final ArrayList<Object[]> rows = new ArrayList<>();
    private Statistic stats;

    TableBuilderInfo field(String name, SqlTypeName type) {
      fields.add(new FieldType(name, type));
      return this;
    }

    TableBuilderInfo field(String name, SqlTypeName type, int precision) {
      fields.add(new FieldType(name, type, precision));
      return this;
    }

    TableBuilderInfo statistics(Statistic stats) {
      this.stats = stats;
      return this;
    }

    TableBuilderInfo rows(Object[] data) {
      rows.add(data);
      return this;
    }

    Table build() {
      final Statistic stat = stats;
      return new Table() {
        @Override
        public RelDataType getRowType(
            RelDataTypeFactory relDataTypeFactory) {
          RelDataTypeFactory.FieldInfoBuilder b = relDataTypeFactory.builder();
          for (FieldType f : fields) {
            if (f.precision == FieldType.NO_PRECISION) {
              b.add(f.name, f.type);
            } else {
              b.add(f.name, f.type, f.precision);
            }
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

  static TableBuilderInfo newTable() {
    return new TableBuilderInfo();
  }
}
