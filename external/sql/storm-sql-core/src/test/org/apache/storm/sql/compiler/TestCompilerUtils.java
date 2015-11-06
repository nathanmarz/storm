package org.apache.storm.sql.compiler;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
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
    Table table = new CompilerUtil.TableBuilderInfo(typeFactory)
        .field("ID", SqlTypeName.INTEGER).build();
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
