package org.apache.storm.sql.compiler;

import backtype.storm.tuple.Values;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {
  static CalciteState sqlOverDummyTable(String sql)
      throws RelConversionException, ValidationException, SqlParseException {
    SchemaPlus schema = Frameworks.createRootSchema(true);
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl
        (RelDataTypeSystem.DEFAULT);
    Table table = new CompilerUtil.TableBuilderInfo(typeFactory)
        .field("ID", SqlTypeName.INTEGER).build();
    schema.add("FOO", table);
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(
        schema).build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode tree = planner.convert(validate);
    return new CalciteState(schema, tree);
  }

  static class CalciteState {
    final SchemaPlus schema;
    final RelNode tree;

    private CalciteState(SchemaPlus schema, RelNode tree) {
      this.schema = schema;
      this.tree = tree;
    }
  }

  public static class MockDataSource implements DataSource {
    private final ArrayList<Values> RECORDS = new ArrayList<>();

    public MockDataSource() {
      for (int i = 0; i < 5; ++i) {
        RECORDS.add(new Values(i));
      }
    }

    @Override
    public void open(ChannelContext ctx) {
      for (Values v : RECORDS) {
        ctx.emit(v);
      }
      ctx.fireChannelInactive();
    }
  }

  public static class CollectDataChannelHandler implements ChannelHandler {
    private final List<Values> values;

    public CollectDataChannelHandler(List<Values> values) {
      this.values = values;
    }

    @Override
    public void dataReceived(ChannelContext ctx, Values data) {
      values.add(data);
    }

    @Override
    public void channelInactive(ChannelContext ctx) {}

    @Override
    public void exceptionCaught(Throwable cause) {
      throw new RuntimeException(cause);
    }
  }

}
