package backtype.storm.spout;

import java.util.List;
import java.util.Map;

import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;


import static backtype.storm.utils.Utils.tuple;
import static java.util.Arrays.asList;

public class RawMultiScheme implements MultiScheme {
  @Override
  public void prepare(Map stormConf, TopologyContext context, IErrorReporter errorReporter) {
  }

  @Override
  public Iterable<List<Object>> deserialize(byte[] ser) {
    return asList(tuple(ser));
  }

  @Override
  public Fields getOutputFields() {
    return new Fields("bytes");
  }
}
