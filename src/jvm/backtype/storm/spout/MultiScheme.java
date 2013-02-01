package backtype.storm.spout;

import backtype.storm.task.TopologyContext;
import java.util.List;
import java.io.Serializable;

import backtype.storm.tuple.Fields;
import java.util.Map;

public interface MultiScheme extends Serializable {
  void prepare(Map stormConf, TopologyContext context);
  public Iterable<List<Object>> deserialize(byte[] ser);
  public Fields getOutputFields();
}
