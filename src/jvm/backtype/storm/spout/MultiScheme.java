package backtype.storm.spout;

import java.util.List;

import backtype.storm.tuple.Fields;

public interface MultiScheme {
  public Iterable<List<Object>> deserialize(byte[] ser);
  public Fields getOutputFields();
}
