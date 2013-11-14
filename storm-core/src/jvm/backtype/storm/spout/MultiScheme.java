package backtype.storm.spout;

import java.util.List;
import java.io.Serializable;

import backtype.storm.tuple.Fields;

public interface MultiScheme extends Serializable {
  public Iterable<List<Object>> deserialize(byte[] ser);
  public Fields getOutputFields();
}
