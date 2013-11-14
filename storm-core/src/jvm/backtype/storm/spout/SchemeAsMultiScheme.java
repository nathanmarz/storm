package backtype.storm.spout;

import java.util.Arrays;
import java.util.List;

import backtype.storm.tuple.Fields;

public class SchemeAsMultiScheme implements MultiScheme {
  public final Scheme scheme;

  public SchemeAsMultiScheme(Scheme scheme) {
    this.scheme = scheme;
  }

  @Override public Iterable<List<Object>> deserialize(final byte[] ser) {
    List<Object> o = scheme.deserialize(ser);
    if(o == null) return null;
    else return Arrays.asList(o);
  }

  @Override public Fields getOutputFields() {
    return scheme.getOutputFields();
  }
}
