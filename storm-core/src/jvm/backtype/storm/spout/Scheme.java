package backtype.storm.spout;

import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.List;


public interface Scheme extends Serializable {
    public List<Object> deserialize(byte[] ser);
    public Fields getOutputFields();
}
