package backtype.storm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.List;
import java.util.Map;


public interface Scheme extends Serializable {
    void prepare(Map stormConf, TopologyContext context);
    public List<Object> deserialize(byte[] ser);
    public Fields getOutputFields();
}
