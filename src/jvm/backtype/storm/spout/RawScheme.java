package backtype.storm.spout;

import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.List;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

public class RawScheme implements Scheme {
    @Override
    public void prepare(Map stormConf, TopologyContext context, IErrorReporter errorReporter) {
    }

    public List<Object> deserialize(byte[] ser) {
        return tuple(ser);
    }

    public Fields getOutputFields() {
        return new Fields("bytes");
    }
}
