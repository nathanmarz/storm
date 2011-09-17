package backtype.storm.spout;

import backtype.storm.tuple.Fields;
import java.util.List;
import static backtype.storm.utils.Utils.tuple;

public class RawScheme implements Scheme {
    public List<Object> deserialize(byte[] ser) {
        return tuple(ser);
    }

    public Fields getOutputFields() {
        return new Fields("bytes");
    }
}
