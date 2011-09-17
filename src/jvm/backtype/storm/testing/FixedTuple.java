package backtype.storm.testing;

import backtype.storm.utils.Utils;
import java.io.Serializable;
import java.util.List;

public class FixedTuple implements Serializable {
    public int stream;
    public List<Object> values;

    public FixedTuple(List<Object> values) {
        this.stream = Utils.DEFAULT_STREAM_ID;
        this.values = values;
    }

    public FixedTuple(int stream, List<Object> values) {
        this.stream = stream;
        this.values = values;
    }

}