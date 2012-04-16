package backtype.storm.testing;

import backtype.storm.utils.Utils;
import java.io.Serializable;
import java.util.List;

public class FixedTuple implements Serializable {
    public String stream;
    public List<Object> values;

    public FixedTuple(List<Object> values) {
        this.stream = Utils.DEFAULT_STREAM_ID;
        this.values = values;
    }

    public FixedTuple(String stream, List<Object> values) {
        this.stream = stream;
        this.values = values;
    }

    @Override
    public String toString() {
        return stream + ":" + "<" + values.toString() + ">";
    }
}