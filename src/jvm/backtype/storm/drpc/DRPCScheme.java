package backtype.storm.drpc;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONValue;
import static backtype.storm.utils.Utils.tuple;

public class DRPCScheme implements Scheme {
    public List<Object> deserialize(byte[] bytes) {
        try {
            Map obj = (Map) JSONValue.parse(new String(bytes, "UTF-8"));
            return tuple(obj.get("args"), obj.get("return"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
        return new Fields("args", "return");
    }

}
