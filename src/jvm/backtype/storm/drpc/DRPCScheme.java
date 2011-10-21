package backtype.storm.drpc;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONValue;

public class DRPCScheme implements Scheme {
    public List<Object> deserialize(byte[] bytes) {
        try {
            Map obj = (Map) JSONValue.parse(new String(bytes, "UTF-8"));
            return new Values(obj.get("args"), obj.get("return"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
        return new Fields("args", "return");
    }

}
