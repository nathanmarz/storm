package storm.trident.state;

import java.io.UnsupportedEncodingException;
import org.json.simple.JSONValue;


public class JSONNonTransactionalSerializer implements Serializer {

    @Override
    public byte[] serialize(Object obj) {
        try {
            return JSONValue.toJSONString(obj).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object deserialize(byte[] b) {
        try {
            return JSONValue.parse(new String(b, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
}
