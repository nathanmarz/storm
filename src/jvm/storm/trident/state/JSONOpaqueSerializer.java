package storm.trident.state;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.json.simple.JSONValue;


public class JSONOpaqueSerializer<T> implements Serializer<OpaqueValue<T>> {

    @Override
    public byte[] serialize(OpaqueValue<T> obj) {
        // not generic param T as currTxid is a long
        List toSer = new ArrayList(3);
        toSer.add(obj.currTxid);
        toSer.add(obj.curr);
        toSer.add(obj.prev);
        try {
            return JSONValue.toJSONString(toSer).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OpaqueValue<T> deserialize(byte[] b) {
        try {
            String s = new String(b, "UTF-8");
            List deser = List.class.cast(JSONValue.parse(s));
            // not generic param T as currTxid is a long
            return new OpaqueValue((Long) deser.get(0), deser.get(1), deser.get(2));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
