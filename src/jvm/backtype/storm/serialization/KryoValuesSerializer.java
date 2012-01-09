package backtype.storm.serialization;

import backtype.storm.utils.ListDelegate;
import com.esotericsoftware.kryo.ObjectBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class KryoValuesSerializer {
    ObjectBuffer _kryo;
    ListDelegate _delegate;
    
    public KryoValuesSerializer(Map conf) {
        _kryo = SerializationFactory.getKryo(conf);
        _delegate = new ListDelegate();
    }
    
    public void serializeInto(List<Object> values, OutputStream out) throws IOException {
        // this ensures that list of values is always written the same way, regardless
        // of whether it's a java collection or one of clojure's persistent collections 
        // (which have different serializers)
        // Doing this lets us deserialize as ArrayList and avoid writing the class here
        _delegate.setDelegate(values);
        _kryo.writeObject(out, _delegate);        
    }
    
    public byte[] serialize(List<Object> values) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializeInto(values, out);
        return out.toByteArray();
    }
    
    public byte[] serializeObject(Object obj) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        _kryo.writeClassAndObject(out, obj);
        return out.toByteArray();
    }
}
