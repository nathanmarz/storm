package backtype.storm.serialization;

import com.esotericsoftware.kryo.ObjectBuffer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KryoValuesDeserializer {
    ObjectBuffer _kryo;
    
    public KryoValuesDeserializer(Map conf) {
        _kryo = SerializationFactory.getKryo(conf);
    }
    
    public List<Object> deserializeFrom(InputStream in) throws IOException { 
        return (List<Object>) _kryo.readObject(in, ArrayList.class);
    }
    
    public List<Object> deserialize(byte[] ser) throws IOException {
        return deserializeFrom(new ByteArrayInputStream(ser));
    }
    
    public Object deserializeObject(byte[] ser) throws IOException {
        return _kryo.readClassAndObject(new ByteArrayInputStream(ser));
    }
}
