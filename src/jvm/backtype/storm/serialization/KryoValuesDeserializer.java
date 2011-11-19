package backtype.storm.serialization;

import com.esotericsoftware.kryo.ObjectBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KryoValuesDeserializer {
    ObjectBuffer _kryo;
    
    public KryoValuesDeserializer(Map conf) {
        _kryo = KryoFactory.getKryo(conf);
    }
    
    public List<Object> deserializeFrom(InputStream in) throws IOException { 
        return (List<Object>) _kryo.readObject(in, ArrayList.class);
    }   
}
