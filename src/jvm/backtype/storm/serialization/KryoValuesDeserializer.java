package backtype.storm.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KryoValuesDeserializer {
    Kryo _kryo;
    Input _kryoInput;
    
    public KryoValuesDeserializer(Map conf) {
        _kryo = SerializationFactory.getKryo(conf);
        _kryoInput = new Input(1);
    }
    
    public List<Object> deserializeFrom(Input input) {
        return (List<Object>) _kryo.readObject(input, ArrayList.class);
    }
    
    public List<Object> deserialize(byte[] ser) throws IOException {
        _kryoInput.setBuffer(ser);
        return deserializeFrom(_kryoInput);
    }
    
    public Object deserializeObject(byte[] ser) throws IOException {
        _kryoInput.setBuffer(ser);
        return _kryo.readClassAndObject(_kryoInput);
    }
}
