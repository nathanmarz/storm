package backtype.storm.utils;

import java.util.Map;

public class LocalStateSerializer implements StateSerializer {
    public byte[] serializeState (Map<Object, Object> val) {
        return Utils.serialize(val);
    }
    
    public Map<Object, Object> deserializeState (byte[] ser) {
        return (Map<Object, Object>) Utils.deserialize(ser);
    }
    
}