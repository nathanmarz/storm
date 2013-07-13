package backtype.storm.utils;

import java.util.Map;

/**
 * Interface for serializing state, for example, when the `Supervisor`
 * serializes `LocalState`.
 */
public interface StateSerializer {
    public byte[] serializeState (Map<Object, Object> val);
    public Map<Object, Object> deserializeState (byte[] ser);
}