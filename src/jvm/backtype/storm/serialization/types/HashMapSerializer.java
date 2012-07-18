package backtype.storm.serialization.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import java.util.HashMap;
import java.util.Map;


public class HashMapSerializer extends MapSerializer {
    @Override
    public Map create(Kryo kryo, Input input, Class<Map> type) {
        return new HashMap();
    }
}
