package storm.kafka;

import backtype.storm.tuple.Values;
import com.google.common.collect.ImmutableMap;

import java.util.List;

public class StringKeyValueScheme extends StringScheme implements KeyValueScheme {

    @Override
    public List<Object> deserializeKeyAndValue(byte[] key, byte[] value) {
        if ( key == null ) {
            return deserialize(value);
        }
        String keyString = StringScheme.deserializeString(key);
        String valueString = StringScheme.deserializeString(value);
        return new Values(ImmutableMap.of(keyString, valueString));
    }

}
