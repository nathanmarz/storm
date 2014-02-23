package storm.kafka;

import backtype.storm.spout.Scheme;

import java.util.List;

public interface KeyValueScheme extends Scheme {

    public List<Object> deserializeKeyAndValue(byte[] key, byte[] value);

}
