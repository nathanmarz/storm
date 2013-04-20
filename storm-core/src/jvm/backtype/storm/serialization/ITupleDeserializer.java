package backtype.storm.serialization;

import backtype.storm.tuple.Tuple;
import java.io.IOException;

public interface ITupleDeserializer {
    Tuple deserialize(byte[] ser);        
}
