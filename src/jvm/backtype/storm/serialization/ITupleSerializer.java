package backtype.storm.serialization;

import backtype.storm.tuple.Tuple;
import java.io.IOException;


public interface ITupleSerializer {
    byte[] serialize(Tuple tuple) throws IOException;
    long crc32(Tuple tuple);
}
