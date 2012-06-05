package backtype.storm.serialization;

import backtype.storm.tuple.Tuple;


public interface ITupleSerializer {
    byte[] serialize(Tuple tuple);
//    long crc32(Tuple tuple);
}
