package org.apache.storm.redis.trident;

import backtype.storm.tuple.ITuple;
import org.apache.storm.redis.common.mapper.TupleMapper;

public class WordCountTupleMapper implements TupleMapper {
    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getString(0);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getInteger(1).toString();
    }
}
