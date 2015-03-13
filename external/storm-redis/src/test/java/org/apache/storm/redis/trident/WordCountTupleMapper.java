package org.apache.storm.redis.trident;

import org.apache.storm.redis.trident.mapper.TridentTupleMapper;
import storm.trident.tuple.TridentTuple;

public class WordCountTupleMapper implements TridentTupleMapper {
    @Override
    public String getKeyFromTridentTuple(TridentTuple tuple) {
        return tuple.getString(0);
    }

    @Override
    public String getValueFromTridentTuple(TridentTuple tuple) {
        return tuple.getInteger(1).toString();
    }
}
