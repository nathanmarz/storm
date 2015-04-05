package org.apache.storm.redis.trident;

import backtype.storm.tuple.ITuple;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;

public class WordCountStoreMapper implements RedisStoreMapper {
    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, "test");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return "test_" + tuple.getString(0);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getInteger(1).toString();
    }
}