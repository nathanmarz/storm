package org.apache.storm.redis.trident;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;

import java.util.ArrayList;
import java.util.List;

public class WordCountLookupMapper implements RedisLookupMapper {
    @Override
    public List<Values> toTuple(ITuple input, Object value) {
        List<Values> values = new ArrayList<Values>();
        values.add(new Values(getKeyFromTuple(input), value));
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "value"));
    }

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