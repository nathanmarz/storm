package org.apache.storm.elasticsearch.common;

import backtype.storm.tuple.ITuple;

public class DefaultEsTupleMapper implements EsTupleMapper {
    @Override
    public String getSource(ITuple tuple) {
        return tuple.getStringByField("source");
    }

    @Override
    public String getIndex(ITuple tuple) {
        return tuple.getStringByField("index");
    }

    @Override
    public String getType(ITuple tuple) {
        return tuple.getStringByField("type");
    }

    @Override
    public String getId(ITuple tuple) {
        return tuple.getStringByField("id");
    }
}
