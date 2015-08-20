package org.apache.storm.elasticsearch;

import org.elasticsearch.action.get.GetRequest;

import backtype.storm.tuple.Tuple;

public interface ElasticsearchGetRequest {

    GetRequest extractFrom(Tuple tuple);
}
